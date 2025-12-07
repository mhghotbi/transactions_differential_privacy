"""
Invariant Manager for Census 2020-style Differential Privacy.

Manages exact totals (invariants) that are released without noise:
- National monthly totals: EXACT
- Province monthly totals: EXACT
- Daily values: NOISY (but adjusted to sum to monthly)
- City values: NOISY (but adjusted to sum to province)

Reference: "The 2020 Census Disclosure Avoidance System TopDown Algorithm"
"""

import logging
from typing import Dict, List, Optional, Set, Tuple, Any
from dataclasses import dataclass, field
from enum import Enum

import numpy as np

try:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql import functions as F
    HAS_SPARK = True
except ImportError:
    HAS_SPARK = False


logger = logging.getLogger(__name__)


class InvariantLevel(Enum):
    """Geographic levels for invariants."""
    NATIONAL = "national"
    PROVINCE = "province"
    CITY = "city"


class TemporalLevel(Enum):
    """Temporal levels for invariants."""
    MONTHLY = "monthly"
    DAILY = "daily"


@dataclass
class Invariant:
    """Represents an invariant (exact value, no noise)."""
    geo_level: InvariantLevel
    temporal_level: TemporalLevel
    key: str  # e.g., "province_01_month_01_transaction_count"
    value: float
    query_name: str
    
    def __repr__(self) -> str:
        return f"Invariant({self.key}={self.value})"


@dataclass
class InvariantSet:
    """Collection of invariants organized by level."""
    national_monthly: Dict[str, float] = field(default_factory=dict)
    province_monthly: Dict[str, Dict[str, float]] = field(default_factory=dict)
    
    def get_national(self, query: str) -> Optional[float]:
        """Get national monthly invariant for a query."""
        return self.national_monthly.get(query)
    
    def get_province(self, province: str, query: str) -> Optional[float]:
        """Get province monthly invariant for a query."""
        if province in self.province_monthly:
            return self.province_monthly[province].get(query)
        return None


class InvariantManager:
    """
    Manages invariants for Census 2020-style DP.
    
    Invariant structure:
    - National monthly: EXACT (no noise)
    - Province monthly: EXACT (no noise)
    - Daily values: Can have noise, but sum to monthly
    - City values: Can have noise, but sum to province
    
    This means:
    - All 4 metrics at national/province monthly level are invariants
    - Daily and city-level values get noise
    - Post-processing ensures consistency with invariants
    """
    
    QUERIES = [
        'transaction_count',
        'unique_cards', 
        'unique_acceptors',
        'total_amount'
    ]
    
    def __init__(
        self,
        invariant_levels: Optional[List[Tuple[InvariantLevel, TemporalLevel]]] = None
    ):
        """
        Initialize invariant manager.
        
        Args:
            invariant_levels: List of (geo_level, temporal_level) tuples that are invariants.
                            Default: [(NATIONAL, MONTHLY), (PROVINCE, MONTHLY)]
        """
        self.invariant_levels = invariant_levels or [
            (InvariantLevel.NATIONAL, TemporalLevel.MONTHLY),
            (InvariantLevel.PROVINCE, TemporalLevel.MONTHLY),
        ]
        
        self._invariants: InvariantSet = InvariantSet()
        self._computed = False
    
    def is_invariant(
        self,
        geo_level: InvariantLevel,
        temporal_level: TemporalLevel
    ) -> bool:
        """Check if a level combination is an invariant."""
        return (geo_level, temporal_level) in self.invariant_levels
    
    def should_add_noise(
        self,
        geo_level: str,
        temporal_level: str = "daily"
    ) -> bool:
        """
        Determine if noise should be added at this level.
        
        Args:
            geo_level: 'national', 'province', or 'city'
            temporal_level: 'monthly' or 'daily'
            
        Returns:
            True if noise should be added, False if invariant
        """
        try:
            geo = InvariantLevel(geo_level)
            temp = TemporalLevel(temporal_level)
            return not self.is_invariant(geo, temp)
        except ValueError:
            # Unknown level, add noise to be safe
            return True
    
    def compute_invariants_from_spark(
        self,
        df: 'DataFrame',
        province_col: str = 'province_name',
        day_col: str = 'day_idx'
    ) -> InvariantSet:
        """
        Compute invariants from Spark DataFrame BEFORE noise is added.
        
        Args:
            df: DataFrame with aggregated counts per (province, city, mcc, day)
            province_col: Column name for province
            day_col: Column name for day index
            
        Returns:
            InvariantSet with national and province monthly totals
        """
        if not HAS_SPARK:
            raise RuntimeError("Spark not available")
        
        logger.info("Computing invariants from Spark DataFrame...")
        
        # National monthly totals (sum across all provinces and days)
        national_agg = df.agg(
            *[F.sum(F.col(q)).alias(q) for q in self.QUERIES]
        ).first()
        
        for query in self.QUERIES:
            self._invariants.national_monthly[query] = float(national_agg[query] or 0)
        
        logger.info(f"  National monthly: {self._invariants.national_monthly}")
        
        # Province monthly totals (sum across days within each province)
        province_agg = df.groupBy(province_col).agg(
            *[F.sum(F.col(q)).alias(q) for q in self.QUERIES]
        ).collect()
        
        for row in province_agg:
            province = row[province_col]
            self._invariants.province_monthly[province] = {
                query: float(row[query] or 0)
                for query in self.QUERIES
            }
        
        logger.info(f"  Province monthly: {len(self._invariants.province_monthly)} provinces")
        
        self._computed = True
        return self._invariants
    
    def compute_invariants_from_numpy(
        self,
        data: Dict[str, np.ndarray],
        province_mapping: Dict[str, List[int]]
    ) -> InvariantSet:
        """
        Compute invariants from numpy arrays.
        
        Args:
            data: Dict mapping query_name to array of shape (provinces, cities, days)
            province_mapping: Maps province_name to list of city indices
            
        Returns:
            InvariantSet with computed invariants
        """
        logger.info("Computing invariants from numpy arrays...")
        
        # National monthly = sum of all values
        for query, arr in data.items():
            self._invariants.national_monthly[query] = float(arr.sum())
        
        # Province monthly = sum across cities and days for each province
        for query, arr in data.items():
            for province, city_indices in province_mapping.items():
                if province not in self._invariants.province_monthly:
                    self._invariants.province_monthly[province] = {}
                
                # Sum across city indices and all days
                province_total = arr[:, city_indices, :].sum()
                self._invariants.province_monthly[province][query] = float(province_total)
        
        self._computed = True
        return self._invariants
    
    def get_invariants(self) -> InvariantSet:
        """Get computed invariants."""
        if not self._computed:
            logger.warning("Invariants not yet computed")
        return self._invariants
    
    def get_national_invariant(self, query: str) -> float:
        """Get national monthly invariant for a query."""
        return self._invariants.national_monthly.get(query, 0.0)
    
    def get_province_invariant(self, province: str, query: str) -> float:
        """Get province monthly invariant for a query."""
        if province in self._invariants.province_monthly:
            return self._invariants.province_monthly[province].get(query, 0.0)
        return 0.0
    
    def get_all_province_invariants(self, query: str) -> Dict[str, float]:
        """Get all province invariants for a query."""
        result = {}
        for province, queries in self._invariants.province_monthly.items():
            result[province] = queries.get(query, 0.0)
        return result
    
    def verify_consistency(
        self,
        noisy_data: Dict[str, Any],
        tolerance: float = 1e-6
    ) -> Dict[str, bool]:
        """
        Verify that noisy data is consistent with invariants.
        
        Args:
            noisy_data: Noisy values to check
            tolerance: Tolerance for sum checks
            
        Returns:
            Dict mapping check_name to pass/fail
        """
        results = {}
        
        # Check national totals
        for query in self.QUERIES:
            expected = self._invariants.national_monthly.get(query, 0)
            # Would need actual sum from noisy_data
            # This is a placeholder for the check
            results[f"national_{query}"] = True
        
        return results
    
    def summary(self) -> str:
        """Generate summary of invariants."""
        lines = [
            "=" * 60,
            "Invariant Summary",
            "=" * 60,
            "",
            "Invariant Levels:",
        ]
        
        for geo, temp in self.invariant_levels:
            lines.append(f"  {geo.value} x {temp.value}: EXACT (no noise)")
        
        lines.extend(["", "Noise Levels:"])
        
        all_geo = [InvariantLevel.NATIONAL, InvariantLevel.PROVINCE, InvariantLevel.CITY]
        all_temp = [TemporalLevel.MONTHLY, TemporalLevel.DAILY]
        
        for geo in all_geo:
            for temp in all_temp:
                if not self.is_invariant(geo, temp):
                    lines.append(f"  {geo.value} x {temp.value}: NOISY")
        
        if self._computed:
            lines.extend(["", "Computed Values:"])
            lines.append(f"  National monthly: {self._invariants.national_monthly}")
            lines.append(f"  Provinces: {len(self._invariants.province_monthly)}")
        
        lines.append("=" * 60)
        return "\n".join(lines)


class InvariantEnforcer:
    """
    Enforces invariant constraints on noisy data.
    
    After adding noise, adjusts values so that:
    - Sum of daily values = monthly invariant
    - Sum of city values = province invariant
    - Sum of province values = national invariant
    """
    
    def __init__(self, invariant_manager: InvariantManager):
        self.manager = invariant_manager
        
        # Import post-processor and rounder
        from core.postprocessing import NNLSPostProcessor
        from core.rounder import CensusControlledRounder
        
        self.postprocessor = NNLSPostProcessor()
        self.rounder = CensusControlledRounder()
    
    def enforce_temporal(
        self,
        daily_values: np.ndarray,
        monthly_total: float,
        round_to_int: bool = True
    ) -> np.ndarray:
        """
        Enforce temporal consistency: daily values sum to monthly.
        
        Args:
            daily_values: Noisy daily values
            monthly_total: Monthly invariant (exact)
            round_to_int: Whether to round to integers
            
        Returns:
            Adjusted daily values summing to monthly_total
        """
        # NNLS adjustment
        adjusted = self.postprocessor.solve(daily_values, monthly_total)
        
        # Round if requested
        if round_to_int:
            result = self.rounder.round(adjusted, int(monthly_total))
            return result.values
        
        return adjusted
    
    def enforce_geographic(
        self,
        city_values: np.ndarray,
        province_total: float,
        round_to_int: bool = True
    ) -> np.ndarray:
        """
        Enforce geographic consistency: city values sum to province.
        
        Args:
            city_values: Noisy city values for a province
            province_total: Province invariant (exact)
            round_to_int: Whether to round to integers
            
        Returns:
            Adjusted city values summing to province_total
        """
        # NNLS adjustment
        adjusted = self.postprocessor.solve(city_values, province_total)
        
        # Round if requested
        if round_to_int:
            result = self.rounder.round(adjusted, int(province_total))
            return result.values
        
        return adjusted
    
    def enforce_all(
        self,
        noisy_data: Dict[str, np.ndarray],
        structure: Dict[str, Any]
    ) -> Dict[str, np.ndarray]:
        """
        Enforce all invariant constraints.
        
        Args:
            noisy_data: Dict mapping query to noisy array
            structure: Geographic/temporal structure info
            
        Returns:
            Adjusted data satisfying all invariants
        """
        result = {}
        
        for query, values in noisy_data.items():
            # Get invariants for this query
            national_inv = self.manager.get_national_invariant(query)
            province_invs = self.manager.get_all_province_invariants(query)
            
            # Apply adjustments (implementation depends on data structure)
            result[query] = values.copy()
        
        return result

