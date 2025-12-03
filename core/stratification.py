"""
Stratification for Transaction Data Differential Privacy.

This module implements stratified noise calibration based on:
- City tier (metro/urban/suburban/rural) by transaction volume
- Day type (weekday/weekend/holiday) by temporal pattern
- MCC group (from mcc_groups.py) by transaction amount

Key insight: Strata defined by (MCC_group, city_tier, day_type) are DISJOINT.
Each transaction belongs to exactly ONE stratum, so we can use PARALLEL COMPOSITION.
This means each stratum can use the FULL privacy budget without additive cost.

Why stratification matters for transaction data:
- Different strata have vastly different sensitivity characteristics
- A metro area with millions of transactions can tolerate more absolute noise
- A rural area with hundreds needs suppression, not excessive noise
- MCC groups have different typical amounts (grocery vs car sales)
"""

import math
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Any
from enum import Enum
from datetime import datetime, date

try:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType, IntegerType
    HAS_SPARK = True
except ImportError:
    HAS_SPARK = False

import numpy as np


logger = logging.getLogger(__name__)


class CityTier(Enum):
    """City classification by transaction volume."""
    METRO = "metro"           # Very high volume (e.g., > 1M transactions/month)
    URBAN = "urban"           # High volume (e.g., 100K - 1M)
    SUBURBAN = "suburban"     # Medium volume (e.g., 10K - 100K)
    RURAL = "rural"           # Low volume (e.g., < 10K)
    UNKNOWN = "unknown"


class DayType(Enum):
    """Day classification by temporal pattern."""
    WEEKDAY = "weekday"       # Monday - Friday (non-holiday)
    WEEKEND = "weekend"       # Saturday - Sunday
    HOLIDAY = "holiday"       # National/religious holidays
    UNKNOWN = "unknown"


@dataclass
class CityTierInfo:
    """Information about a city tier."""
    tier: CityTier
    min_transactions: int
    max_transactions: int
    num_cities: int = 0
    total_transactions: int = 0
    avg_transactions_per_city: float = 0.0
    
    def __repr__(self) -> str:
        return (f"CityTierInfo({self.tier.value}: {self.num_cities} cities, "
                f"range=[{self.min_transactions:,}, {self.max_transactions:,}], "
                f"total={self.total_transactions:,})")


@dataclass
class CityTierResult:
    """Result of city tier classification."""
    city_to_tier: Dict[str, CityTier]
    tier_info: Dict[CityTier, CityTierInfo]
    tier_boundaries: Dict[CityTier, Tuple[int, int]]  # (min, max) transactions
    
    @property
    def num_cities(self) -> int:
        return len(self.city_to_tier)
    
    def get_tier(self, city: str) -> CityTier:
        """Get tier for a city."""
        return self.city_to_tier.get(city, CityTier.UNKNOWN)
    
    def summary(self) -> str:
        """Generate summary of city tier classification."""
        lines = [
            "=" * 60,
            "City Tier Classification Summary",
            "=" * 60,
            f"Total cities: {self.num_cities}",
            "",
            "Tier breakdown:",
        ]
        
        for tier in [CityTier.METRO, CityTier.URBAN, CityTier.SUBURBAN, CityTier.RURAL]:
            if tier in self.tier_info:
                info = self.tier_info[tier]
                lines.append(
                    f"  {tier.value:10s}: {info.num_cities:5d} cities, "
                    f"{info.total_transactions:12,} transactions"
                )
        
        lines.append("=" * 60)
        return "\n".join(lines)


@dataclass 
class DayTypeInfo:
    """Information about a day type."""
    day_type: DayType
    num_days: int = 0
    total_transactions: int = 0
    avg_transactions_per_day: float = 0.0


@dataclass
class DayTypeResult:
    """Result of day type classification."""
    date_to_type: Dict[str, DayType]  # date string -> DayType
    day_idx_to_type: Dict[int, DayType]  # day index -> DayType
    type_info: Dict[DayType, DayTypeInfo]
    holidays: List[str]  # List of holiday dates
    
    def get_type(self, day_idx: int) -> DayType:
        """Get day type for a day index."""
        return self.day_idx_to_type.get(day_idx, DayType.UNKNOWN)
    
    def get_type_by_date(self, date_str: str) -> DayType:
        """Get day type for a date string."""
        return self.date_to_type.get(date_str, DayType.UNKNOWN)


class CityTierClassifier:
    """
    Classifies cities into tiers based on transaction volume.
    
    Tiers are determined by percentiles of transaction counts:
    - METRO: Top 5% of cities by volume
    - URBAN: 5-25%
    - SUBURBAN: 25-75%
    - RURAL: Bottom 25%
    """
    
    # Default percentile boundaries
    DEFAULT_BOUNDARIES = {
        CityTier.METRO: (95, 100),      # Top 5%
        CityTier.URBAN: (75, 95),       # 75-95%
        CityTier.SUBURBAN: (25, 75),    # 25-75%
        CityTier.RURAL: (0, 25),        # Bottom 25%
    }
    
    def __init__(
        self,
        boundaries: Optional[Dict[CityTier, Tuple[int, int]]] = None,
        min_transactions_for_tier: Optional[Dict[CityTier, int]] = None
    ):
        """
        Initialize classifier.
        
        Args:
            boundaries: Custom percentile boundaries per tier (min_pct, max_pct)
            min_transactions_for_tier: Minimum transaction counts for each tier
        """
        self.boundaries = boundaries or self.DEFAULT_BOUNDARIES
        self.min_transactions = min_transactions_for_tier or {
            CityTier.METRO: 1000000,
            CityTier.URBAN: 100000,
            CityTier.SUBURBAN: 10000,
            CityTier.RURAL: 0,
        }
    
    def classify_from_spark(
        self,
        df: 'DataFrame',
        city_col: str = "acceptor_city"
    ) -> CityTierResult:
        """
        Classify cities from Spark DataFrame.
        
        Args:
            df: Transaction DataFrame
            city_col: Column name for city
            
        Returns:
            CityTierResult with classification
        """
        if not HAS_SPARK:
            raise RuntimeError("Spark not available")
        
        logger.info("Classifying cities into tiers...")
        
        # Count transactions per city
        city_counts = df.groupBy(city_col).agg(
            F.count("*").alias("tx_count")
        ).collect()
        
        if not city_counts:
            logger.warning("No cities found in data")
            return CityTierResult(
                city_to_tier={},
                tier_info={},
                tier_boundaries={}
            )
        
        # Build city -> count mapping
        city_txns = {row[city_col]: row["tx_count"] for row in city_counts}
        counts = list(city_txns.values())
        
        logger.info(f"Found {len(city_txns)} cities")
        logger.info(f"Transaction range: {min(counts):,} - {max(counts):,}")
        
        # Compute percentiles
        percentiles = {
            p: np.percentile(counts, p) 
            for p in [25, 50, 75, 90, 95, 99]
        }
        logger.info(f"Percentiles: {percentiles}")
        
        # Classify each city
        city_to_tier = {}
        tier_cities = {tier: [] for tier in CityTier}
        
        for city, count in city_txns.items():
            tier = self._get_tier_for_count(count, percentiles)
            city_to_tier[city] = tier
            tier_cities[tier].append((city, count))
        
        # Build tier info
        tier_info = {}
        tier_boundaries = {}
        
        for tier in [CityTier.METRO, CityTier.URBAN, CityTier.SUBURBAN, CityTier.RURAL]:
            cities = tier_cities[tier]
            if cities:
                counts_in_tier = [c for _, c in cities]
                tier_info[tier] = CityTierInfo(
                    tier=tier,
                    min_transactions=min(counts_in_tier),
                    max_transactions=max(counts_in_tier),
                    num_cities=len(cities),
                    total_transactions=sum(counts_in_tier),
                    avg_transactions_per_city=sum(counts_in_tier) / len(cities)
                )
                tier_boundaries[tier] = (min(counts_in_tier), max(counts_in_tier))
        
        result = CityTierResult(
            city_to_tier=city_to_tier,
            tier_info=tier_info,
            tier_boundaries=tier_boundaries
        )
        
        logger.info(result.summary())
        return result
    
    def _get_tier_for_count(self, count: int, percentiles: Dict[int, float]) -> CityTier:
        """Determine tier based on transaction count."""
        if count >= percentiles[95]:
            return CityTier.METRO
        elif count >= percentiles[75]:
            return CityTier.URBAN
        elif count >= percentiles[25]:
            return CityTier.SUBURBAN
        else:
            return CityTier.RURAL


class DayTypeClassifier:
    """
    Classifies days into types based on day of week and holidays.
    
    Day types affect transaction patterns:
    - Weekdays: B2B transactions, regular shopping
    - Weekends: Consumer shopping, entertainment
    - Holidays: Seasonal patterns, reduced B2B
    """
    
    # Default holidays (Iranian calendar - adjust as needed)
    DEFAULT_HOLIDAYS = []  # Add actual holiday dates here
    
    def __init__(self, holidays: Optional[List[str]] = None):
        """
        Initialize classifier.
        
        Args:
            holidays: List of holiday dates in YYYY-MM-DD format
        """
        self.holidays = set(holidays or self.DEFAULT_HOLIDAYS)
    
    def classify_from_dates(
        self,
        dates: List[date],
        base_date: Optional[date] = None
    ) -> DayTypeResult:
        """
        Classify dates into day types.
        
        Args:
            dates: List of dates to classify
            base_date: Base date for computing day indices
            
        Returns:
            DayTypeResult with classification
        """
        logger.info(f"Classifying {len(dates)} dates into day types...")
        
        if not dates:
            return DayTypeResult(
                date_to_type={},
                day_idx_to_type={},
                type_info={},
                holidays=list(self.holidays)
            )
        
        base_date = base_date or min(dates)
        
        date_to_type = {}
        day_idx_to_type = {}
        type_counts = {dt: 0 for dt in DayType}
        
        for d in dates:
            date_str = d.strftime("%Y-%m-%d")
            day_idx = (d - base_date).days
            
            if date_str in self.holidays:
                day_type = DayType.HOLIDAY
            elif d.weekday() >= 5:  # Saturday = 5, Sunday = 6
                day_type = DayType.WEEKEND
            else:
                day_type = DayType.WEEKDAY
            
            date_to_type[date_str] = day_type
            day_idx_to_type[day_idx] = day_type
            type_counts[day_type] += 1
        
        # Build type info
        type_info = {
            dt: DayTypeInfo(day_type=dt, num_days=count)
            for dt, count in type_counts.items()
            if count > 0
        }
        
        result = DayTypeResult(
            date_to_type=date_to_type,
            day_idx_to_type=day_idx_to_type,
            type_info=type_info,
            holidays=list(self.holidays)
        )
        
        logger.info(f"Day type distribution:")
        for dt, info in type_info.items():
            logger.info(f"  {dt.value}: {info.num_days} days")
        
        return result
    
    def classify_from_spark(
        self,
        df: 'DataFrame',
        date_col: str = "transaction_date"
    ) -> DayTypeResult:
        """
        Classify days from Spark DataFrame.
        
        Args:
            df: Transaction DataFrame
            date_col: Column name for date
            
        Returns:
            DayTypeResult with classification
        """
        if not HAS_SPARK:
            raise RuntimeError("Spark not available")
        
        # Get unique dates
        unique_dates = df.select(date_col).distinct().collect()
        dates = [row[date_col] for row in unique_dates if row[date_col]]
        
        # Convert to date objects if needed
        date_objects = []
        for d in dates:
            if isinstance(d, str):
                date_objects.append(datetime.strptime(d, "%Y-%m-%d").date())
            elif isinstance(d, datetime):
                date_objects.append(d.date())
            elif isinstance(d, date):
                date_objects.append(d)
        
        return self.classify_from_dates(date_objects)


@dataclass
class Stratum:
    """
    Represents a stratum (combination of MCC group, city tier, day type).
    
    Each transaction belongs to exactly ONE stratum, enabling parallel composition.
    """
    mcc_group: str
    city_tier: CityTier
    day_type: DayType
    
    # Statistics computed from data
    num_transactions: int = 0
    num_cards: int = 0
    max_cells_per_card: int = 1
    typical_amount: float = 0.0
    amount_cap: float = 0.0
    
    # Computed sensitivities
    count_sensitivity: float = 0.0
    unique_sensitivity: float = 0.0
    amount_sensitivity: float = 0.0
    
    @property
    def stratum_id(self) -> str:
        """Unique identifier for this stratum."""
        return f"{self.mcc_group}_{self.city_tier.value}_{self.day_type.value}"
    
    def compute_sensitivities(self, k_bound: int = 1) -> None:
        """Compute L2 sensitivities for this stratum."""
        sqrt_d = math.sqrt(max(1, self.max_cells_per_card))
        self.count_sensitivity = sqrt_d * k_bound
        self.unique_sensitivity = sqrt_d * 1.0
        self.amount_sensitivity = sqrt_d * self.amount_cap
    
    def __repr__(self) -> str:
        return (f"Stratum({self.stratum_id}: "
                f"{self.num_transactions:,} txns, "
                f"L2_count={self.count_sensitivity:.2f}, "
                f"L2_amount={self.amount_sensitivity:,.0f})")


class StratumManager:
    """
    Manages strata for stratified noise calibration.
    
    Creates and tracks strata based on (MCC group, city tier, day type) combinations.
    Each stratum has its own sensitivity parameters.
    """
    
    def __init__(
        self,
        city_tier_result: Optional[CityTierResult] = None,
        day_type_result: Optional[DayTypeResult] = None,
        mcc_to_group: Optional[Dict[str, str]] = None
    ):
        """
        Initialize stratum manager.
        
        Args:
            city_tier_result: City tier classification
            day_type_result: Day type classification
            mcc_to_group: MCC code to group name mapping
        """
        self.city_tiers = city_tier_result
        self.day_types = day_type_result
        self.mcc_to_group = mcc_to_group or {}
        
        self._strata: Dict[str, Stratum] = {}
    
    def compute_strata_from_spark(
        self,
        df: 'DataFrame',
        city_col: str = "acceptor_city",
        mcc_col: str = "mcc",
        day_idx_col: str = "day_idx",
        card_col: str = "card_number",
        amount_col: str = "amount",
        k_bound: int = 1,
        amount_percentile: float = 99.0
    ) -> Dict[str, Stratum]:
        """
        Compute strata and their sensitivities from data.
        
        Args:
            df: Transaction DataFrame with stratification columns
            city_col: City column name
            mcc_col: MCC column name
            day_idx_col: Day index column name
            card_col: Card column name
            amount_col: Amount column name
            k_bound: Per-cell contribution bound
            amount_percentile: Percentile for amount cap
            
        Returns:
            Dictionary mapping stratum_id to Stratum
        """
        if not HAS_SPARK:
            raise RuntimeError("Spark not available")
        
        logger.info("Computing strata from data...")
        
        # Add stratum columns to dataframe
        df_with_strata = self._add_stratum_columns(df, city_col, mcc_col, day_idx_col)
        
        # Get unique strata
        unique_strata = df_with_strata.select(
            "mcc_group", "city_tier", "day_type"
        ).distinct().collect()
        
        logger.info(f"Found {len(unique_strata)} unique strata")
        
        # Compute statistics for each stratum
        for row in unique_strata:
            mcc_group = row["mcc_group"] or "unknown"
            city_tier_str = row["city_tier"] or "unknown"
            day_type_str = row["day_type"] or "unknown"
            
            try:
                city_tier = CityTier(city_tier_str)
            except ValueError:
                city_tier = CityTier.UNKNOWN
            
            try:
                day_type = DayType(day_type_str)
            except ValueError:
                day_type = DayType.UNKNOWN
            
            # Filter to this stratum
            stratum_df = df_with_strata.filter(
                (F.col("mcc_group") == mcc_group) &
                (F.col("city_tier") == city_tier_str) &
                (F.col("day_type") == day_type_str)
            )
            
            # Compute statistics
            stats = stratum_df.agg(
                F.count("*").alias("num_transactions"),
                F.countDistinct(card_col).alias("num_cards"),
                F.mean(amount_col).alias("typical_amount"),
                F.expr(f"percentile_approx({amount_col}, {amount_percentile/100})").alias("amount_cap")
            ).collect()[0]
            
            # Compute max cells per card in this stratum
            cells_per_card = stratum_df.groupBy(card_col).agg(
                F.countDistinct(city_col, mcc_col, day_idx_col).alias("num_cells")
            )
            max_cells = cells_per_card.agg(F.max("num_cells")).collect()[0][0] or 1
            
            # Create stratum
            stratum = Stratum(
                mcc_group=mcc_group,
                city_tier=city_tier,
                day_type=day_type,
                num_transactions=int(stats["num_transactions"]),
                num_cards=int(stats["num_cards"]),
                max_cells_per_card=int(max_cells),
                typical_amount=float(stats["typical_amount"] or 0),
                amount_cap=float(stats["amount_cap"] or 0)
            )
            stratum.compute_sensitivities(k_bound)
            
            self._strata[stratum.stratum_id] = stratum
        
        logger.info(f"Computed {len(self._strata)} strata")
        for stratum in self._strata.values():
            logger.info(f"  {stratum}")
        
        return self._strata
    
    def _add_stratum_columns(
        self,
        df: 'DataFrame',
        city_col: str,
        mcc_col: str,
        day_idx_col: str
    ) -> 'DataFrame':
        """Add stratification columns to DataFrame."""
        # Add MCC group column
        if self.mcc_to_group:
            # Create mapping UDF or use when/otherwise
            mcc_group_default = "unknown"
            conditions = None
            for mcc, group in self.mcc_to_group.items():
                if conditions is None:
                    conditions = F.when(F.col(mcc_col) == mcc, group)
                else:
                    conditions = conditions.when(F.col(mcc_col) == mcc, group)
            
            if conditions is not None:
                df = df.withColumn("mcc_group", conditions.otherwise(mcc_group_default))
            else:
                df = df.withColumn("mcc_group", F.lit(mcc_group_default))
        else:
            df = df.withColumn("mcc_group", F.lit("all"))
        
        # Add city tier column
        if self.city_tiers and self.city_tiers.city_to_tier:
            city_tier_default = CityTier.UNKNOWN.value
            conditions = None
            for city, tier in self.city_tiers.city_to_tier.items():
                if conditions is None:
                    conditions = F.when(F.col(city_col) == city, tier.value)
                else:
                    conditions = conditions.when(F.col(city_col) == city, tier.value)
            
            if conditions is not None:
                df = df.withColumn("city_tier", conditions.otherwise(city_tier_default))
            else:
                df = df.withColumn("city_tier", F.lit(city_tier_default))
        else:
            df = df.withColumn("city_tier", F.lit(CityTier.UNKNOWN.value))
        
        # Add day type column
        if self.day_types and self.day_types.day_idx_to_type:
            day_type_default = DayType.UNKNOWN.value
            conditions = None
            for day_idx, day_type in self.day_types.day_idx_to_type.items():
                if conditions is None:
                    conditions = F.when(F.col(day_idx_col) == day_idx, day_type.value)
                else:
                    conditions = conditions.when(F.col(day_idx_col) == day_idx, day_type.value)
            
            if conditions is not None:
                df = df.withColumn("day_type", conditions.otherwise(day_type_default))
            else:
                df = df.withColumn("day_type", F.lit(day_type_default))
        else:
            df = df.withColumn("day_type", F.lit(DayType.UNKNOWN.value))
        
        return df
    
    def get_stratum(
        self,
        mcc_group: str,
        city_tier: CityTier,
        day_type: DayType
    ) -> Optional[Stratum]:
        """Get stratum by its components."""
        stratum_id = f"{mcc_group}_{city_tier.value}_{day_type.value}"
        return self._strata.get(stratum_id)
    
    def get_sensitivity_for_stratum(
        self,
        stratum_id: str,
        query_name: str
    ) -> float:
        """Get L2 sensitivity for a specific stratum and query."""
        if stratum_id not in self._strata:
            # Return max sensitivity across all strata (conservative)
            if query_name == "total_amount":
                return max(s.amount_sensitivity for s in self._strata.values()) if self._strata else 1.0
            else:
                return max(s.count_sensitivity for s in self._strata.values()) if self._strata else 1.0
        
        stratum = self._strata[stratum_id]
        
        if query_name == "transaction_count":
            return stratum.count_sensitivity
        elif query_name in ("unique_cards", "unique_acceptors"):
            return stratum.unique_sensitivity
        elif query_name == "total_amount":
            return stratum.amount_sensitivity
        else:
            return stratum.count_sensitivity
    
    @property
    def strata(self) -> Dict[str, Stratum]:
        """Get all strata."""
        return self._strata
    
    def summary(self) -> str:
        """Generate summary of all strata."""
        lines = [
            "=" * 70,
            "Stratum Summary",
            "=" * 70,
            f"Total strata: {len(self._strata)}",
            "",
            "Strata details:",
        ]
        
        for stratum in sorted(self._strata.values(), key=lambda s: s.num_transactions, reverse=True):
            lines.append(
                f"  {stratum.stratum_id:40s}: "
                f"{stratum.num_transactions:10,} txns, "
                f"D_max={stratum.max_cells_per_card:4d}, "
                f"L2_count={stratum.count_sensitivity:8.2f}, "
                f"L2_amount={stratum.amount_sensitivity:12,.0f}"
            )
        
        lines.append("=" * 70)
        return "\n".join(lines)

