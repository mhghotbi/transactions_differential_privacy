"""
Privacy Budget Management for zCDP (zero-Concentrated Differential Privacy).

This module handles:
- Budget allocation across geographic levels (province/city)
- Budget allocation across queries
- Budget composition and tracking
- Conversion between rho and (epsilon, delta)
- Monthly budget management for periodic releases

COMPOSITION RULES (Critical for Privacy):
===========================================

1. SEQUENTIAL COMPOSITION: When the SAME data is used multiple times
   - Total rho = sum of individual rho values
   - Example: Adding noise to same cell at province AND city level
   - Example: Same cards appear in multiple months of releases

2. PARALLEL COMPOSITION: When DISJOINT subsets of data are used
   - Total rho = max of individual rho values  
   - Example: Different MCC groups (each transaction in exactly one MCC)
   - Example: Different days IF each card only appears on one day (RARE)

3. POST-PROCESSING: Operations on noisy outputs don't consume budget
   - NNLS adjustment, rounding, suppression are "free"

TRANSACTION DATA CONSIDERATIONS:
================================
- Cards can appear across multiple days -> Days are NOT parallel (sequential)
- Cards can appear across multiple cities -> Cities are NOT parallel (sequential)
- Cards can appear across multiple MCCs -> MCCs MAY be parallel if disjoint
- Monthly releases with same cards -> Months are sequential
"""

import math
import logging
from dataclasses import dataclass, field
from fractions import Fraction
from typing import Dict, List, Optional, Tuple
from enum import Enum

import numpy as np


logger = logging.getLogger(__name__)


class CompositionType(Enum):
    """Type of composition for privacy budget accounting."""
    SEQUENTIAL = "sequential"  # Same data, budgets add
    PARALLEL = "parallel"      # Disjoint data, budget = max


@dataclass
class BudgetAllocation:
    """Represents a budget allocation for a specific component."""
    name: str
    rho: Fraction
    level: str  # 'province', 'city', or 'query'
    
    @property
    def sigma_squared(self) -> float:
        """
        Compute sigma^2 for Discrete Gaussian mechanism.
        For zCDP with parameter rho, sigma^2 = 1/(2*rho)
        """
        if self.rho == 0:
            return float('inf')
        return 1.0 / (2.0 * float(self.rho))
    
    @property
    def sigma(self) -> float:
        """Compute sigma for Discrete Gaussian mechanism."""
        return math.sqrt(self.sigma_squared)
    
    def __repr__(self) -> str:
        return f"BudgetAllocation({self.name}, rho={self.rho}, sigma={self.sigma:.4f})"


class Budget:
    """
    Manages the total privacy budget and its allocation.
    
    Uses zCDP (zero-Concentrated Differential Privacy) accounting.
    Under zCDP:
    - Composition: rho_total = sum(rho_i) for independent mechanisms
    - Conversion to (epsilon, delta)-DP: epsilon = rho + 2*sqrt(rho * ln(1/delta))
    """
    
    def __init__(
        self,
        total_rho: Fraction,
        delta: float = 1e-10,
        geographic_split: Optional[Dict[str, float]] = None,
        query_split: Optional[Dict[str, float]] = None
    ):
        """
        Initialize budget manager.
        
        Args:
            total_rho: Total privacy budget in zCDP (rho parameter)
            delta: Delta parameter for (epsilon, delta)-DP conversion
            geographic_split: Dict mapping level names to proportions
            query_split: Dict mapping query names to proportions
        """
        self.total_rho = total_rho
        self.delta = delta
        
        self.geographic_split = geographic_split or {
            "province": 0.2,
            "city": 0.8
        }
        
        self.query_split = query_split or {
            "transaction_count": 0.34,
            "unique_cards": 0.33,
            "total_amount": 0.33
        }
        
        self._validate_splits()
        self._used_budget: Fraction = Fraction(0)
        self._allocations: List[BudgetAllocation] = []
    
    def _validate_splits(self) -> None:
        """Validate that splits sum to 1.0."""
        geo_sum = sum(self.geographic_split.values())
        if abs(geo_sum - 1.0) > 1e-6:
            raise ValueError(f"Geographic split must sum to 1.0, got {geo_sum}")
        
        query_sum = sum(self.query_split.values())
        if abs(query_sum - 1.0) > 1e-6:
            raise ValueError(f"Query split must sum to 1.0, got {query_sum}")
    
    @property
    def total_epsilon(self) -> float:
        """
        Convert total rho to epsilon using zCDP to (epsilon, delta)-DP conversion.
        
        epsilon = rho + 2*sqrt(rho * ln(1/delta))
        """
        rho = float(self.total_rho)
        return rho + 2 * math.sqrt(rho * math.log(1 / self.delta))
    
    @property
    def remaining_budget(self) -> Fraction:
        """Get remaining unused budget."""
        return self.total_rho - self._used_budget
    
    def get_geo_level_budget(self, level: str) -> Fraction:
        """
        Get budget allocated to a geographic level.
        
        Args:
            level: Geographic level ('province' or 'city')
            
        Returns:
            Fraction of total_rho allocated to this level
        """
        if level not in self.geographic_split:
            raise ValueError(f"Unknown geographic level: {level}")
        
        proportion = Fraction(self.geographic_split[level]).limit_denominator(1000)
        return self.total_rho * proportion
    
    def get_query_budget(self, query_name: str, geo_level: str) -> Fraction:
        """
        Get budget for a specific query at a geographic level.
        
        The budget is: total_rho * geo_proportion * query_proportion
        
        Args:
            query_name: Name of the query
            geo_level: Geographic level
            
        Returns:
            Fraction of rho allocated to this query at this level
        """
        if query_name not in self.query_split:
            raise ValueError(f"Unknown query: {query_name}")
        
        geo_budget = self.get_geo_level_budget(geo_level)
        query_proportion = Fraction(self.query_split[query_name]).limit_denominator(1000)
        
        return geo_budget * query_proportion
    
    def allocate(
        self,
        name: str,
        query_name: str,
        geo_level: str
    ) -> BudgetAllocation:
        """
        Allocate budget for a specific query at a geographic level.
        
        Args:
            name: Descriptive name for this allocation
            query_name: Name of the query
            geo_level: Geographic level
            
        Returns:
            BudgetAllocation object with the allocated rho
        """
        rho = self.get_query_budget(query_name, geo_level)
        
        allocation = BudgetAllocation(
            name=name,
            rho=rho,
            level=geo_level
        )
        
        self._allocations.append(allocation)
        self._used_budget += rho
        
        logger.debug(f"Allocated budget: {allocation}")
        return allocation
    
    def get_all_allocations(self) -> Dict[str, Dict[str, BudgetAllocation]]:
        """
        Pre-compute all budget allocations for all queries and levels.
        
        Returns:
            Nested dict: allocations[geo_level][query_name] = BudgetAllocation
        """
        allocations = {}
        
        for geo_level in self.geographic_split.keys():
            allocations[geo_level] = {}
            for query_name in self.query_split.keys():
                name = f"{geo_level}_{query_name}"
                rho = self.get_query_budget(query_name, geo_level)
                allocations[geo_level][query_name] = BudgetAllocation(
                    name=name,
                    rho=rho,
                    level=geo_level
                )
        
        return allocations
    
    def compute_sigma_for_query(
        self,
        query_name: str,
        geo_level: str,
        sensitivity: float = 1.0
    ) -> float:
        """
        Compute the sigma (standard deviation) for Discrete Gaussian noise.
        
        For zCDP with sensitivity Delta and parameter rho:
        sigma^2 = Delta^2 / (2 * rho)
        
        Args:
            query_name: Name of the query
            geo_level: Geographic level
            sensitivity: L2 sensitivity of the query (default 1.0 for counting)
            
        Returns:
            Sigma value for the Discrete Gaussian mechanism
        """
        rho = self.get_query_budget(query_name, geo_level)
        
        if rho == 0:
            return float('inf')
        
        sigma_squared = (sensitivity ** 2) / (2 * float(rho))
        return math.sqrt(sigma_squared)
    
    def summary(self) -> str:
        """Generate a summary of budget allocation."""
        lines = [
            "=" * 60,
            "Privacy Budget Summary (zCDP)",
            "=" * 60,
            f"Total rho:      {self.total_rho} ({float(self.total_rho):.6f})",
            f"Total epsilon:  {self.total_epsilon:.4f} (at delta={self.delta})",
            "",
            "Geographic Allocation:",
        ]
        
        for level, prop in self.geographic_split.items():
            rho = self.get_geo_level_budget(level)
            lines.append(f"  {level:12s}: {prop:.0%} -> rho={float(rho):.6f}")
        
        lines.extend(["", "Query Allocation (per geo level):"])
        
        for query, prop in self.query_split.items():
            lines.append(f"  {query:20s}: {prop:.0%}")
        
        lines.extend(["", "Combined Allocations (rho per query per level):"])
        
        for geo_level in self.geographic_split.keys():
            for query_name in self.query_split.keys():
                rho = self.get_query_budget(query_name, geo_level)
                sigma = self.compute_sigma_for_query(query_name, geo_level)
                lines.append(
                    f"  {geo_level:8s} x {query_name:20s}: "
                    f"rho={float(rho):.6f}, sigma={sigma:.4f}"
                )
        
        lines.append("=" * 60)
        return "\n".join(lines)


class BudgetAllocator:
    """
    Allocates budget across the geographic and temporal hierarchy.
    
    For transaction data with Province -> City hierarchy and daily reports.
    
    IMPORTANT COMPOSITION CONSIDERATIONS:
    =====================================
    
    1. Geographic Levels (Province, City):
       - Province and City are SEQUENTIAL composition (same cards appear in both)
       - Budget split: province gets p%, city gets (1-p)%
       - Total rho = rho_province + rho_city
    
    2. Days within a month:
       - Days are SEQUENTIAL if same cards appear across days (usual case)
       - We use the FULL budget for the entire month, not per-day
       - Daily values are post-processed from monthly noisy totals
       
    3. MCC Groups:
       - MCCs are PARALLEL if each transaction belongs to exactly one MCC
       - Each MCC group can use the FULL budget (parallel composition)
    """
    
    def __init__(
        self, 
        budget: Budget, 
        num_days: int = 30,
        temporal_composition: CompositionType = CompositionType.SEQUENTIAL
    ):
        """
        Initialize allocator.
        
        Args:
            budget: Budget manager instance
            num_days: Number of days in the reporting period
            temporal_composition: How to compose budget across days
                - SEQUENTIAL: Cards appear across multiple days (default, conservative)
                - PARALLEL: Each card only appears on one day (rare, aggressive)
        """
        self.budget = budget
        self.num_days = num_days
        self.temporal_composition = temporal_composition
        
        # Pre-compute allocations
        self._allocations = self.budget.get_all_allocations()
        
        if temporal_composition == CompositionType.PARALLEL:
            logger.warning(
                "Using PARALLEL composition for days. This assumes each card "
                "appears on AT MOST ONE day. If this is not true, privacy may be violated!"
            )
    
    def get_daily_allocation(
        self,
        query_name: str,
        geo_level: str,
        day_index: int
    ) -> BudgetAllocation:
        """
        Get budget allocation for a specific day.
        
        COMPOSITION RULES:
        - SEQUENTIAL (default): Total budget is for the entire month.
          Daily values should be derived from monthly totals via post-processing.
          Each day does NOT get independent budget.
          
        - PARALLEL: Each day gets full budget (only valid if cards don't span days)
        
        Args:
            query_name: Query name
            geo_level: Geographic level
            day_index: Day index (0 to num_days-1)
            
        Returns:
            BudgetAllocation for this query/level/day
        """
        base_allocation = self._allocations[geo_level][query_name]
        
        if self.temporal_composition == CompositionType.PARALLEL:
            # Each day gets full budget (DANGEROUS if cards span days)
            return BudgetAllocation(
                name=f"{base_allocation.name}_day{day_index}",
                rho=base_allocation.rho,
                level=geo_level
            )
        else:
            # SEQUENTIAL: Budget is for the month, not per day
            # Return the monthly budget - noise should be added at monthly level
            # then distributed to days via post-processing
            return BudgetAllocation(
                name=f"{base_allocation.name}_monthly",
                rho=base_allocation.rho,
                level=geo_level
            )
    
    def get_monthly_allocation(
        self,
        query_name: str,
        geo_level: str
    ) -> BudgetAllocation:
        """
        Get budget allocation for the entire month.
        
        This is the correct allocation for sequential composition across days.
        Add noise to monthly aggregates, then post-process to daily values.
        
        Args:
            query_name: Query name
            geo_level: Geographic level
            
        Returns:
            BudgetAllocation for this query/level for the entire month
        """
        return self._allocations[geo_level][query_name]
    
    def get_sigma_matrix(self) -> Dict[str, Dict[str, float]]:
        """
        Get sigma values for all query/level combinations.
        
        Returns:
            Nested dict: sigma_matrix[geo_level][query_name] = sigma
        """
        sigma_matrix = {}
        
        for geo_level in self.budget.geographic_split.keys():
            sigma_matrix[geo_level] = {}
            for query_name in self.budget.query_split.keys():
                sigma = self.budget.compute_sigma_for_query(query_name, geo_level)
                sigma_matrix[geo_level][query_name] = sigma
        
        return sigma_matrix


class MonthlyBudgetManager:
    """
    Manages privacy budget across multiple monthly releases.
    
    For ongoing monthly data releases where the same cards may appear
    across multiple months, we must use SEQUENTIAL composition.
    
    Options:
    1. Fixed annual budget: ρ_year / 12 per month
    2. Sliding window: Only protect last N months
    3. Decaying budget: More budget for recent months
    """
    
    def __init__(
        self,
        annual_rho: Fraction,
        num_months: int = 12,
        strategy: str = "fixed"
    ):
        """
        Initialize monthly budget manager.
        
        Args:
            annual_rho: Total privacy budget for the year
            num_months: Number of months in the year (default 12)
            strategy: Budget allocation strategy
                - "fixed": Equal budget per month (ρ_year / 12)
                - "decay": More budget for recent months
                - "sliding": Rolling window of N months
        """
        self.annual_rho = annual_rho
        self.num_months = num_months
        self.strategy = strategy
        
        self._monthly_budgets: Dict[int, Fraction] = {}
        self._used_months: List[int] = []
        
        self._compute_monthly_budgets()
    
    def _compute_monthly_budgets(self) -> None:
        """Compute budget allocation for each month."""
        if self.strategy == "fixed":
            # Equal budget per month
            monthly_rho = self.annual_rho / self.num_months
            for month in range(self.num_months):
                self._monthly_budgets[month] = monthly_rho
                
        elif self.strategy == "decay":
            # More budget for later months (exponential decay)
            # Month 12 gets ~2x budget of month 1
            weights = [2 ** (i / self.num_months) for i in range(self.num_months)]
            total_weight = sum(weights)
            for month, weight in enumerate(weights):
                self._monthly_budgets[month] = self.annual_rho * Fraction(weight / total_weight).limit_denominator(1000)
                
        elif self.strategy == "sliding":
            # Sliding window - reuse budget after window expires
            # This provides weaker guarantees but more utility
            window_size = 6  # 6-month sliding window
            monthly_rho = self.annual_rho / window_size
            for month in range(self.num_months):
                self._monthly_budgets[month] = monthly_rho
        else:
            raise ValueError(f"Unknown strategy: {self.strategy}")
        
        logger.info(f"Monthly budget strategy: {self.strategy}")
        logger.info(f"Annual rho: {float(self.annual_rho):.6f}")
        logger.info(f"Monthly rho: {float(self._monthly_budgets[0]):.6f}")
    
    def get_month_budget(self, month: int) -> Fraction:
        """Get the privacy budget for a specific month (0-indexed)."""
        if month not in self._monthly_budgets:
            raise ValueError(f"Month {month} not in range [0, {self.num_months})")
        return self._monthly_budgets[month]
    
    def record_month_release(self, month: int) -> None:
        """Record that a month's data has been released."""
        if month not in self._used_months:
            self._used_months.append(month)
            logger.info(f"Recorded release for month {month}")
    
    def get_cumulative_budget_used(self) -> Fraction:
        """Get total budget used across all released months."""
        return sum(self._monthly_budgets[m] for m in self._used_months)
    
    def get_cumulative_epsilon(self, delta: float = 1e-10) -> float:
        """Get cumulative epsilon across all released months."""
        total_rho = float(self.get_cumulative_budget_used())
        if total_rho <= 0:
            return 0.0
        return total_rho + 2 * math.sqrt(total_rho * math.log(1 / delta))
    
    def summary(self) -> str:
        """Generate summary of monthly budget usage."""
        lines = [
            "=" * 60,
            "Monthly Budget Summary",
            "=" * 60,
            f"Strategy: {self.strategy}",
            f"Annual rho: {float(self.annual_rho):.6f}",
            f"Months released: {len(self._used_months)}/{self.num_months}",
            f"Cumulative rho used: {float(self.get_cumulative_budget_used()):.6f}",
            "",
            "Per-month budgets:",
        ]
        
        for month in range(self.num_months):
            rho = self._monthly_budgets[month]
            status = "RELEASED" if month in self._used_months else "pending"
            lines.append(f"  Month {month+1:2d}: rho={float(rho):.6f} [{status}]")
        
        lines.append("=" * 60)
        return "\n".join(lines)


class BudgetVerifier:
    """
    Verifies that privacy budget is not exceeded during pipeline execution.
    
    Tracks all noise additions and ensures total doesn't exceed allocated budget.
    """
    
    def __init__(self, total_rho: Fraction):
        """Initialize verifier with total budget."""
        self.total_rho = total_rho
        self._used_rho = Fraction(0)
        self._allocations: List[Tuple[str, Fraction]] = []
    
    def record_noise_addition(self, name: str, rho: Fraction) -> None:
        """Record a noise addition that consumes privacy budget."""
        self._allocations.append((name, rho))
        self._used_rho += rho
        
        if self._used_rho > self.total_rho:
            logger.error(
                f"PRIVACY BUDGET EXCEEDED! "
                f"Used {float(self._used_rho):.6f}, allowed {float(self.total_rho):.6f}"
            )
            raise RuntimeError(f"Privacy budget exceeded: {self._used_rho} > {self.total_rho}")
        
        logger.debug(f"Budget: {name} used rho={float(rho):.6f}, total={float(self._used_rho):.6f}")
    
    def verify(self) -> bool:
        """Verify total budget is not exceeded."""
        if self._used_rho > self.total_rho:
            return False
        return True
    
    def get_remaining(self) -> Fraction:
        """Get remaining budget."""
        return self.total_rho - self._used_rho
    
    def summary(self) -> str:
        """Generate summary of budget usage."""
        lines = [
            "Budget Verification Summary",
            f"Total budget: {float(self.total_rho):.6f}",
            f"Used budget: {float(self._used_rho):.6f}",
            f"Remaining: {float(self.get_remaining()):.6f}",
            f"Status: {'OK' if self.verify() else 'EXCEEDED'}",
            "",
            "Allocations:"
        ]
        for name, rho in self._allocations:
            lines.append(f"  {name}: {float(rho):.6f}")
        return "\n".join(lines)

