"""
Global Sensitivity Calculator for Differential Privacy.

Computes the true global L2 sensitivity accounting for individuals
appearing in multiple cells.

Census 2020 Context:
- A person can only be in ONE geographic unit (their residence)
- But they contribute to multiple queries (age, race, etc.)
- Sensitivity is sqrt(number_of_queries) for L2

Our Transaction Context (User-Level DP):
- A card can appear in MULTIPLE (city, mcc, day) cells
- Privacy unit is the CARD (user-level), not individual transaction (event-level)
- If card appears in D distinct cells, removing the card affects D cells
- For counting queries: L2 sensitivity = sqrt(D) * K where K = max txns per cell
- For unique counts: L2 sensitivity = sqrt(D) * 1 (each cell changes by at most 1)
- For sum queries: L2 sensitivity = sqrt(D) * cap (capped contribution per cell)

Key Difference from Event-Level DP:
- Event-level: Protects individual transactions (sensitivity = 1 per transaction)
- User-level: Protects entire card history (sensitivity depends on card's footprint)
"""

import math
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Any
from fractions import Fraction

try:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql import functions as F
    HAS_SPARK = True
except ImportError:
    HAS_SPARK = False

import numpy as np
import logging

logger = logging.getLogger(__name__)


@dataclass
class UserLevelSensitivity:
    """
    Stores user-level sensitivity information for a query.
    
    User-level DP protects entire card histories, not individual transactions.
    """
    query_name: str
    max_cells_per_user: int          # D_max: max distinct cells any card appears in
    per_cell_contribution: float     # K or cap: max contribution per cell
    l2_sensitivity: float            # sqrt(D_max) * per_cell_contribution
    l1_sensitivity: float            # D_max * per_cell_contribution
    sensitivity_type: str            # 'count', 'unique', or 'sum'
    
    def __post_init__(self):
        """Compute sensitivities if not provided."""
        if self.l2_sensitivity == 0:
            self.l2_sensitivity = math.sqrt(self.max_cells_per_user) * self.per_cell_contribution
        if self.l1_sensitivity == 0:
            self.l1_sensitivity = self.max_cells_per_user * self.per_cell_contribution
    
    def get_sigma_for_rho(self, rho: float) -> float:
        """
        Compute sigma for Discrete Gaussian mechanism given zCDP budget rho.
        
        For zCDP: sigma^2 = (sensitivity^2) / (2 * rho)
        """
        if rho <= 0:
            return float('inf')
        return math.sqrt((self.l2_sensitivity ** 2) / (2 * rho))
    
    def __repr__(self) -> str:
        return (f"UserLevelSensitivity({self.query_name}: "
                f"D_max={self.max_cells_per_user}, K={self.per_cell_contribution:.2f}, "
                f"L2={self.l2_sensitivity:.4f})")


@dataclass
class StratumSensitivity:
    """
    Sensitivity information for a specific stratum (MCC_group, city_tier, day_type).
    
    Different strata can have different sensitivity bounds due to:
    - Different typical transaction amounts (affects sum sensitivity)
    - Different transaction volumes (affects noise-to-signal ratio)
    """
    stratum_id: str
    mcc_group: str
    city_tier: str
    day_type: str
    
    # Per-stratum statistics
    num_cells: int = 0
    num_cards: int = 0
    max_cells_per_card: int = 1
    typical_count: float = 0.0
    typical_amount: float = 0.0
    amount_cap: float = 0.0
    
    # Computed sensitivities
    count_sensitivity: float = 0.0
    unique_sensitivity: float = 0.0
    amount_sensitivity: float = 0.0
    
    def compute_sensitivities(self, k_bound: int = 1) -> None:
        """Compute L2 sensitivities for this stratum."""
        sqrt_d = math.sqrt(max(1, self.max_cells_per_card))
        self.count_sensitivity = sqrt_d * k_bound
        self.unique_sensitivity = sqrt_d * 1  # unique counts change by at most 1 per cell
        self.amount_sensitivity = sqrt_d * self.amount_cap


class GlobalSensitivityCalculator:
    """
    Calculates global L2 sensitivity for queries accounting for
    multi-cell appearances under USER-LEVEL differential privacy.
    
    L2 Sensitivity Definition:
    For a function f: D -> R^n, the L2 sensitivity is:
        Delta_2 = max_{D, D'} ||f(D) - f(D')||_2
    where D and D' differ by one CARD (user-level, not transaction/event-level).
    
    User-Level Sensitivity Analysis:
    - Adding/removing a card C affects all cells where C has transactions
    - If C appears in D distinct cells, up to D output values change
    - For counting queries: L2 = sqrt(D) * K (each cell changes by at most K)
    - For unique queries: L2 = sqrt(D) * 1 (each cell changes by at most 1)
    - For sum queries: L2 = sqrt(D) * cap (each cell's sum changes by at most cap)
    
    This is MORE CONSERVATIVE than event-level DP but provides STRONGER privacy:
    - Event-level: Adding one transaction changes one cell by 1
    - User-level: Adding one card can change many cells by their max contributions
    """
    
    SENSITIVITY_METHODS = ["local", "global", "fixed", "per_query", "user_level"]
    
    def __init__(
        self,
        spark: 'SparkSession' = None,
        method: str = "user_level",
        fixed_max_cells: Optional[int] = None
    ):
        """
        Initialize the GlobalSensitivityCalculator.
        
        Args:
            spark: SparkSession for distributed computation.
            method: Sensitivity calculation method:
                - "local": Assume each card in exactly 1 cell (sensitivity = K) [UNSAFE]
                - "global": Compute max cells per card from data (sensitivity = sqrt(M) * K)
                - "fixed": Use fixed max_cells value (sensitivity = sqrt(fixed_max_cells) * K)
                - "per_query": Different sensitivity for each query type
                - "user_level": Full user-level DP with per-query sensitivities [RECOMMENDED]
            fixed_max_cells: Maximum cells per individual for "fixed" method.
        """
        if method not in self.SENSITIVITY_METHODS:
            raise ValueError(f"Method must be one of {self.SENSITIVITY_METHODS}")
        
        self.spark = spark
        self.method = method
        self.fixed_max_cells = fixed_max_cells
        
        # Cache computed values
        self._max_cells_per_card: Optional[int] = None
        self._cells_distribution: Optional[Dict[int, int]] = None
        self._percentile_cells: Dict[int, int] = {}  # p50, p90, p99 cells per card
        
        # User-level sensitivity cache
        self._user_level_sensitivities: Dict[str, UserLevelSensitivity] = {}
        
        # Stratum sensitivities for stratified noise
        self._stratum_sensitivities: Dict[str, StratumSensitivity] = {}
        
        logger.info(f"GlobalSensitivityCalculator initialized: method={method}")
    
    def compute_max_cells_per_individual(
        self,
        df: 'DataFrame',
        individual_column: str = "card_number",
        cell_columns: list = None
    ) -> int:
        """
        Compute the maximum number of distinct cells any individual appears in.
        
        This is D_max in the sensitivity formula: L2 = sqrt(D_max) * K
        
        Args:
            df: DataFrame with transaction data.
            individual_column: Column identifying individuals (e.g., card_number).
            cell_columns: Columns defining a cell (e.g., city, mcc, day).
        
        Returns:
            Maximum number of cells any individual appears in.
        """
        if cell_columns is None:
            cell_columns = ["acceptor_city", "mcc", "day_idx"]
            
        if not HAS_SPARK:
            raise RuntimeError("Spark not available for sensitivity computation")
            
        logger.info(f"Computing max cells per card (user-level DP)")
        logger.info(f"  Individual column: {individual_column}")
        logger.info(f"  Cell columns: {cell_columns}")
        
        # Count distinct cells per individual
        cells_per_individual = df.groupBy(individual_column).agg(
            F.countDistinct(*cell_columns).alias("num_cells")
        )
        
        # Cache for multiple aggregations
        cells_per_individual.cache()
        
        # Get max and percentiles
        stats = cells_per_individual.agg(
            F.max(F.col("num_cells")).alias("max"),
            F.expr("percentile_approx(num_cells, 0.5)").alias("p50"),
            F.expr("percentile_approx(num_cells, 0.9)").alias("p90"),
            F.expr("percentile_approx(num_cells, 0.95)").alias("p95"),
            F.expr("percentile_approx(num_cells, 0.99)").alias("p99"),
            F.mean("num_cells").alias("mean"),
            F.count("*").alias("num_cards")
        ).first()
        
        self._max_cells_per_card = int(stats["max"]) if stats["max"] else 1
        self._percentile_cells = {
            50: int(stats["p50"]) if stats["p50"] else 1,
            90: int(stats["p90"]) if stats["p90"] else 1,
            95: int(stats["p95"]) if stats["p95"] else 1,
            99: int(stats["p99"]) if stats["p99"] else 1,
        }
        
        # Uncache
        cells_per_individual.unpersist()
        
        logger.info(f"User-level sensitivity analysis:")
        logger.info(f"  Total cards: {stats['num_cards']:,}")
        logger.info(f"  Mean cells per card: {stats['mean']:.2f}")
        logger.info(f"  Median (p50) cells: {self._percentile_cells[50]}")
        logger.info(f"  P90 cells: {self._percentile_cells[90]}")
        logger.info(f"  P99 cells: {self._percentile_cells[99]}")
        logger.info(f"  Max cells (D_max): {self._max_cells_per_card}")
        logger.info(f"  sqrt(D_max) = {math.sqrt(self._max_cells_per_card):.4f}")
        
        return self._max_cells_per_card
    
    def compute_cells_distribution(
        self,
        df: 'DataFrame',
        individual_column: str = "card_number",
        cell_columns: list = None
    ) -> Dict[int, int]:
        """
        Compute distribution of cells per individual.
        
        This helps understand the sensitivity landscape:
        - Most cards appear in few cells (low sensitivity)
        - Few cards appear in many cells (high sensitivity, drives D_max)
        
        Args:
            df: DataFrame with transaction data.
            individual_column: Column identifying individuals.
            cell_columns: Columns defining a cell.
        
        Returns:
            Dictionary mapping num_cells -> count of individuals.
        """
        if cell_columns is None:
            cell_columns = ["acceptor_city", "mcc", "day_idx"]
            
        if not HAS_SPARK:
            raise RuntimeError("Spark not available")
            
        logger.info("Computing cells distribution per card (for sensitivity analysis)")
        
        cells_per_individual = df.groupBy(individual_column).agg(
            F.countDistinct(*cell_columns).alias("num_cells")
        )
        
        # Cache intermediate result to avoid recomputation during groupBy shuffle
        cells_per_individual.cache()
        
        # Group by num_cells and count - final result is small (distinct num_cells values)
        # Use toLocalIterator() for memory efficiency to prevent OOM with thousands of unique values
        distribution_df = cells_per_individual.groupBy("num_cells").count()
        
        # Uncache to free memory
        cells_per_individual.unpersist()
        
        # Stream results to driver instead of collecting all at once
        self._cells_distribution = {row["num_cells"]: row["count"] for row in distribution_df.toLocalIterator()}
        
        # Log summary statistics
        total_individuals = sum(self._cells_distribution.values())
        max_cells = max(self._cells_distribution.keys()) if self._cells_distribution else 0
        
        # Compute what fraction of cards drive the high sensitivity
        cards_at_max = self._cells_distribution.get(max_cells, 0)
        pct_at_max = 100 * cards_at_max / total_individuals if total_individuals > 0 else 0
        
        logger.info(f"Cells distribution summary:")
        logger.info(f"  Total cards: {total_individuals:,}")
        logger.info(f"  Max cells per card: {max_cells}")
        logger.info(f"  Cards at max: {cards_at_max:,} ({pct_at_max:.4f}%)")
        
        return self._cells_distribution
    
    def compute_l2_sensitivity(
        self,
        df: 'DataFrame',
        k_bound: int = 1,
        individual_column: str = "card_number",
        cell_columns: list = None
    ) -> float:
        """
        Compute the global L2 sensitivity for counting queries under user-level DP.
        
        User-Level L2 Sensitivity Formula:
            Delta_2 = sqrt(D_max) * K
        where:
            D_max = maximum number of distinct cells any card appears in
            K = bounded contribution per cell (max transactions per card per cell)
        
        Args:
            df: DataFrame with transaction data.
            k_bound: Per-cell contribution bound.
            individual_column: Column identifying individuals.
            cell_columns: Columns defining a cell.
        
        Returns:
            Global L2 sensitivity for user-level DP.
        """
        if cell_columns is None:
            cell_columns = ["acceptor_city", "mcc", "day_idx"]
            
        if self.method == "local":
            # UNSAFE: Assumes each card in exactly 1 cell
            # Only use for testing or if you're certain about data structure
            sensitivity = float(k_bound)
            logger.warning(f"Using LOCAL sensitivity ({sensitivity}) - this may underestimate true sensitivity!")
            return sensitivity
        
        elif self.method == "fixed":
            if self.fixed_max_cells is None:
                raise ValueError("fixed_max_cells must be set for 'fixed' method")
            sensitivity = math.sqrt(self.fixed_max_cells) * k_bound
            logger.info(f"Fixed user-level sensitivity (D_max={self.fixed_max_cells}, K={k_bound}): {sensitivity:.4f}")
            return sensitivity
        
        elif self.method in ("global", "user_level"):
            # Compute D_max from data
            if self._max_cells_per_card is None:
                self.compute_max_cells_per_individual(df, individual_column, cell_columns)
            
            sensitivity = math.sqrt(self._max_cells_per_card) * k_bound
            logger.info(f"User-level L2 sensitivity:")
            logger.info(f"  D_max (max cells per card): {self._max_cells_per_card}")
            logger.info(f"  K (per-cell bound): {k_bound}")
            logger.info(f"  L2 = sqrt({self._max_cells_per_card}) * {k_bound} = {sensitivity:.4f}")
            return sensitivity
        
        elif self.method == "per_query":
            # For per_query, return max sensitivity; specific queries use compute_user_level_sensitivities
            if self._max_cells_per_card is None:
                self.compute_max_cells_per_individual(df, individual_column, cell_columns)
            sensitivity = math.sqrt(self._max_cells_per_card) * k_bound
            return sensitivity
        
        else:
            raise ValueError(f"Unknown method: {self.method}")
    
    def compute_sensitivities_per_query(
        self,
        df: 'DataFrame',
        k_bound: int = 1,
        winsorize_cap: float = 1000000.0,
        individual_column: str = "card_number",
        cell_columns: list = None
    ) -> Dict[str, float]:
        """
        Compute L2 sensitivity for each query type under USER-LEVEL DP.
        
        This is the correct sensitivity analysis for protecting card histories.
        
        Args:
            df: DataFrame with transaction data.
            k_bound: Per-cell contribution bound for transaction_count.
            winsorize_cap: Cap for amount (sensitivity for total_amount).
            individual_column: Column identifying individuals.
            cell_columns: Columns defining a cell.
        
        Returns:
            Dictionary mapping query name to its L2 sensitivity.
        """
        if cell_columns is None:
            cell_columns = ["acceptor_city", "mcc", "day_idx"]
            
        # First compute max cells if needed
        if self._max_cells_per_card is None:
            self.compute_max_cells_per_individual(df, individual_column, cell_columns)
        
        D_max = self._max_cells_per_card
        sqrt_D = math.sqrt(D_max)
        
        sensitivities = {
            # transaction_count: 
            # - Card C appears in D cells
            # - In each cell, C contributes at most K transactions
            # - Removing C changes D cells, each by at most K
            # - L2 = sqrt(sum of squared changes) = sqrt(D * K^2) = sqrt(D) * K
            "transaction_count": sqrt_D * k_bound,
            
            # unique_cards:
            # - Card C appears in D cells
            # - In each cell, C contributes exactly 1 to unique_cards count
            # - Removing C changes D cells, each by exactly 1
            # - L2 = sqrt(D * 1^2) = sqrt(D)
            "unique_cards": sqrt_D * 1.0,
            
            # unique_acceptors:
            # - Card C visits some acceptors across D cells
            # - Removing C only changes unique_acceptors if C was the ONLY card using that acceptor
            # - Worst case: C uses a different unique acceptor in each of D cells
            # - Each affected cell changes by at most 1
            # - L2 = sqrt(D * 1^2) = sqrt(D) (conservative)
            "unique_acceptors": sqrt_D * 1.0,
            
            # total_amount:
            # - Card C has transactions in D cells
            # - In each cell, C's total is at most winsorize_cap (per cell, per card)
            # - Removing C changes D cells, each by at most cap
            # - L2 = sqrt(D * cap^2) = sqrt(D) * cap
            "total_amount": sqrt_D * winsorize_cap,
        }
        
        logger.info("=" * 60)
        logger.info("User-Level L2 Sensitivities")
        logger.info("=" * 60)
        logger.info(f"D_max (max cells per card): {D_max}")
        logger.info(f"sqrt(D_max): {sqrt_D:.4f}")
        logger.info(f"K (per-cell transaction bound): {k_bound}")
        logger.info(f"Amount cap (per-cell): {winsorize_cap:,.2f}")
        logger.info("")
        for query, sens in sensitivities.items():
            logger.info(f"  {query:20s}: L2 = {sens:,.4f}")
        logger.info("=" * 60)
        
        return sensitivities
    
    def compute_user_level_sensitivities(
        self,
        df: 'DataFrame',
        k_bound: int = 1,
        winsorize_cap: float = 1000000.0,
        individual_column: str = "card_number",
        cell_columns: list = None
    ) -> Dict[str, UserLevelSensitivity]:
        """
        Compute detailed user-level sensitivity objects for each query.
        
        Returns UserLevelSensitivity objects with both L1 and L2 sensitivities,
        plus helper methods for computing sigma from rho.
        
        Args:
            df: DataFrame with transaction data.
            k_bound: Per-cell contribution bound.
            winsorize_cap: Cap for amount queries.
            individual_column: Column identifying individuals.
            cell_columns: Columns defining a cell.
            
        Returns:
            Dictionary mapping query name to UserLevelSensitivity object.
        """
        if cell_columns is None:
            cell_columns = ["acceptor_city", "mcc", "day_idx"]
        
        if self._max_cells_per_card is None:
            self.compute_max_cells_per_individual(df, individual_column, cell_columns)
        
        D_max = self._max_cells_per_card
        sqrt_D = math.sqrt(D_max)
        
        self._user_level_sensitivities = {
            "transaction_count": UserLevelSensitivity(
                query_name="transaction_count",
                max_cells_per_user=D_max,
                per_cell_contribution=float(k_bound),
                l2_sensitivity=sqrt_D * k_bound,
                l1_sensitivity=D_max * k_bound,
                sensitivity_type="count"
            ),
            "unique_cards": UserLevelSensitivity(
                query_name="unique_cards",
                max_cells_per_user=D_max,
                per_cell_contribution=1.0,
                l2_sensitivity=sqrt_D,
                l1_sensitivity=float(D_max),
                sensitivity_type="unique"
            ),
            "unique_acceptors": UserLevelSensitivity(
                query_name="unique_acceptors",
                max_cells_per_user=D_max,
                per_cell_contribution=1.0,
                l2_sensitivity=sqrt_D,
                l1_sensitivity=float(D_max),
                sensitivity_type="unique"
            ),
            "total_amount": UserLevelSensitivity(
                query_name="total_amount",
                max_cells_per_user=D_max,
                per_cell_contribution=winsorize_cap,
                l2_sensitivity=sqrt_D * winsorize_cap,
                l1_sensitivity=D_max * winsorize_cap,
                sensitivity_type="sum"
            ),
        }
        
        logger.info("Computed UserLevelSensitivity objects:")
        for name, sens in self._user_level_sensitivities.items():
            logger.info(f"  {sens}")
        
        return self._user_level_sensitivities
    
    def get_sensitivity_for_query(
        self,
        query_name: str,
        k_bound: int = 1,
        winsorize_cap: float = 1000000.0
    ) -> float:
        """
        Get the L2 sensitivity for a specific query.
        
        Must call compute_max_cells_per_individual first.
        
        Args:
            query_name: Name of the query.
            k_bound: Per-cell contribution bound.
            winsorize_cap: Cap for amount queries.
            
        Returns:
            L2 sensitivity for the query.
        """
        if self._max_cells_per_card is None:
            raise ValueError("Must call compute_max_cells_per_individual first")
        
        D_max = self._max_cells_per_card
        sqrt_D = math.sqrt(D_max)
        
        if query_name == "transaction_count":
            return sqrt_D * k_bound
        elif query_name in ("unique_cards", "unique_acceptors"):
            return sqrt_D * 1.0
        elif query_name == "total_amount":
            return sqrt_D * winsorize_cap
        else:
            # Unknown query - use conservative estimate
            logger.warning(f"Unknown query '{query_name}', using count sensitivity")
            return sqrt_D * k_bound
    
    def get_sensitivity_report(self) -> Dict:
        """
        Generate a report of sensitivity calculations.
        
        Returns:
            Dictionary with sensitivity calculation details.
        """
        report = {
            "method": self.method,
            "max_cells_per_individual": self._max_cells_per_card,
            "cells_distribution": self._cells_distribution,
        }
        
        if self._max_cells_per_card:
            report["sqrt_M"] = math.sqrt(self._max_cells_per_card)
        
        if self._cells_distribution:
            total = sum(self._cells_distribution.values())
            report["total_individuals"] = total
            
            # Compute percentiles
            sorted_items = sorted(self._cells_distribution.items())
            cumsum = 0
            for num_cells, count in sorted_items:
                cumsum += count
                if cumsum >= total * 0.50 and "median_cells" not in report:
                    report["median_cells"] = num_cells
                if cumsum >= total * 0.90 and "p90_cells" not in report:
                    report["p90_cells"] = num_cells
                if cumsum >= total * 0.99 and "p99_cells" not in report:
                    report["p99_cells"] = num_cells
        
        return report


class SensitivityBudgetOptimizer:
    """
    Optimizes privacy budget allocation based on sensitivity.
    
    Higher sensitivity queries should get more budget to maintain utility.
    This follows the principle: allocate budget proportional to sensitivity^2.
    """
    
    def __init__(self, total_rho: float):
        self.total_rho = total_rho
    
    def optimize_allocation(
        self,
        sensitivities: Dict[str, float],
        weights: Optional[Dict[str, float]] = None
    ) -> Dict[str, float]:
        """
        Compute optimal budget allocation across queries.
        
        Args:
            sensitivities: Dictionary of query sensitivities.
            weights: Optional importance weights for queries.
        
        Returns:
            Dictionary mapping query names to their rho allocation.
        """
        if weights is None:
            weights = {q: 1.0 for q in sensitivities}
        
        # Allocate proportional to (sensitivity * weight)^2
        # This minimizes total MSE
        weighted_sens_sq = {
            q: (sensitivities[q] * weights.get(q, 1.0)) ** 2
            for q in sensitivities
        }
        
        total_weighted = sum(weighted_sens_sq.values())
        
        if total_weighted == 0:
            # Equal allocation if all sensitivities are 0
            n = len(sensitivities)
            return {q: self.total_rho / n for q in sensitivities}
        
        allocation = {
            q: self.total_rho * (ws / total_weighted)
            for q, ws in weighted_sens_sq.items()
        }
        
        logger.info("Optimized budget allocation:")
        for q, rho in allocation.items():
            logger.info(f"  {q}: rho={rho:.6f} (sensitivity={sensitivities[q]:.4f})")
        
        return allocation
    
    def optimize_for_user_level(
        self,
        user_sensitivities: Dict[str, UserLevelSensitivity],
        query_weights: Optional[Dict[str, float]] = None
    ) -> Dict[str, Tuple[float, float]]:
        """
        Optimize budget allocation for user-level DP.
        
        Returns both rho allocation and resulting sigma for each query.
        
        Args:
            user_sensitivities: UserLevelSensitivity objects per query.
            query_weights: Importance weights (higher = more budget).
            
        Returns:
            Dict mapping query_name -> (rho, sigma)
        """
        if query_weights is None:
            query_weights = {q: 1.0 for q in user_sensitivities}
        
        # Extract L2 sensitivities
        sensitivities = {
            q: s.l2_sensitivity 
            for q, s in user_sensitivities.items()
        }
        
        # Get rho allocations
        rho_allocation = self.optimize_allocation(sensitivities, query_weights)
        
        # Compute resulting sigmas
        result = {}
        for query_name, rho in rho_allocation.items():
            sens = user_sensitivities[query_name]
            sigma = sens.get_sigma_for_rho(rho)
            result[query_name] = (rho, sigma)
        
        return result


class StratumSensitivityCalculator:
    """
    Computes sensitivity for different strata (MCC_group, city_tier, day_type).
    
    Different strata have different sensitivity characteristics:
    - High-amount MCCs have higher sum sensitivity
    - High-volume cities have more cards, potentially higher D_max
    - Weekdays vs weekends may have different transaction patterns
    
    Key insight: Strata are DISJOINT, so parallel composition applies.
    We can use the FULL budget for each stratum without additive cost.
    """
    
    def __init__(self, spark: 'SparkSession' = None):
        self.spark = spark
        self._strata: Dict[str, StratumSensitivity] = {}
    
    def compute_stratum_sensitivities(
        self,
        df: 'DataFrame',
        mcc_group_col: str = "mcc_group",
        city_tier_col: str = "city_tier",
        day_type_col: str = "day_type",
        card_col: str = "card_number",
        amount_col: str = "amount",
        cell_columns: list = None,
        k_bound: int = 1,
        amount_percentile: float = 99.0
    ) -> Dict[str, StratumSensitivity]:
        """
        Compute sensitivity for each stratum.
        
        Args:
            df: DataFrame with stratum columns.
            mcc_group_col: Column for MCC group.
            city_tier_col: Column for city tier.
            day_type_col: Column for day type.
            card_col: Column for card identifier.
            amount_col: Column for transaction amount.
            cell_columns: Columns defining a cell within stratum.
            k_bound: Per-cell contribution bound.
            amount_percentile: Percentile for amount cap.
            
        Returns:
            Dict mapping stratum_id -> StratumSensitivity.
        """
        if not HAS_SPARK:
            raise RuntimeError("Spark not available")
            
        if cell_columns is None:
            cell_columns = ["acceptor_city", "mcc", "day_idx"]
        
        logger.info("Computing per-stratum sensitivities...")
        
        # Get unique strata
        strata = df.select(mcc_group_col, city_tier_col, day_type_col).distinct().collect()
        
        for row in strata:
            mcc_group = str(row[mcc_group_col]) if row[mcc_group_col] else "unknown"
            city_tier = str(row[city_tier_col]) if row[city_tier_col] else "unknown"
            day_type = str(row[day_type_col]) if row[day_type_col] else "unknown"
            stratum_id = f"{mcc_group}_{city_tier}_{day_type}"
            
            # Filter to this stratum
            stratum_df = df.filter(
                (F.col(mcc_group_col) == row[mcc_group_col]) &
                (F.col(city_tier_col) == row[city_tier_col]) &
                (F.col(day_type_col) == row[day_type_col])
            )
            
            # Compute stratum statistics
            stats = stratum_df.agg(
                F.countDistinct(*cell_columns).alias("num_cells"),
                F.countDistinct(card_col).alias("num_cards"),
                F.mean(amount_col).alias("typical_amount"),
                F.expr(f"percentile_approx({amount_col}, {amount_percentile/100})").alias("amount_cap")
            ).first()
            
            # Compute max cells per card within this stratum
            cells_per_card = stratum_df.groupBy(card_col).agg(
                F.countDistinct(*cell_columns).alias("num_cells")
            )
            max_cells = cells_per_card.agg(F.max(F.col("num_cells"))).first()[0] or 1
            
            # Create stratum sensitivity
            stratum_sens = StratumSensitivity(
                stratum_id=stratum_id,
                mcc_group=mcc_group,
                city_tier=city_tier,
                day_type=day_type,
                num_cells=int(stats["num_cells"] or 0),
                num_cards=int(stats["num_cards"] or 0),
                max_cells_per_card=int(max_cells),
                typical_amount=float(stats["typical_amount"] or 0),
                amount_cap=float(stats["amount_cap"] or 0)
            )
            stratum_sens.compute_sensitivities(k_bound)
            
            self._strata[stratum_id] = stratum_sens
            
            logger.info(f"  {stratum_id}: D_max={max_cells}, cap={stats['amount_cap']:,.0f}")
        
        return self._strata
    
    def get_stratum_sensitivity(
        self,
        mcc_group: str,
        city_tier: str,
        day_type: str,
        query_name: str
    ) -> float:
        """Get L2 sensitivity for a specific stratum and query."""
        stratum_id = f"{mcc_group}_{city_tier}_{day_type}"
        
        if stratum_id not in self._strata:
            logger.warning(f"Unknown stratum {stratum_id}, using max sensitivity")
            # Return max across all known strata
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

