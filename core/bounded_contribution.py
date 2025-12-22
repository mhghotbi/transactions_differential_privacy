"""
Bounded Contribution for Statistical Disclosure Control.

Implements contribution bounding to prevent extreme outliers from dominating statistics
and to improve SDC protection in secure enclave deployments.

Each card's contribution per cell (city, mcc, day) is limited to K transactions.

WHY BOUND CONTRIBUTIONS (SDC context):
- Prevents single cards with thousands of transactions from skewing aggregates
- Improves utility by reducing impact of outliers on noise calibration
- Maintains realistic statistics (e.g., one card shouldn't represent 90% of a cell)
- Complements plausibility bounds for better disclosure control

Methods:
- Transaction-Weighted Percentile (RECOMMENDED): K where p% of TRANSACTIONS are kept
  Minimizes data loss by finding K such that cumulative transactions / total ≥ p/100
  Example: p=99 means keep 99% of transactions (~1% data loss)
  
- Cell Percentile: K = p-th percentile of cell counts (transactions per card-cell)
  Keeps p% of CELLS intact but can lose 50%+ of transactions if distribution is skewed
  Example: p=99, K=10 means 99% of cells have ≤10 txns, but high-volume cells get clipped
  
- IQR: K = ceil(Q3 + 1.5 * IQR) - statistical outlier detection
  
- Fixed: K = user-specified value
"""

import logging
import math
from typing import Optional, Tuple, Dict, Any, TYPE_CHECKING
from dataclasses import dataclass

import numpy as np

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

try:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    from pyspark.sql.types import IntegerType, LongType
    HAS_SPARK = True
except ImportError:
    HAS_SPARK = False
    DataFrame = None  # type: ignore
    SparkSession = None  # type: ignore


logger = logging.getLogger(__name__)


@dataclass
class ContributionBoundResult:
    """Result of contribution bound calculation."""
    k: int                      # The computed/configured bound K
    method: str                 # Method used: 'iqr', 'percentile', 'fixed'
    stats: Dict[str, float]     # Statistics: q1, q3, iqr, max, etc.
    records_before: int         # Records before clipping
    records_after: int          # Records after clipping
    records_clipped: int        # Records removed by clipping
    
    def summary(self) -> str:
        """Generate summary string."""
        pct_clipped = (self.records_clipped / self.records_before * 100) if self.records_before > 0 else 0
        lines = [
            "=" * 60,
            "Bounded Contribution Summary",
            "=" * 60,
            f"Method: {self.method}",
            f"K (max contribution per card-cell): {self.k}",
            "",
            "Statistics:",
        ]
        for key, value in self.stats.items():
            lines.append(f"  {key}: {value:.2f}")
        
        lines.extend([
            "",
            f"Records before clipping: {self.records_before:,}",
            f"Records after clipping:  {self.records_after:,}",
            f"Records clipped:         {self.records_clipped:,} ({pct_clipped:.2f}%)",
            "=" * 60,
        ])
        return "\n".join(lines)


class BoundedContributionCalculator:
    """
    Calculates and applies bounded contribution for SDC.
    
    Ensures each card contributes at most K transactions per cell (city, mcc, day).
    This prevents extreme outliers from dominating statistics and improves utility.
    
    K is calculated using statistical methods to balance data retention and outlier prevention.
    """
    
    def __init__(
        self,
        method: str = 'iqr',
        iqr_multiplier: float = 1.5,
        fixed_k: int = 5,
        percentile: float = 99.0,
        compute_per_group: bool = True
    ):
        """
        Initialize bounded contribution calculator for SDC.
        
        Args:
            method: One of:
                - 'transaction_weighted_percentile': Keep p% of transactions (RECOMMENDED)
                  Minimizes data loss while removing extreme outliers
                - 'percentile': p-th percentile of cell counts (can lose many transactions)
                - 'iqr': Statistical outlier detection (Q3 + 1.5*IQR)
                - 'fixed': Use fixed K value
            iqr_multiplier: Multiplier for IQR method (default 1.5)
            fixed_k: Fixed K value if method='fixed'
            percentile: Target percentile (0-100) for percentile methods
            compute_per_group: If True, compute K per individual MCC, then take max.
                              Reduces memory for large datasets.
        """
        self.method = method
        self.iqr_multiplier = iqr_multiplier
        self.fixed_k = fixed_k
        self.percentile = percentile
        self.compute_per_group = compute_per_group
        
        self._computed_k: Optional[int] = None
        self._stats: Dict[str, float] = {}
    
    def compute_k_from_spark(
        self,
        df: DataFrame,
        card_col: str = 'card_number',
        city_col: str = 'acceptor_city',
        mcc_col: str = 'mcc',
        day_col: str = 'day_idx'
    ) -> int:
        """
        Compute K from Spark DataFrame using configured method.
        
        For large datasets (billions of records), if df has 'mcc_group' column
        and compute_per_group=True, will compute K per MCC group and take maximum.
        This reduces memory usage by processing smaller chunks.
        
        Args:
            df: Input DataFrame with transaction data
            card_col: Column name for card number
            city_col: Column name for city
            mcc_col: Column name for MCC
            day_col: Column name for day index
            
        Returns:
            Computed K value
        """
        if not HAS_SPARK:
            raise RuntimeError("Spark not available")
        
        logger.info(f"Computing contribution bound K using method: {self.method}")
        
        # IMPORTANT: K is ALWAYS computed globally (across ALL MCC groups)
        # This ensures transaction_weighted_percentile keeps exactly p% of ALL transactions
        # The compute_per_group flag only affects noise application, not K computation
        # [VERBOSE] logger.info("  Computing cell counts globally (groupBy card, city, mcc, day)...")
        cell_counts = df.groupBy(card_col, city_col, mcc_col, day_col).agg(
            F.count('*').alias('tx_count')
        )
        # [VERBOSE] logger.info("  ✓ Cell counts computed")
        
        if self.method == 'fixed':
            self._computed_k = self.fixed_k
            self._stats = {'fixed_k': float(self.fixed_k)}
            logger.info(f"  Using fixed K = {self._computed_k}")
            return self._computed_k
        
        # Compute statistics
        stats = cell_counts.select(
            F.min(F.col('tx_count')).alias('min'),
            F.max(F.col('tx_count')).alias('max'),
            F.mean('tx_count').alias('mean'),
            F.stddev('tx_count').alias('stddev'),
            F.expr('percentile_approx(tx_count, 0.25)').alias('q1'),
            F.expr('percentile_approx(tx_count, 0.5)').alias('median'),
            F.expr('percentile_approx(tx_count, 0.75)').alias('q3'),
            F.expr(f'percentile_approx(tx_count, {self.percentile / 100})').alias('pct')
        ).first()
        
        self._stats = {
            'min': float(stats['min']),
            'max': float(stats['max']),
            'mean': float(stats['mean']),
            'stddev': float(stats['stddev'] or 0),
            'q1': float(stats['q1']),
            'median': float(stats['median']),
            'q3': float(stats['q3']),
            f'p{int(self.percentile)}': float(stats['pct']),
        }
        
        # [VERBOSE] logger.info(f"  Statistics: {self._stats}")
        
        if self.method == 'transaction_weighted_percentile':
            # Transaction-weighted percentile: Find K such that p% of TRANSACTIONS are kept
            # MEMORY-EFFICIENT: Aggregate by tx_count first to avoid window over millions of rows
            # [VERBOSE] logger.info(f"  Using transaction-weighted percentile method (target: keep {self.percentile}% of transactions)")
            
            # Step 1: Aggregate - count how many cells have each tx_count value
            # This reduces millions of cells to ~hundreds/thousands of distinct values
            tx_count_freq = cell_counts.groupBy('tx_count').agg(
                F.count('*').alias('num_cells'),
                (F.col('tx_count') * F.count('*')).alias('total_txns_at_count')
            ).orderBy('tx_count')
            
            # Step 2: Collect aggregated data (small - typically < 10K rows)
            freq_data = tx_count_freq.collect()
            
            if not freq_data:
                logger.warning("  No cell count data, using default K=1")
                self._computed_k = 1
                return self._computed_k
            
            # Step 3: Compute cumulative sum in memory (efficient - O(distinct_tx_counts))
            total_txns = sum(row['total_txns_at_count'] for row in freq_data)
            total_cells = sum(row['num_cells'] for row in freq_data)
            
            cumsum_txns = 0
            self._computed_k = 1
            actual_retention = 0.0
            
            for row in freq_data:
                cumsum_txns += row['total_txns_at_count']
                retention_pct = (cumsum_txns / total_txns) * 100
                
                if retention_pct >= self.percentile:
                    self._computed_k = max(1, int(math.ceil(row['tx_count'])))
                    actual_retention = retention_pct
                    break
            else:
                # If we didn't break (shouldn't happen), use the last value
                self._computed_k = int(freq_data[-1]['tx_count'])
                actual_retention = 100.0
            
            self._stats['total_transactions'] = float(total_txns)
            self._stats['total_cells'] = float(total_cells)
            self._stats['target_retention_pct'] = self.percentile
            self._stats['actual_retention_pct'] = float(actual_retention)
            self._stats['estimated_data_loss_pct'] = 100.0 - float(actual_retention)
            
            # [VERBOSE] logger.info(f"  Total transactions: {total_txns:,}")
            # logger.info(f"  Total cells: {total_cells:,}")
            # logger.info(f"  Distinct tx_count values: {len(freq_data):,}")
            # logger.info(f"  Target retention: {self.percentile}%")
            logger.info(f"  Computed K = {self._computed_k} (keeps {actual_retention:.2f}% of transactions)")
            # logger.info(f"  Estimated retention: {actual_retention:.2f}% of transactions")
            # logger.info(f"  Estimated data loss: {100-actual_retention:.2f}%")
            
        elif self.method == 'iqr':
            q1 = self._stats['q1']
            q3 = self._stats['q3']
            iqr = q3 - q1
            upper_bound = q3 + self.iqr_multiplier * iqr
            self._computed_k = max(1, int(math.ceil(upper_bound)))
            self._stats['iqr'] = iqr
            self._stats['upper_bound'] = upper_bound
            # [VERBOSE] logger.info(f"  IQR = {iqr:.2f}, Upper bound = {upper_bound:.2f}")
            
        elif self.method == 'percentile':
            # Cell-based percentile (old method - can lose many transactions)
            logger.warning(
                f"  Using 'percentile' method which keeps {self.percentile}% of CELLS intact, "
                "but may lose 50%+ of transactions if distribution is skewed. "
                "Consider using 'transaction_weighted_percentile' to minimize data loss."
            )
            self._computed_k = max(1, int(math.ceil(self._stats[f'p{int(self.percentile)}'])))
            
        else:
            raise ValueError(f"Unknown method: {self.method}")
        
        # [VERBOSE] logger.info(f"  Computed K = {self._computed_k}")
        return self._computed_k
    
    def _compute_k_per_group(
        self,
        df: DataFrame,
        card_col: str,
        city_col: str,
        mcc_col: str,
        day_col: str
    ) -> int:
        """
        Compute K per individual MCC for memory efficiency.
        
        Strategy:
        1. Get all distinct MCC codes
        2. Compute K for each MCC: K₁, K₂, ..., Kₙ
        3. Take MAX(K₁, K₂, ..., Kₙ) as global K
        4. Return this K to be used globally
        
        Args:
            df: Input DataFrame
            card_col, city_col, mcc_col, day_col: Column names
            
        Returns:
            Maximum K across all MCCs
        """
        # [VERBOSE] logger.info(f"  Computing K per individual MCC for memory efficiency")
        
        # Get list of distinct MCCs
        mccs = df.select(mcc_col).distinct().rdd.flatMap(lambda x: x).collect()
        mccs = sorted([str(m) for m in mccs if m is not None])
        num_mccs = len(mccs)
        
        # [VERBOSE] logger.info(f"  Found {num_mccs} distinct MCC codes for K computation")
        
        # Handle edge case: no MCCs in data
        if num_mccs == 0:
            logger.warning("  No MCC codes found in data, using default K=1")
            self._computed_k = 1
            self._stats = {
                'num_mccs': 0,
                'k_values': [],
                'global_k': 1,
                'min_k': 1,
                'max_k': 1,
                'mean_k': 1.0,
                'mcc_stats': []
            }
            logger.info(f"  Global K (default): {self._computed_k}")
            return self._computed_k
        
        # [VERBOSE] logger.info(f"  Will compute K for each MCC, then take maximum")
        
        k_values = []
        mcc_stats = []
        
        for i, mcc_code in enumerate(mccs, 1):
            # [VERBOSE] logger.info(f"  [{i}/{num_mccs}] Processing MCC {mcc_code}...")
            
            # Filter to this MCC only
            df_mcc = df.filter(F.col(mcc_col) == mcc_code)
            
            # [VERBOSE] Count records in this MCC
            # mcc_count = df_mcc.count()
            # logger.info(f"    Records in MCC: {mcc_count:,}")
            
            # Compute cell counts for this MCC
            cell_counts = df_mcc.groupBy(card_col, city_col, mcc_col, day_col).agg(
                F.count('*').alias('tx_count')
            )
            
            # Compute K using the standard logic (same as global computation)
            if self.method == 'fixed':
                k_mcc = self.fixed_k
                stats_mcc = {'mcc': mcc_code, 'fixed_k': float(self.fixed_k)}
            else:
                # Compute statistics for this MCC
                stats = cell_counts.select(
                    F.min(F.col('tx_count')).alias('min'),
                    F.max(F.col('tx_count')).alias('max'),
                    F.mean('tx_count').alias('mean'),
                    F.stddev('tx_count').alias('stddev'),
                    F.expr('percentile_approx(tx_count, 0.25)').alias('q1'),
                    F.expr('percentile_approx(tx_count, 0.5)').alias('median'),
                    F.expr('percentile_approx(tx_count, 0.75)').alias('q3'),
                    F.expr(f'percentile_approx(tx_count, {self.percentile / 100})').alias('pct')
                ).first()
                
                stats_mcc = {
                    'mcc': mcc_code,
                    'min': float(stats['min']),
                    'max': float(stats['max']),
                    'mean': float(stats['mean']),
                    'median': float(stats['median']),
                }
                
                if self.method == 'transaction_weighted_percentile':
                    # Memory-efficient transaction-weighted percentile for this MCC
                    tx_count_freq = cell_counts.groupBy('tx_count').agg(
                        F.count('*').alias('num_cells'),
                        (F.col('tx_count') * F.count('*')).alias('total_txns_at_count')
                    ).orderBy('tx_count')
                    
                    freq_data = tx_count_freq.collect()
                    
                    if not freq_data:
                        k_mcc = 1
                    else:
                        total_txns = sum(row['total_txns_at_count'] for row in freq_data)
                        cumsum_txns = 0
                        k_mcc = 1
                        
                        for row in freq_data:
                            cumsum_txns += row['total_txns_at_count']
                            retention_pct = (cumsum_txns / total_txns) * 100
                            
                            if retention_pct >= self.percentile:
                                k_mcc = max(1, int(math.ceil(row['tx_count'])))
                                stats_mcc['retention_pct'] = float(retention_pct)
                                break
                        else:
                            k_mcc = int(freq_data[-1]['tx_count'])
                            stats_mcc['retention_pct'] = 100.0
                        
                        stats_mcc['total_transactions'] = float(total_txns)
                
                elif self.method == 'iqr':
                    q1 = stats_mcc['q1'] = float(stats['q1'])
                    q3 = stats_mcc['q3'] = float(stats['q3'])
                    iqr = q3 - q1
                    upper_bound = q3 + self.iqr_multiplier * iqr
                    k_mcc = max(1, int(math.ceil(upper_bound)))
                    stats_mcc['iqr'] = iqr
                    stats_mcc['upper_bound'] = upper_bound
                
                elif self.method == 'percentile':
                    k_mcc = max(1, int(math.ceil(float(stats['pct']))))
                    stats_mcc[f'p{int(self.percentile)}'] = float(stats['pct'])
                
                else:
                    raise ValueError(f"Unknown method: {self.method}")
            
            k_values.append(k_mcc)
            mcc_stats.append(stats_mcc)
            # [VERBOSE] logger.info(f"    K for MCC {mcc_code}: {k_mcc}")
        
        # Take maximum K across all MCCs
        self._computed_k = max(k_values)
        
        # Store aggregated statistics
        self._stats = {
            'num_mccs': num_mccs,
            'k_values': k_values,
            'global_k': self._computed_k,
            'min_k': min(k_values),
            'max_k': max(k_values),
            'mean_k': sum(k_values) / len(k_values),
            'mcc_stats': mcc_stats
        }
        
        # [VERBOSE] logger.info(f"  Per-MCC K values: {k_values}")
        logger.info(f"  Global K (maximum): {self._computed_k}")
        # logger.info(f"  K range: [{min(k_values)}, {max(k_values)}]")
        
        return self._computed_k
    
    def compute_k_from_numpy(self, contributions: np.ndarray) -> int:
        """
        Compute K from numpy array of contribution counts.
        
        Args:
            contributions: Array of transaction counts per card-cell
            
        Returns:
            Computed K value
        """
        if self.method == 'fixed':
            self._computed_k = self.fixed_k
            return self._computed_k
        
        self._stats = {
            'min': float(np.min(contributions)),
            'max': float(np.max(contributions)),
            'mean': float(np.mean(contributions)),
            'stddev': float(np.std(contributions)),
            'q1': float(np.percentile(contributions, 25)),
            'median': float(np.percentile(contributions, 50)),
            'q3': float(np.percentile(contributions, 75)),
        }
        
        if self.method == 'transaction_weighted_percentile':
            # Transaction-weighted percentile for numpy
            total_txns = np.sum(contributions)
            sorted_contributions = np.sort(contributions)
            cumsum = np.cumsum(sorted_contributions)
            retention_pct = (cumsum / total_txns) * 100
            
            # Find minimum K where retention >= target
            mask = retention_pct >= self.percentile
            if np.any(mask):
                self._computed_k = max(1, int(math.ceil(sorted_contributions[mask][0])))
                actual_retention = retention_pct[mask][0]
            else:
                self._computed_k = int(np.max(contributions))
                actual_retention = 100.0
            
            self._stats['total_transactions'] = float(total_txns)
            self._stats['total_cells'] = len(contributions)
            self._stats['target_retention_pct'] = self.percentile
            self._stats['actual_retention_pct'] = float(actual_retention)
            
        elif self.method == 'iqr':
            q1 = self._stats['q1']
            q3 = self._stats['q3']
            iqr = q3 - q1
            upper_bound = q3 + self.iqr_multiplier * iqr
            self._computed_k = max(1, int(math.ceil(upper_bound)))
            self._stats['iqr'] = iqr
            self._stats['upper_bound'] = upper_bound
            
        elif self.method == 'percentile':
            pct_value = np.percentile(contributions, self.percentile)
            self._computed_k = max(1, int(math.ceil(pct_value)))
            self._stats[f'p{int(self.percentile)}'] = float(pct_value)
        
        return self._computed_k
    
    def clip_contributions_spark(
        self,
        df: DataFrame,
        k: Optional[int] = None,
        card_col: str = 'card_number',
        city_col: str = 'acceptor_city',
        mcc_col: str = 'mcc',
        day_col: str = 'day_idx',
        order_col: str = 'transaction_date',
        skip_counts: bool = False
    ) -> Tuple[DataFrame, ContributionBoundResult]:
        """
        Clip contributions in Spark DataFrame to at most K per card-cell.
        
        Args:
            df: Input DataFrame
            k: Max contributions per cell (uses computed K if None)
            card_col: Card number column
            city_col: City column
            mcc_col: MCC column
            day_col: Day index column
            order_col: Column to order by when selecting which transactions to keep
            skip_counts: If True, skip expensive count() operations (for 10B+ row datasets)
                        These counts are ONLY for logging and do NOT affect DP correctness
            
        Returns:
            Tuple of (clipped DataFrame, result summary)
        """
        if not HAS_SPARK:
            raise RuntimeError("Spark not available")
        
        if k is None:
            if self._computed_k is None:
                raise ValueError("K not computed. Call compute_k_from_spark first or provide k.")
            k = self._computed_k
        
        # [VERBOSE] logger.info(f"Clipping contributions to K = {k}")
        
        # CRITICAL: The window function and clipping operations below are ESSENTIAL for DP correctness
        # They MUST run - no skipping or optimization that affects the actual clipping logic.
        # This implements the bounded contribution mechanism required for user-level DP.
        
        # Count before clipping (for statistics/logging only - does NOT affect DP correctness)
        # On 4.5B rows, this can take 10+ minutes but is only for reporting
        if skip_counts:
            # [VERBOSE] logger.info("  Skipping record counts (skip_counts=True for performance)")
            records_before = -1  # Sentinel value indicating count was skipped
        else:
            # [VERBOSE] logger.info("  Counting records before clipping (for statistics only - may take time on large datasets)...")
            records_before = df.count()
        
        # CRITICAL DP OPERATION: Window function with row_number()
        # This is ESSENTIAL for bounded contribution - it identifies which transactions to keep
        # within each (card, city, mcc, day) cell, ordered by transaction_date
        # This operation is expensive on 4.5B rows but MUST run for DP correctness
        # 
        # Performance note: This requires sorting within partitions, which is expensive.
        # Consider checkpointing before this operation for fault tolerance on very large datasets.
        # [VERBOSE] logger.info("  Applying window function with row_number() (CRITICAL for DP correctness)...")
        window = Window.partitionBy(card_col, city_col, mcc_col, day_col).orderBy(order_col)
        
        df_with_rownum = df.withColumn('_row_num', F.row_number().over(window))
        
        # CRITICAL DP OPERATION: Filter to keep only first K transactions per card-cell
        # This enforces the contribution bound K, which is required for user-level DP sensitivity
        # This operation MUST run - it directly affects the DP mechanism
        df_clipped = df_with_rownum.filter(F.col('_row_num') <= k).drop('_row_num')
        
        # Count after clipping (for statistics/logging only - does NOT affect DP correctness)
        if skip_counts:
            records_after = -1  # Sentinel value indicating count was skipped
            records_clipped = -1
            # [VERBOSE] logger.info("  Skipped record counts for performance (skip_counts=True)")
        else:
            # [VERBOSE] logger.info("  Counting records after clipping (for statistics only - may take time on large datasets)...")
            records_after = df_clipped.count()
            records_clipped = records_before - records_after
        
        result = ContributionBoundResult(
            k=k,
            method=self.method,
            stats=self._stats.copy(),
            records_before=records_before,
            records_after=records_after,
            records_clipped=records_clipped
        )
        
        # [VERBOSE] Log statistics (these counts are for monitoring only, not used in DP computation)
        # if not skip_counts:
        #     logger.info(f"  Records before clipping: {records_before:,}")
        #     logger.info(f"  Records after clipping:  {records_after:,}")
        #     if records_before > 0:
        #         logger.info(f"  Records clipped: {records_clipped:,} ({records_clipped/records_before*100:.2f}%)")
        #     else:
        #         logger.info(f"  Records clipped: {records_clipped:,}")
        
        return df_clipped, result
    
    @property
    def k(self) -> Optional[int]:
        """Get computed K value."""
        return self._computed_k
    
    @property
    def stats(self) -> Dict[str, float]:
        """Get statistics from computation."""
        return self._stats.copy()


def compute_and_clip_contributions(
    df: DataFrame,
    config: Any,
    card_col: str = 'card_number',
    city_col: str = 'acceptor_city',
    mcc_col: str = 'mcc',
    day_col: str = 'day_idx'
) -> Tuple[DataFrame, int, ContributionBoundResult]:
    """
    Convenience function to compute K and clip contributions in one call.
    
    Args:
        df: Input Spark DataFrame
        config: Config object with privacy settings
        card_col: Card number column
        city_col: City column
        mcc_col: MCC column
        day_col: Day index column
        
    Returns:
        Tuple of (clipped DataFrame, K, result summary)
    """
    calculator = BoundedContributionCalculator(
        method=config.privacy.contribution_bound_method,
        iqr_multiplier=config.privacy.contribution_bound_iqr_multiplier,
        fixed_k=config.privacy.contribution_bound_fixed,
        percentile=config.privacy.contribution_bound_percentile
    )
    
    # Compute K
    k = calculator.compute_k_from_spark(df, card_col, city_col, mcc_col, day_col)
    
    # Store in config
    config.privacy.computed_contribution_bound = k
    
    # Clip contributions
    df_clipped, result = calculator.clip_contributions_spark(
        df, k, card_col, city_col, mcc_col, day_col
    )
    
    return df_clipped, k, result
