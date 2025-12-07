"""
Bounded Contribution for Differential Privacy.

Implements contribution bounding to ensure correct sensitivity for DP queries.
Each card's contribution per cell (city, mcc, day) is limited to K.

Methods:
- IQR: K = ceil(Q3 + 1.5 * IQR) - statistical outlier detection
- Percentile: K = percentile(contributions, p)
- Fixed: K = user-specified value

Reference: Census 2020 also bounds household contributions.
"""

import logging
import math
from typing import Optional, Tuple, Dict, Any
from dataclasses import dataclass

import numpy as np

try:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    from pyspark.sql.types import IntegerType, LongType
    HAS_SPARK = True
except ImportError:
    HAS_SPARK = False


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
    Calculates and applies bounded contribution.
    
    Ensures each card contributes at most K transactions per cell (city, mcc, day).
    K is calculated using IQR method by default to exclude outliers.
    """
    
    def __init__(
        self,
        method: str = 'iqr',
        iqr_multiplier: float = 1.5,
        fixed_k: int = 5,
        percentile: float = 99.0
    ):
        """
        Initialize calculator.
        
        Args:
            method: 'iqr', 'percentile', or 'fixed'
            iqr_multiplier: Multiplier for IQR method (default 1.5)
            fixed_k: Fixed K value if method='fixed'
            percentile: Percentile if method='percentile'
        """
        self.method = method
        self.iqr_multiplier = iqr_multiplier
        self.fixed_k = fixed_k
        self.percentile = percentile
        
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
        
        # Count transactions per card-cell
        cell_counts = df.groupBy(card_col, city_col, mcc_col, day_col).agg(
            F.count('*').alias('tx_count')
        )
        
        if self.method == 'fixed':
            self._computed_k = self.fixed_k
            self._stats = {'fixed_k': float(self.fixed_k)}
            logger.info(f"  Using fixed K = {self._computed_k}")
            return self._computed_k
        
        # Compute statistics
        stats = cell_counts.select(
            F.min('tx_count').alias('min'),
            F.max('tx_count').alias('max'),
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
        
        logger.info(f"  Statistics: {self._stats}")
        
        if self.method == 'iqr':
            q1 = self._stats['q1']
            q3 = self._stats['q3']
            iqr = q3 - q1
            upper_bound = q3 + self.iqr_multiplier * iqr
            self._computed_k = max(1, int(math.ceil(upper_bound)))
            self._stats['iqr'] = iqr
            self._stats['upper_bound'] = upper_bound
            logger.info(f"  IQR = {iqr:.2f}, Upper bound = {upper_bound:.2f}")
            
        elif self.method == 'percentile':
            self._computed_k = max(1, int(math.ceil(self._stats[f'p{int(self.percentile)}'])))
            
        else:
            raise ValueError(f"Unknown method: {self.method}")
        
        logger.info(f"  Computed K = {self._computed_k}")
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
        
        if self.method == 'iqr':
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
        order_col: str = 'transaction_date'
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
            
        Returns:
            Tuple of (clipped DataFrame, result summary)
        """
        if not HAS_SPARK:
            raise RuntimeError("Spark not available")
        
        if k is None:
            if self._computed_k is None:
                raise ValueError("K not computed. Call compute_k_from_spark first or provide k.")
            k = self._computed_k
        
        logger.info(f"Clipping contributions to K = {k}")
        
        # For very large datasets (4.5B+ rows), skip expensive count() operations
        # The window function with row_number() is already very expensive and necessary
        # Count operations force full materialization which can take hours on 4.5B rows
        # Make counts optional - they're only used for logging statistics
        # 
        # To skip counts for performance, set this to True:
        # skip_counts = True  # Recommended for datasets > 1B rows
        skip_counts = False
        
        if skip_counts:
            logger.info("  Skipping record counts for performance (very large dataset)")
            records_before = None
            records_after = None
            records_clipped = None
        else:
            # Count before clipping (expensive on 4.5B rows - can take 10+ minutes)
            logger.info("  Counting records before clipping (this may take time on large datasets)...")
            records_before = df.count()
        
        # Add row number within each card-cell, ordered by transaction date
        # NOTE: This window function is VERY expensive on 4.5B rows but necessary for correctness
        # It requires sorting within partitions, which is one of the most expensive Spark operations
        # Consider checkpointing the DataFrame before this operation for fault tolerance
        logger.info("  Applying window function with row_number() (expensive operation on large datasets)...")
        window = Window.partitionBy(card_col, city_col, mcc_col, day_col).orderBy(order_col)
        
        df_with_rownum = df.withColumn('_row_num', F.row_number().over(window))
        
        # Keep only first K transactions per card-cell
        df_clipped = df_with_rownum.filter(F.col('_row_num') <= k).drop('_row_num')
        
        # Count after clipping (also expensive)
        if not skip_counts and records_before is not None:
            logger.info("  Counting records after clipping (this may take time on large datasets)...")
            records_after = df_clipped.count()
            records_clipped = records_before - records_after
        elif skip_counts:
            records_after = None
            records_clipped = None
        
        result = ContributionBoundResult(
            k=k,
            method=self.method,
            stats=self._stats.copy(),
            records_before=records_before or 0,
            records_after=records_after or 0,
            records_clipped=records_clipped or 0
        )
        
        if records_before is not None:
            logger.info(f"  Records before: {records_before:,}")
            logger.info(f"  Records after:  {records_after:,}")
            if records_clipped is not None and records_before > 0:
                logger.info(f"  Records clipped: {records_clipped:,} ({records_clipped/records_before*100:.2f}%)")
            else:
                logger.info(f"  Records clipped: {records_clipped:,}")
        else:
            logger.info("  Record counts skipped for performance (very large dataset)")
        
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
