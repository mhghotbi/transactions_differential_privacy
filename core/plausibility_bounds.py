"""
Plausibility Bounds Calculator for Context-Aware Noise.

Computes data-driven plausibility bounds per (Province, MCC, Weekday) context.
These bounds ensure that noisy outputs remain realistic based on historical data patterns.

Design Philosophy:
- Let the data define what's plausible for each context
- No manual/expert overrides needed
- Bounds are computed from percentiles (default p5-p95)
- Province-specific bounds align with province-specific scaling factors
"""

import logging
from typing import Optional, Dict, Tuple
from dataclasses import dataclass

try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql import functions as F
    from pyspark.sql.types import DoubleType
    HAS_SPARK = True
except ImportError:
    HAS_SPARK = False


logger = logging.getLogger(__name__)


@dataclass
class BoundsConfig:
    """Configuration for plausibility bounds computation."""
    lower_percentile: float = 0.05  # p5 for lower bound
    upper_percentile: float = 0.95  # p95 for upper bound
    min_samples_for_bounds: int = 3  # Minimum samples needed to compute bounds
    fallback_multiplier: float = 2.0  # For contexts with too few samples


class PlausibilityBoundsCalculator:
    """
    Compute plausibility bounds per (Province, MCC, Weekday) from historical data.
    
    The bounds define realistic min/max ranges for:
    - transaction_count: How many transactions are plausible
    - avg_amount: What average transaction amount is plausible
    - tx_per_card: How many transactions per unique card is plausible
    
    These bounds are used to clamp noisy values to realistic ranges.
    Province-specific bounds ensure alignment with province-specific scaling factors
    applied during invariant preservation.
    """
    
    def __init__(
        self,
        lower_percentile: float = 0.05,
        upper_percentile: float = 0.95,
        min_samples: int = 3
    ):
        """
        Initialize bounds calculator.
        
        Args:
            lower_percentile: Lower percentile for bounds (default 0.05 = p5)
            upper_percentile: Upper percentile for bounds (default 0.95 = p95)
            min_samples: Minimum samples needed per context to compute bounds
        """
        self.lower_pct = lower_percentile
        self.upper_pct = upper_percentile
        self.min_samples = min_samples
        
        self._bounds_df: Optional[DataFrame] = None
        self._global_bounds: Optional[Dict[str, Tuple[float, float]]] = None
        
        # [VERBOSE] logger.info(f"PlausibilityBoundsCalculator initialized:")
        # logger.info(f"  Lower percentile: p{int(lower_percentile * 100)}")
        # logger.info(f"  Upper percentile: p{int(upper_percentile * 100)}")
        # logger.info(f"  Min samples per context: {min_samples}")
    
    def compute_bounds(
        self,
        df: 'DataFrame',
        context_cols: list = None
    ) -> 'DataFrame':
        """
        Compute plausibility bounds per context from historical data.
        
        Args:
            df: DataFrame with aggregated data (must have transaction_count, 
                unique_cards, total_amount columns)
            context_cols: Columns defining context (default: ['province_idx', 'mcc_idx', 'weekday'])
        
        Returns:
            DataFrame with bounds per context
        """
        if not HAS_SPARK:
            raise RuntimeError("Spark not available")
        
        if context_cols is None:
            context_cols = ['province_idx', 'mcc_idx', 'weekday']
        
        # [VERBOSE] logger.info("=" * 60)
        # logger.info("Computing Data-Driven Plausibility Bounds")
        # logger.info("=" * 60)
        # logger.info(f"Context columns: {context_cols}")
        
        # First compute global bounds as fallback for sparse contexts
        self._compute_global_bounds(df)
        
        # Compute per-context bounds
        # Add derived ratio columns for bounds computation
        df_with_ratios = df.withColumn(
            'avg_amount',
            F.when(F.col('transaction_count') > 0,
                   F.col('total_amount') / F.col('transaction_count'))
             .otherwise(F.lit(0.0))
        ).withColumn(
            'tx_per_card',
            F.when(F.col('unique_cards') > 0,
                   F.col('transaction_count') / F.col('unique_cards'))
             .otherwise(F.lit(1.0))
        )
        
        # Group by context and compute percentile bounds
        bounds_df = df_with_ratios.groupBy(*context_cols).agg(
            # Sample count for this context
            F.count('*').alias('sample_count'),
            
            # Count bounds
            F.expr(f'percentile_approx(transaction_count, {self.lower_pct})').alias('count_min'),
            F.expr(f'percentile_approx(transaction_count, {self.upper_pct})').alias('count_max'),
            F.mean('transaction_count').alias('count_mean'),
            
            # Avg amount bounds
            F.expr(f'percentile_approx(avg_amount, {self.lower_pct})').alias('avg_amount_min'),
            F.expr(f'percentile_approx(avg_amount, {self.upper_pct})').alias('avg_amount_max'),
            F.mean('avg_amount').alias('avg_amount_mean'),
            
            # TX per card bounds
            F.expr(f'percentile_approx(tx_per_card, {self.lower_pct})').alias('tx_per_card_min'),
            F.expr(f'percentile_approx(tx_per_card, {self.upper_pct})').alias('tx_per_card_max'),
            F.mean('tx_per_card').alias('tx_per_card_mean'),
        )
        
        # For contexts with too few samples, use wider bounds based on mean
        # This prevents overly tight bounds from sparse data
        bounds_df = bounds_df.withColumn(
            'count_min',
            F.when(F.col('sample_count') < self.min_samples,
                   F.col('count_mean') * 0.1)  # 10% of mean as min
             .otherwise(F.col('count_min'))
        ).withColumn(
            'count_max',
            F.when(F.col('sample_count') < self.min_samples,
                   F.col('count_mean') * 3.0)  # 3x mean as max
             .otherwise(F.col('count_max'))
        )
        
        # Ensure minimums are at least 1 for counts
        bounds_df = bounds_df.withColumn(
            'count_min', F.greatest(F.lit(1.0), F.col('count_min'))
        ).withColumn(
            'tx_per_card_min', F.greatest(F.lit(1.0), F.col('tx_per_card_min'))
        ).withColumn(
            'avg_amount_min', F.greatest(F.lit(0.0), F.col('avg_amount_min'))
        )
        
        # Cache bounds for reuse
        self._bounds_df = bounds_df.cache()
        
        # Log summary statistics
        bounds_stats = self._bounds_df.agg(
            F.count('*').alias('num_contexts'),
            F.mean('sample_count').alias('avg_samples_per_context'),
            F.sum(F.when(F.col('sample_count') < self.min_samples, 1).otherwise(0)).alias('sparse_contexts')
        ).first()
        
        logger.info(f"Bounds computed for {bounds_stats['num_contexts']:,} contexts")
        # [VERBOSE] logger.info(f"Average samples per context: {bounds_stats['avg_samples_per_context']:.1f}")
        # logger.info(f"Sparse contexts (< {self.min_samples} samples): {bounds_stats['sparse_contexts']:,}")
        
        # [VERBOSE] Show sample bounds
        # logger.info("\nSample bounds (first 5 contexts):")
        # sample_bounds = self._bounds_df.select(
        #     *context_cols, 'sample_count', 
        #     'count_min', 'count_max',
        #     'avg_amount_min', 'avg_amount_max'
        # ).limit(5).collect()
        # 
        # for row in sample_bounds:
        #     ctx = '_'.join(str(row[c]) for c in context_cols)
        #     logger.info(f"  {ctx}: count=[{row['count_min']:.0f}, {row['count_max']:.0f}], "
        #                f"avg_amt=[{row['avg_amount_min']:.0f}, {row['avg_amount_max']:.0f}]")
        
        return self._bounds_df
    
    def _compute_global_bounds(self, df: 'DataFrame') -> None:
        """Compute global bounds as fallback for unknown contexts."""
        
        # [VERBOSE] logger.info("Computing global fallback bounds...")
        
        # Add ratio columns
        df_with_ratios = df.withColumn(
            'avg_amount',
            F.when(F.col('transaction_count') > 0,
                   F.col('total_amount') / F.col('transaction_count'))
             .otherwise(F.lit(0.0))
        ).withColumn(
            'tx_per_card',
            F.when(F.col('unique_cards') > 0,
                   F.col('transaction_count') / F.col('unique_cards'))
             .otherwise(F.lit(1.0))
        )
        
        global_stats = df_with_ratios.agg(
            F.expr(f'percentile_approx(transaction_count, {self.lower_pct})').alias('count_min'),
            F.expr(f'percentile_approx(transaction_count, {self.upper_pct})').alias('count_max'),
            F.expr(f'percentile_approx(avg_amount, {self.lower_pct})').alias('avg_amount_min'),
            F.expr(f'percentile_approx(avg_amount, {self.upper_pct})').alias('avg_amount_max'),
            F.expr(f'percentile_approx(tx_per_card, {self.lower_pct})').alias('tx_per_card_min'),
            F.expr(f'percentile_approx(tx_per_card, {self.upper_pct})').alias('tx_per_card_max'),
        ).first()
        
        self._global_bounds = {
            'count': (float(global_stats['count_min']), float(global_stats['count_max'])),
            'avg_amount': (float(global_stats['avg_amount_min']), float(global_stats['avg_amount_max'])),
            'tx_per_card': (float(global_stats['tx_per_card_min']), float(global_stats['tx_per_card_max'])),
        }
        
        # [VERBOSE] logger.info(f"  Global count bounds: [{self._global_bounds['count'][0]:.0f}, {self._global_bounds['count'][1]:.0f}]")
        # logger.info(f"  Global avg_amount bounds: [{self._global_bounds['avg_amount'][0]:.0f}, {self._global_bounds['avg_amount'][1]:.0f}]")
        # logger.info(f"  Global tx_per_card bounds: [{self._global_bounds['tx_per_card'][0]:.1f}, {self._global_bounds['tx_per_card'][1]:.1f}]")
    
    def get_bounds_df(self) -> Optional['DataFrame']:
        """Get the computed bounds DataFrame."""
        return self._bounds_df
    
    def get_global_bounds(self) -> Optional[Dict[str, Tuple[float, float]]]:
        """Get the global fallback bounds."""
        return self._global_bounds
    
    def cleanup(self) -> None:
        """Unpersist cached bounds DataFrame."""
        if self._bounds_df is not None:
            self._bounds_df.unpersist()
            self._bounds_df = None


def compute_and_apply_bounds(
    df: 'DataFrame',
    bounds_df: 'DataFrame',
    context_cols: list = None
) -> 'DataFrame':
    """
    Apply plausibility bounds to a DataFrame.
    
    Args:
        df: DataFrame with noisy values
        bounds_df: DataFrame with bounds per context
        context_cols: Context columns for joining
    
    Returns:
        DataFrame with bounds columns joined
    """
    if context_cols is None:
        context_cols = ['province_idx', 'mcc_idx', 'weekday']
    
    # Select only the bounds columns we need
    bounds_cols = ['count_min', 'count_max', 'avg_amount_min', 'avg_amount_max',
                   'tx_per_card_min', 'tx_per_card_max']
    
    bounds_select = bounds_df.select(*context_cols, *bounds_cols)
    
    # Join bounds to main DataFrame (broadcast for efficiency)
    df_with_bounds = df.join(
        F.broadcast(bounds_select),
        context_cols,
        'left'
    )
    
    return df_with_bounds

