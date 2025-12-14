"""
Context-Aware Plausibility-Based Noise Engine.

STATISTICAL DISCLOSURE CONTROL implementation with:
1. Multiplicative jitter that preserves ratios naturally
2. Data-driven plausibility bounds per (MCC, City, Weekday) context
3. Proper random noise (not hash-based)
4. Province invariants for count maintained exactly
5. Derived amount and cards that preserve realistic relationships

DESIGN PRINCIPLES:
- Outputs must be REALISTIC for each context
- Ratios (avg_amount, tx_per_card) are preserved approximately
- Noisy values are clamped to plausibility bounds from data
- Cross-query consistency: if count=0 → cards=0, amount=0
"""

import logging
from typing import Dict, Optional
from dataclasses import dataclass

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, StructType, StructField, IntegerType

from core.config import Config
from schema.geography import Geography
from schema.histogram_spark import SparkHistogram


logger = logging.getLogger(__name__)


@dataclass
class NoiseConfig:
    """Configuration for noise parameters."""
    noise_level: float = 0.15  # 15% relative noise
    seed: int = 42  # For reproducibility
    bounds_lower_percentile: float = 0.05  # p5 for lower bound
    bounds_upper_percentile: float = 0.95  # p95 for upper bound


class TopDownSparkEngine:
    """
    Context-Aware Plausibility-Based Noise Engine.
    
    DESIGN PHILOSOPHY:
    - Statistical Disclosure Control (NOT formal DP)
    - Multiplicative jitter preserves ratios naturally
    - Data-driven bounds ensure realistic outputs per context
    - Province count invariants maintained exactly
    
    CONTEXT DEFINITION:
    - Context = (MCC, City, Weekday)
    - Each context has its own plausibility bounds from historical data
    - Bounds are p5-p95 percentiles computed per context
    
    NOISE APPLICATION:
    1. Compute plausibility bounds per context from data
    2. Apply independent multiplicative jitter to all three values (count, cards, amount)
    3. Clamp all noisy values to context-specific bounds
    4. Validate and adjust ratios (avg_amount, tx_per_card) to stay within bounds
    5. Scale all three values to match province invariants
    6. Re-validate ratios after scaling and adjust if needed
    7. Validate all outputs are logically consistent
    """
    
    def __init__(
        self,
        spark: SparkSession,
        config: Config,
        geography: Geography
    ):
        """Initialize context-aware noise engine."""
        self.spark = spark
        self.config = config
        self.geography = geography
        
        # Noise configuration
        self.noise_config = NoiseConfig(
            noise_level=getattr(config.privacy, 'noise_level', 0.15),
            seed=getattr(config.privacy, 'noise_seed', 42)
        )
        
        # Statistics (small DataFrames, cached)
        self._invariants: Optional[DataFrame] = None
        self._bounds_df: Optional[DataFrame] = None
        
        logger.info("=" * 60)
        logger.info("Context-Aware Plausibility-Based Noise Engine")
        logger.info("=" * 60)
        logger.info(f"Noise level: {self.noise_config.noise_level:.1%}")
        logger.info(f"Seed: {self.noise_config.seed}")
        logger.info(f"Bounds: p{int(self.noise_config.bounds_lower_percentile*100)}-p{int(self.noise_config.bounds_upper_percentile*100)}")
        logger.info(f"Strategy: Multiplicative jitter with data-driven bounds")
        logger.info("=" * 60)
    
    def set_user_level_params(self, d_max: int, k_bound: int, winsorize_cap: float) -> None:
        """Set bounded contribution parameters (D_max, K, winsorize cap) from preprocessing."""
        logger.info(f"Bounded contribution params: D_max={d_max}, K={k_bound}, Winsorize_cap={winsorize_cap:,.0f}")
    
    def run(self, histogram: SparkHistogram) -> SparkHistogram:
        """
        Apply context-aware plausibility-based noise.
        
        Algorithm:
        1. Compute province invariants (count is exact)
        2. Compute plausibility bounds per (MCC, City, Weekday) context
        3. Store original ratios per cell
        4. Apply independent multiplicative jitter to all three values (count, cards, amount)
        5. Clamp all values to plausibility bounds and validate/adjust ratios
        6. Scale all three values to match province invariants
        7. Re-validate ratios after scaling and adjust if needed
        8. Finalize values and ensure consistency
        9. Controlled rounding
        10. Final validation
        """
        logger.info("=" * 70)
        logger.info("CONTEXT-AWARE PLAUSIBILITY-BASED NOISE")
        logger.info("=" * 70)
        
        df = histogram.df
        
        # Check if weekday column exists (should be provided by preprocessor)
        if 'weekday' not in df.columns:
            raise ValueError(
                "weekday column not found in histogram. "
                "The preprocessor must compute weekday from transaction_date before aggregation. "
                "Please ensure the preprocessor includes weekday in the groupBy aggregation."
            )
        
        # CRITICAL: Check if total_amount_original is available (from preprocessor)
        # SDC must work with real (original) amounts, not winsorized amounts
        use_original = 'total_amount_original' in df.columns
        
        if use_original:
            logger.info("  Using ORIGINAL (unwinsorized) amounts for invariants, bounds, ratios, and noise")
            self._amount_col = 'total_amount_original'
        else:
            logger.warning("  total_amount_original not found, using winsorized total_amount (legacy behavior)")
            self._amount_col = 'total_amount'
        
        # OPTIMIZATION: Repartition by province ONCE to minimize shuffles
        num_provinces = df.select('province_idx').distinct().count()
        num_partitions = max(31, num_provinces * 2)
        
        logger.info(f"\nOptimization: Repartitioning to {num_partitions} partitions by province_idx")
        df = df.repartition(num_partitions, 'province_idx')
        
        # ========================================
        # PHASE 1: Compute Province Invariants
        # ========================================
        logger.info("\n" + "=" * 70)
        logger.info("PHASE 1: Computing Province Invariants")
        logger.info("=" * 70)
        
        self._invariants = df.groupBy('province_idx').agg(
            F.sum('transaction_count').alias('invariant_count'),
            F.sum(self._amount_col).alias('invariant_amount'),  # Use original if available
            F.sum('unique_cards').alias('original_cards_sum')
        ).cache()
        
        inv_summary = self._invariants.agg(
            F.sum('invariant_count').alias('total_count'),
            F.sum('invariant_amount').alias('total_amount'),
            F.count('*').alias('num_provinces')
        ).first()
        
        logger.info(f"  Total count: {inv_summary['total_count']:,}")
        logger.info(f"  Total amount: {inv_summary['total_amount']:,}")
        logger.info(f"  Provinces: {inv_summary['num_provinces']}")
        
        # ========================================
        # PHASE 2: Compute Data-Driven Plausibility Bounds
        # ========================================
        logger.info("\n" + "=" * 70)
        logger.info("PHASE 2: Computing Data-Driven Plausibility Bounds")
        logger.info("=" * 70)
        
        self._bounds_df = self._compute_plausibility_bounds(df, self._amount_col)
        
        # ========================================
        # PHASE 3: Store Original Ratios Per Cell
        # ========================================
        logger.info("\n" + "=" * 70)
        logger.info("PHASE 3: Computing Original Ratios (Cell-Level)")
        logger.info("=" * 70)
        
        df = df.withColumn(
            'original_avg_amount',
            F.when(F.col('transaction_count') > 0,
                   F.col(self._amount_col) / F.col('transaction_count'))  # Use original if available
             .otherwise(F.lit(0.0))
        ).withColumn(
            'original_tx_per_card',
            F.when(F.col('unique_cards') > 0,
                   F.col('transaction_count') / F.col('unique_cards'))
             .otherwise(F.lit(1.0))
        )
        
        logger.info("  ✓ Original ratios stored per cell")
        
        # ========================================
        # PHASE 4: Apply Multiplicative Jitter with Proper Randomness
        # ========================================
        logger.info("\n" + "=" * 70)
        logger.info("PHASE 4: Applying Multiplicative Jitter to All Three Values")
        logger.info("=" * 70)
        
        noise_level = self.noise_config.noise_level
        seed = self.noise_config.seed
        
        # Get minimum noise deviation from config (default 0.01 = 1%)
        min_deviation = getattr(self.config.privacy, 'min_noise_factor_deviation', 0.01)
        
        # Generate three independent noise factors using different seed offsets
        # This ensures independence between count, cards, and amount noise
        # rand() generates uniform [0, 1), we transform to multiplicative factor
        # noise_factor = 1 + noise_level * (uniform - 0.5) * 2
        # This gives range [1 - noise_level, 1 + noise_level]
        
        # Noise factor for count
        df = df.withColumn(
            'noise_uniform_count',
            F.rand(seed=seed)
        ).withColumn(
            'noise_factor_count_raw',
            1.0 + noise_level * (F.col('noise_uniform_count') - 0.5) * 2.0
        )
        
        # Noise factor for cards
        df = df.withColumn(
            'noise_uniform_cards',
            F.rand(seed=seed + 1)
        ).withColumn(
            'noise_factor_cards_raw',
            1.0 + noise_level * (F.col('noise_uniform_cards') - 0.5) * 2.0
        )
        
        # Noise factor for amount
        df = df.withColumn(
            'noise_uniform_amount',
            F.rand(seed=seed + 2)
        ).withColumn(
            'noise_factor_amount_raw',
            1.0 + noise_level * (F.col('noise_uniform_amount') - 0.5) * 2.0
        )
        
        # Enforce minimum deviation from 1.0 to prevent zero noise for all three factors
        if min_deviation > 0:
            df = df.withColumn(
                'noise_factor_count',
                F.when(
                    F.abs(F.col('noise_factor_count_raw') - 1.0) < min_deviation,
                    F.when(F.col('noise_factor_count_raw') >= 1.0,
                           1.0 + min_deviation
                    ).otherwise(
                        1.0 - min_deviation
                    )
                ).otherwise(F.col('noise_factor_count_raw'))
            ).withColumn(
                'noise_factor_cards',
                F.when(
                    F.abs(F.col('noise_factor_cards_raw') - 1.0) < min_deviation,
                    F.when(F.col('noise_factor_cards_raw') >= 1.0,
                           1.0 + min_deviation
                    ).otherwise(
                        1.0 - min_deviation
                    )
                ).otherwise(F.col('noise_factor_cards_raw'))
            ).withColumn(
                'noise_factor_amount',
                F.when(
                    F.abs(F.col('noise_factor_amount_raw') - 1.0) < min_deviation,
                    F.when(F.col('noise_factor_amount_raw') >= 1.0,
                           1.0 + min_deviation
                    ).otherwise(
                        1.0 - min_deviation
                    )
                ).otherwise(F.col('noise_factor_amount_raw'))
            ).drop('noise_factor_count_raw', 'noise_factor_cards_raw', 'noise_factor_amount_raw')
        else:
            df = df.withColumn('noise_factor_count', F.col('noise_factor_count_raw')) \
                   .withColumn('noise_factor_cards', F.col('noise_factor_cards_raw')) \
                   .withColumn('noise_factor_amount', F.col('noise_factor_amount_raw')) \
                   .drop('noise_factor_count_raw', 'noise_factor_cards_raw', 'noise_factor_amount_raw')
        
        # Apply noise to all three values independently
        # CRITICAL: Apply noise to original amounts, not winsorized amounts
        df = df.withColumn(
            'noisy_count_raw',
            F.col('transaction_count').cast(DoubleType()) * F.col('noise_factor_count')
        ).withColumn(
            'noisy_cards_raw',
            F.col('unique_cards').cast(DoubleType()) * F.col('noise_factor_cards')
        ).withColumn(
            'noisy_amount_raw',
            F.col(self._amount_col).cast(DoubleType()) * F.col('noise_factor_amount')  # Use original if available
        )
        
        logger.info(f"  Noise level: ±{noise_level:.0%} (applied to all three values)")
        logger.info(f"  Seed: {seed} (count), {seed+1} (cards), {seed+2} (amount)")
        logger.info(f"  Noise factor range: [{1-noise_level:.2f}, {1+noise_level:.2f}]")
        if min_deviation > 0:
            logger.info(f"  Minimum noise deviation: ±{min_deviation:.1%} (prevents zero noise)")
        logger.info("  ✓ Independent random noise applied to count, cards, and amount")
        
        # Drop intermediate noise columns
        df = df.drop('noise_uniform_count', 'noise_uniform_cards', 'noise_uniform_amount',
                     'noise_factor_count', 'noise_factor_cards', 'noise_factor_amount')
        
        # ========================================
        # PHASE 5: Clamp to Bounds and Validate/Adjust Ratios
        # ========================================
        logger.info("\n" + "=" * 70)
        logger.info("PHASE 5: Clamping to Bounds & Validating Ratios")
        logger.info("=" * 70)
        
        # Join with bounds (context = mcc_idx, city_idx, weekday)
        df = df.join(
            F.broadcast(self._bounds_df),
            ['mcc_idx', 'city_idx', 'weekday'],
            'left'
        )
        
        # Fill missing bounds with reasonable defaults (for unseen contexts)
        df = df.fillna({
            'count_min': 1.0,
            'count_max': 10000.0,
            'avg_amount_min': 0.0,
            'avg_amount_max': 100000000.0,
            'tx_per_card_min': 1.0,
            'tx_per_card_max': 100.0
        })
        
        # Clamp all three noisy values to their respective bounds
        # For count: use count_min/count_max
        # For cards: derive from count bounds and tx_per_card bounds (cards = count / tx_per_card)
        # For amount: derive from count bounds and avg_amount bounds (amount = count * avg_amount)
        df = df.withColumn(
            'noisy_count_clamped',
            F.greatest(
                F.col('count_min'),
                F.least(F.col('count_max'), F.col('noisy_count_raw'))
            )
        ).withColumn(
            'noisy_cards_clamped',
            F.greatest(
                F.lit(0.0),
                F.least(
                    F.col('noisy_count_clamped') / F.greatest(F.col('tx_per_card_min'), F.lit(1.0)),
                    F.col('noisy_cards_raw')
                )
            )
        ).withColumn(
            'noisy_amount_clamped',
            F.greatest(
                F.lit(0.0),
                F.least(
                    F.col('noisy_count_clamped') * F.col('avg_amount_max'),
                    F.col('noisy_amount_raw')
                )
            )
        )
        
        # Log clamping statistics
        clamping_stats = df.agg(
            F.sum(F.when(F.col('noisy_count_raw') < F.col('count_min'), 1).otherwise(0)).alias('count_clamped_low'),
            F.sum(F.when(F.col('noisy_count_raw') > F.col('count_max'), 1).otherwise(0)).alias('count_clamped_high'),
            F.count('*').alias('total_cells')
        ).first()
        
        total_clamped = clamping_stats['count_clamped_low'] + clamping_stats['count_clamped_high']
        pct_clamped = 100 * total_clamped / clamping_stats['total_cells'] if clamping_stats['total_cells'] > 0 else 0
        logger.info(f"  Count clamped: {total_clamped:,} cells ({pct_clamped:.2f}%)")
        logger.info("  ✓ Values clamped to bounds")
        
        # Now compute actual ratios and validate/adjust if needed
        df = df.withColumn(
            'actual_avg_amount',
            F.when(F.col('noisy_count_clamped') > 0,
                   F.col('noisy_amount_clamped') / F.col('noisy_count_clamped'))
             .otherwise(F.lit(0.0))
        ).withColumn(
            'actual_tx_per_card',
            F.when(F.col('noisy_cards_clamped') > 0,
                   F.col('noisy_count_clamped') / F.col('noisy_cards_clamped'))
             .otherwise(F.lit(1.0))
        )
        
        # Check if ratios are within bounds and make minimal adjustments
        # If avg_amount is too low: increase noisy_amount_clamped to avg_amount_min * count
        # If avg_amount is too high: decrease noisy_amount_clamped to avg_amount_max * count
        # If tx_per_card is too low: decrease noisy_cards_clamped (increase tx_per_card)
        # If tx_per_card is too high: increase noisy_cards_clamped (decrease tx_per_card)
        df = df.withColumn(
            'noisy_amount_adjusted',
            F.when(
                (F.col('noisy_count_clamped') > 0) & 
                (F.col('actual_avg_amount') < F.col('avg_amount_min')),
                F.col('noisy_count_clamped') * F.col('avg_amount_min')
            ).when(
                (F.col('noisy_count_clamped') > 0) & 
                (F.col('actual_avg_amount') > F.col('avg_amount_max')),
                F.col('noisy_count_clamped') * F.col('avg_amount_max')
            ).otherwise(F.col('noisy_amount_clamped'))
        ).withColumn(
            'noisy_cards_adjusted',
            F.when(
                (F.col('noisy_count_clamped') > 0) & 
                (F.col('actual_tx_per_card') > F.col('tx_per_card_max')),
                F.col('noisy_count_clamped') / F.col('tx_per_card_max')
            ).when(
                (F.col('noisy_count_clamped') > 0) & 
                (F.col('actual_tx_per_card') < F.col('tx_per_card_min')),
                F.col('noisy_count_clamped') / F.col('tx_per_card_min')
            ).otherwise(F.col('noisy_cards_clamped'))
        )
        
        # Recompute ratios after adjustment
        df = df.withColumn(
            'final_avg_amount',
            F.when(F.col('noisy_count_clamped') > 0,
                   F.col('noisy_amount_adjusted') / F.col('noisy_count_clamped'))
             .otherwise(F.lit(0.0))
        ).withColumn(
            'final_tx_per_card',
            F.when(F.col('noisy_cards_adjusted') > 0,
                   F.col('noisy_count_clamped') / F.col('noisy_cards_adjusted'))
             .otherwise(F.lit(1.0))
        )
        
        # Log ratio adjustment statistics
        ratio_stats = df.agg(
            F.sum(F.when(
                (F.col('noisy_count_clamped') > 0) & 
                ((F.col('actual_avg_amount') < F.col('avg_amount_min')) | 
                 (F.col('actual_avg_amount') > F.col('avg_amount_max'))),
                1).otherwise(0)
            ).alias('avg_amount_adjusted'),
            F.sum(F.when(
                (F.col('noisy_count_clamped') > 0) & 
                ((F.col('actual_tx_per_card') < F.col('tx_per_card_min')) | 
                 (F.col('actual_tx_per_card') > F.col('tx_per_card_max'))),
                1).otherwise(0)
            ).alias('tx_per_card_adjusted'),
            F.count('*').alias('total')
        ).first()
        
        if ratio_stats['total'] > 0:
            avg_amt_pct = 100 * (ratio_stats['avg_amount_adjusted'] or 0) / ratio_stats['total']
            tx_card_pct = 100 * (ratio_stats['tx_per_card_adjusted'] or 0) / ratio_stats['total']
            logger.info(f"  Avg amount adjusted: {ratio_stats['avg_amount_adjusted'] or 0:,} cells ({avg_amt_pct:.2f}%)")
            logger.info(f"  TX per card adjusted: {ratio_stats['tx_per_card_adjusted'] or 0:,} cells ({tx_card_pct:.2f}%)")
        
        logger.info("  ✓ Ratios validated and adjusted to stay within bounds")
        
        # Rename to final noisy values
        df = df.withColumn('noisy_count', F.col('noisy_count_clamped')) \
               .withColumn('noisy_cards', F.col('noisy_cards_adjusted')) \
               .withColumn('noisy_amount', F.col('noisy_amount_adjusted'))
        
        # Drop intermediate columns
        df = df.drop('noisy_count_raw', 'noisy_cards_raw', 'noisy_amount_raw',
                     'noisy_count_clamped', 'noisy_cards_clamped', 'noisy_amount_clamped',
                     'noisy_amount_adjusted', 'noisy_cards_adjusted',
                     'actual_avg_amount', 'actual_tx_per_card',
                     'final_avg_amount', 'final_tx_per_card')
        
        # ========================================
        # PHASE 6: Scale to Match Province Invariants
        # ========================================
        logger.info("\n" + "=" * 70)
        logger.info("PHASE 6: Scaling All Three Values to Match Province Invariants")
        logger.info("=" * 70)
        
        # Compute sums of noisy values per province
        window = Window.partitionBy('province_idx')
        df = df.withColumn('noisy_count_sum', F.sum('noisy_count').over(window)) \
               .withColumn('noisy_cards_sum', F.sum('noisy_cards').over(window)) \
               .withColumn('noisy_amount_sum', F.sum('noisy_amount').over(window))
        
        # Join invariants
        df = df.join(F.broadcast(self._invariants), 'province_idx', 'left')
        
        # Compute scale factors for all three values
        df = df.withColumn(
            'scale_factor_count',
            F.when(F.col('noisy_count_sum') > 0,
                   F.col('invariant_count') / F.col('noisy_count_sum'))
             .otherwise(F.lit(1.0))
        ).withColumn(
            'scale_factor_cards',
            F.when(F.col('noisy_cards_sum') > 0,
                   F.col('original_cards_sum') / F.col('noisy_cards_sum'))
             .otherwise(F.lit(1.0))
        ).withColumn(
            'scale_factor_amount',
            F.when(F.col('noisy_amount_sum') > 0,
                   F.col('invariant_amount') / F.col('noisy_amount_sum'))
             .otherwise(F.lit(1.0))
        )
        
        # Apply scaling to all three values
        df = df.withColumn('scaled_count', F.col('noisy_count') * F.col('scale_factor_count')) \
               .withColumn('scaled_cards', F.col('noisy_cards') * F.col('scale_factor_cards')) \
               .withColumn('scaled_amount', F.col('noisy_amount') * F.col('scale_factor_amount'))
        
        logger.info("  ✓ All three values scaled to match province invariants")
        
        # After scaling, re-validate ratios and adjust if needed (ratios may change after scaling)
        df = df.withColumn(
            'scaled_avg_amount',
            F.when(F.col('scaled_count') > 0,
                   F.col('scaled_amount') / F.col('scaled_count'))
             .otherwise(F.lit(0.0))
        ).withColumn(
            'scaled_tx_per_card',
            F.when(F.col('scaled_cards') > 0,
                   F.col('scaled_count') / F.col('scaled_cards'))
             .otherwise(F.lit(1.0))
        )
        
        # Adjust if ratios are out of bounds after scaling
        df = df.withColumn(
            'scaled_amount_final',
            F.when(
                (F.col('scaled_count') > 0) & 
                (F.col('scaled_avg_amount') < F.col('avg_amount_min')),
                F.col('scaled_count') * F.col('avg_amount_min')
            ).when(
                (F.col('scaled_count') > 0) & 
                (F.col('scaled_avg_amount') > F.col('avg_amount_max')),
                F.col('scaled_count') * F.col('avg_amount_max')
            ).otherwise(F.col('scaled_amount'))
        ).withColumn(
            'scaled_cards_final',
            F.when(
                (F.col('scaled_count') > 0) & 
                (F.col('scaled_tx_per_card') > F.col('tx_per_card_max')),
                F.col('scaled_count') / F.col('tx_per_card_max')
            ).when(
                (F.col('scaled_count') > 0) & 
                (F.col('scaled_tx_per_card') < F.col('tx_per_card_min')),
                F.col('scaled_count') / F.col('tx_per_card_min')
            ).otherwise(F.col('scaled_cards'))
        )
        
        # Ensure consistency: if count=0, then cards=0 and amount=0
        df = df.withColumn(
            'scaled_count_final',
            F.when(F.col('scaled_count') <= 0.5, F.lit(0.0))
             .otherwise(F.col('scaled_count'))
        ).withColumn(
            'scaled_cards_final',
            F.when(F.col('scaled_count_final') == 0, F.lit(0.0))
             .otherwise(F.col('scaled_cards_final'))
        ).withColumn(
            'scaled_amount_final',
            F.when(F.col('scaled_count_final') == 0, F.lit(0.0))
             .otherwise(F.col('scaled_amount_final'))
        )
        
        # Log post-scaling ratio adjustments
        post_scale_stats = df.agg(
            F.sum(F.when(
                (F.col('scaled_count') > 0) & 
                ((F.col('scaled_avg_amount') < F.col('avg_amount_min')) | 
                 (F.col('scaled_avg_amount') > F.col('avg_amount_max'))),
                1).otherwise(0)
            ).alias('post_scale_avg_amount_adjusted'),
            F.sum(F.when(
                (F.col('scaled_count') > 0) & 
                ((F.col('scaled_tx_per_card') < F.col('tx_per_card_min')) | 
                 (F.col('scaled_tx_per_card') > F.col('tx_per_card_max'))),
                1).otherwise(0)
            ).alias('post_scale_tx_per_card_adjusted'),
            F.count('*').alias('total')
        ).first()
        
        if post_scale_stats['total'] > 0:
            avg_amt_pct = 100 * (post_scale_stats['post_scale_avg_amount_adjusted'] or 0) / post_scale_stats['total']
            tx_card_pct = 100 * (post_scale_stats['post_scale_tx_per_card_adjusted'] or 0) / post_scale_stats['total']
            logger.info(f"  Post-scaling avg amount adjusted: {post_scale_stats['post_scale_avg_amount_adjusted'] or 0:,} cells ({avg_amt_pct:.2f}%)")
            logger.info(f"  Post-scaling TX per card adjusted: {post_scale_stats['post_scale_tx_per_card_adjusted'] or 0:,} cells ({tx_card_pct:.2f}%)")
        
        logger.info("  ✓ Ratios re-validated after scaling")
        
        # Rename to final scaled values
        df = df.withColumn('scaled_count', F.col('scaled_count_final')) \
               .withColumn('scaled_cards', F.col('scaled_cards_final')) \
               .withColumn('scaled_amount', F.col('scaled_amount_final'))
        
        # Drop intermediate columns
        df = df.drop('noisy_count', 'noisy_cards', 'noisy_amount',
                     'noisy_count_sum', 'noisy_cards_sum', 'noisy_amount_sum',
                     'scale_factor_count', 'scale_factor_cards', 'scale_factor_amount',
                     'scaled_avg_amount', 'scaled_tx_per_card',
                     'scaled_count_final', 'scaled_cards_final', 'scaled_amount_final')
        
        # ========================================
        # PHASE 7: Finalize Values and Ensure Consistency
        # ========================================
        logger.info("\n" + "=" * 70)
        logger.info("PHASE 7: Finalizing Values & Ensuring Consistency")
        logger.info("=" * 70)
        
        # Values already have noise applied and ratios validated
        # Just rename to final names and ensure consistency constraints
        df = df.withColumn('final_count', F.col('scaled_count')) \
               .withColumn('final_cards', F.col('scaled_cards')) \
               .withColumn('final_amount', F.col('scaled_amount'))
        
        # Ensure consistency constraints:
        # 1. If count <= 0.5, set count/cards/amount to 0
        # 2. If count > 0, cards must be >= 1 and <= count
        # 3. Amount must be >= 0
        df = df.withColumn(
            'final_count',
            F.when(F.col('final_count') <= 0.5, F.lit(0.0))
             .otherwise(F.greatest(F.lit(1.0), F.col('final_count')))
        ).withColumn(
            'final_cards',
            F.when(F.col('final_count') == 0, F.lit(0.0))
             .otherwise(
                 F.greatest(
                     F.lit(1.0),
                     F.least(F.col('final_cards'), F.col('final_count'))
                 )
             )
        ).withColumn(
            'final_amount',
            F.when(F.col('final_count') == 0, F.lit(0.0))
             .otherwise(F.greatest(F.lit(0.0), F.col('final_amount')))
        )
        
        # Log final statistics
        final_stats = df.agg(
            F.sum('final_count').alias('total_count'),
            F.sum('final_cards').alias('total_cards'),
            F.sum('final_amount').alias('total_amount'),
            F.count('*').alias('total_cells')
        ).first()
        
        logger.info(f"  Final total count: {final_stats['total_count']:,.0f}")
        logger.info(f"  Final total cards: {final_stats['total_cards']:,.0f}")
        logger.info(f"  Final total amount: {final_stats['total_amount']:,.0f}")
        logger.info(f"  Total cells: {final_stats['total_cells']:,}")
        logger.info("  ✓ Values finalized and consistency ensured")
        
        # ========================================
        # PHASE 8: Final Validation
        # ========================================
        logger.info("\n" + "=" * 70)
        logger.info("PHASE 8: Final Validation")
        logger.info("=" * 70)
        
        # Values are already validated in Phase 7
        # Just do a final check for consistency
        inconsistent = df.filter(
            ((F.col('final_count') == 0) & (F.col('final_cards') > 0)) |
            ((F.col('final_count') == 0) & (F.col('final_amount') > 0)) |
            ((F.col('final_count') > 0) & (F.col('final_cards') == 0)) |
            (F.col('final_cards') > F.col('final_count'))
        ).count()
        
        if inconsistent > 0:
            logger.warning(f"  ⚠ Found {inconsistent} inconsistent cells (will be fixed)")
            # Fix any remaining inconsistencies
            df = df.withColumn(
                'final_cards',
                F.when(F.col('final_count') == 0, F.lit(0.0))
                 .when(F.col('final_cards') > F.col('final_count'), F.col('final_count'))
                 .when((F.col('final_count') > 0) & (F.col('final_cards') == 0), F.lit(1.0))
                 .otherwise(F.col('final_cards'))
            ).withColumn(
                'final_amount',
                F.when(F.col('final_count') == 0, F.lit(0.0))
                 .otherwise(F.col('final_amount'))
            )
        
        logger.info("  ✓ Final validation complete")
        
        # Drop intermediate columns (keep only final_*, invariant, and bounds)
        df = df.drop('scaled_count', 'scaled_cards', 'scaled_amount',
                     'original_avg_amount', 'original_tx_per_card')
        
        # ========================================
        # PHASE 9: Controlled Rounding
        # ========================================
        logger.info("\n" + "=" * 70)
        logger.info("PHASE 9: Controlled Integer Rounding")
        logger.info("=" * 70)
        
        df_rounded = self._controlled_rounding(df)
        
        logger.info("  ✓ Controlled rounding complete")
        
        # ========================================
        # PHASE 10: Final Validation
        # ========================================
        logger.info("\n" + "=" * 70)
        logger.info("PHASE 10: Final Validation")
        logger.info("=" * 70)
        
        self._final_validation(df_rounded)
        
        # Cleanup
        if self._invariants is not None:
            self._invariants.unpersist()
        if self._bounds_df is not None:
            self._bounds_df.unpersist()
        
        logger.info("\n" + "=" * 70)
        logger.info("Context-Aware Noise Processing Complete")
        logger.info("=" * 70)
        
        # Drop weekday column before returning - it's a processing dimension, not a structural dimension
        # The histogram structure is 4D (province, city, mcc, day), not 5D
        # Aggregate by (province, city, mcc, day) in case there are any duplicates after dropping weekday
        df_final = df_rounded.groupBy(
            'province_idx', 'city_idx', 'mcc_idx', 'day_idx'
        ).agg(
            F.sum('transaction_count').cast('long').alias('transaction_count'),
            F.sum('unique_cards').cast('long').alias('unique_cards'),
            F.sum('total_amount').cast('long').alias('total_amount')
        )
        
        return SparkHistogram(self.spark, df_final, histogram.dimensions, histogram.city_codes, histogram.min_date)
    
    def _compute_plausibility_bounds(self, df: DataFrame, amount_col: str = 'total_amount') -> DataFrame:
        """
        Compute plausibility bounds per (MCC, City, Weekday) context from data.
        
        Args:
            df: DataFrame with histogram data
            amount_col: Column name for amounts ('total_amount_original' for original, 'total_amount' for winsorized)
        
        Returns DataFrame with bounds columns for each context.
        """
        lower_pct = self.noise_config.bounds_lower_percentile
        upper_pct = self.noise_config.bounds_upper_percentile
        
        logger.info(f"  Computing bounds per context (MCC, City, Weekday)")
        logger.info(f"  Lower percentile: p{int(lower_pct*100)}")
        logger.info(f"  Upper percentile: p{int(upper_pct*100)}")
        logger.info(f"  Using {amount_col} for bounds computation")
        
        # Compute ratios for bounds calculation
        df_with_ratios = df.withColumn(
            'avg_amount',
            F.when(F.col('transaction_count') > 0,
                   F.col(amount_col) / F.col('transaction_count'))  # Use original if available
             .otherwise(F.lit(0.0))
        ).withColumn(
            'tx_per_card',
            F.when(F.col('unique_cards') > 0,
                   F.col('transaction_count') / F.col('unique_cards'))
             .otherwise(F.lit(1.0))
        )
        
        # Group by context and compute percentile bounds
        bounds_df = df_with_ratios.groupBy('mcc_idx', 'city_idx', 'weekday').agg(
            F.count('*').alias('sample_count'),
            
            # Count bounds
            F.expr(f'percentile_approx(transaction_count, {lower_pct})').alias('count_min'),
            F.expr(f'percentile_approx(transaction_count, {upper_pct})').alias('count_max'),
            F.mean('transaction_count').alias('count_mean'),
            
            # Avg amount bounds
            F.expr(f'percentile_approx(avg_amount, {lower_pct})').alias('avg_amount_min'),
            F.expr(f'percentile_approx(avg_amount, {upper_pct})').alias('avg_amount_max'),
            
            # TX per card bounds
            F.expr(f'percentile_approx(tx_per_card, {lower_pct})').alias('tx_per_card_min'),
            F.expr(f'percentile_approx(tx_per_card, {upper_pct})').alias('tx_per_card_max'),
        )
        
        # For sparse contexts (few samples), widen bounds based on mean
        min_samples = 3
        bounds_df = bounds_df.withColumn(
            'count_min',
            F.when(F.col('sample_count') < min_samples,
                   F.greatest(F.lit(1.0), F.col('count_mean') * 0.1))
             .otherwise(F.greatest(F.lit(1.0), F.col('count_min')))
        ).withColumn(
            'count_max',
            F.when(F.col('sample_count') < min_samples,
                   F.col('count_mean') * 5.0)
             .otherwise(F.col('count_max'))
        )
        
        # Ensure minimum bounds make sense
        bounds_df = bounds_df.withColumn(
            'count_min', F.greatest(F.lit(1.0), F.col('count_min'))
        ).withColumn(
            'tx_per_card_min', F.greatest(F.lit(1.0), F.col('tx_per_card_min'))
        ).withColumn(
            'avg_amount_min', F.greatest(F.lit(0.0), F.col('avg_amount_min'))
        )
        
        # Cache bounds
        bounds_df = bounds_df.cache()
        
        # Log statistics
        bounds_stats = bounds_df.agg(
            F.count('*').alias('num_contexts'),
            F.mean('sample_count').alias('avg_samples'),
            F.sum(F.when(F.col('sample_count') < min_samples, 1).otherwise(0)).alias('sparse_contexts')
        ).first()
        
        logger.info(f"  Unique contexts: {bounds_stats['num_contexts']:,}")
        logger.info(f"  Avg samples/context: {bounds_stats['avg_samples']:.1f}")
        logger.info(f"  Sparse contexts (< {min_samples} samples): {bounds_stats['sparse_contexts']:,}")
        logger.info("  ✓ Plausibility bounds computed")
        
        return bounds_df
    
    def _validate_plausibility(self, df: DataFrame) -> DataFrame:
        """
        Validate and enforce plausibility constraints.
        
        Rules:
        1. If count <= 0.5, set count/cards/amount to 0
        2. If count > 0, cards must be >= 1
        3. Cards cannot exceed count
        4. Amount must be >= 0
        """
        # Rule 1: Zero out cells with very low count
        df = df.withColumn(
            'final_count',
            F.when(F.col('scaled_count') <= 0.5, F.lit(0.0))
             .otherwise(F.greatest(F.lit(1.0), F.col('scaled_count')))
        )
        
        # Rule 2 & 3: Cards constraints
        df = df.withColumn(
            'final_cards',
            F.when(F.col('final_count') == 0, F.lit(0.0))
             .otherwise(
                 F.greatest(
                     F.lit(1.0),
                     F.least(F.col('derived_cards'), F.col('final_count'))
                 )
             )
        )
        
        # Rule 4: Amount constraints
        df = df.withColumn(
            'final_amount',
            F.when(F.col('final_count') == 0, F.lit(0.0))
             .otherwise(F.greatest(F.lit(0.0), F.col('derived_amount')))
        )
        
        return df
    
    def _controlled_rounding(self, df: DataFrame) -> DataFrame:
        """
        Perform controlled integer rounding maintaining province count invariant.
        """
        # Prepare for rounding - include ratio bounds for validation
        df_for_rounding = df.select(
            'province_idx', 'city_idx', 'mcc_idx', 'day_idx', 'weekday',
            'final_count', 'final_cards', 'final_amount',
            'invariant_count',
            # Include ratio bounds to check adjustments don't break plausibility
            'avg_amount_min', 'avg_amount_max',
            'tx_per_card_min', 'tx_per_card_max'
        )
        
        output_schema = StructType([
            StructField('province_idx', IntegerType(), False),
            StructField('city_idx', IntegerType(), False),
            StructField('mcc_idx', IntegerType(), False),
            StructField('day_idx', IntegerType(), False),
            StructField('weekday', IntegerType(), False),
            StructField('transaction_count', LongType(), False),
            StructField('unique_cards', LongType(), False),
            StructField('total_amount', LongType(), False)
        ])
        
        def round_province(pdf):
            """
            Round one province maintaining COUNT invariant exactly.
            
            CRITICAL: When count changes, scale amount/cards proportionally to preserve ratios!
            Also check that adjustments keep ratios within plausibility bounds.
            """
            import pandas as pd
            import numpy as np
            
            if len(pdf) == 0:
                return pd.DataFrame(columns=[
                    'province_idx', 'city_idx', 'mcc_idx', 'day_idx', 'weekday',
                    'transaction_count', 'unique_cards', 'total_amount'
                ])
            
            target_count = int(pdf['invariant_count'].iloc[0])
            
            # Get ORIGINAL values (before any adjustments)
            original_count = pdf['final_count'].values.copy()
            original_cards = pdf['final_cards'].values.copy()
            original_amount = pdf['final_amount'].values.copy()
            
            # Get ratio bounds per cell
            avg_amt_min = pdf['avg_amount_min'].values
            avg_amt_max = pdf['avg_amount_max'].values
            tx_card_min = pdf['tx_per_card_min'].values
            tx_card_max = pdf['tx_per_card_max'].values
            
            # Initial floor rounding
            floors_count = np.floor(original_count).astype(np.int64)
            remainders = original_count - floors_count
            floor_sum = floors_count.sum()
            diff = target_count - floor_sum
            
            def check_ratio_valid(idx, new_count):
                """
                Check if adjusting count keeps ratios within bounds.
                IMPORTANT: Use scaled amount/cards (proportional to count change)!
                """
                if new_count <= 0:
                    return True  # Zero cells are always valid
                
                # Calculate scale factor
                scale = new_count / max(original_count[idx], 1)
                
                # Calculate what amount and cards would be after proportional scaling
                scaled_amount = original_amount[idx] * scale
                scaled_cards = original_cards[idx] * scale
                
                # Round to integers (what actual values will be)
                rounded_amount_test = round(scaled_amount)
                rounded_cards_test = max(1, round(scaled_cards))  # At least 1
                
                # Check avg_amount ratio with actual rounded values
                new_avg_amt = rounded_amount_test / new_count
                if new_avg_amt < avg_amt_min[idx] or new_avg_amt > avg_amt_max[idx]:
                    return False
                
                # Check tx_per_card ratio with actual rounded values
                if rounded_cards_test > 0:
                    new_tx_card = new_count / rounded_cards_test
                    if new_tx_card < tx_card_min[idx] or new_tx_card > tx_card_max[idx]:
                        return False
                
                return True
            
            # Adjust cells while respecting ratio bounds
            if diff > 0:
                # Need to round UP some cells
                candidates = np.argsort(-remainders)
                adjusted = 0
                for idx in candidates:
                    if adjusted >= diff:
                        break
                    new_count = floors_count[idx] + 1
                    if check_ratio_valid(idx, new_count):
                        floors_count[idx] = new_count
                        adjusted += 1
                
                # If we couldn't adjust enough cells, force remaining (last resort)
                if adjusted < diff:
                    for idx in candidates:
                        if adjusted >= diff:
                            break
                        if floors_count[idx] == np.floor(original_count[idx]):
                            floors_count[idx] += 1
                            adjusted += 1
                            
            elif diff < 0:
                # Need to round DOWN some cells
                n_down = int(-diff)
                nonzero = np.where(floors_count > 0)[0]
                if len(nonzero) > 0:
                    sorted_nz = nonzero[np.argsort(remainders[nonzero])]
                    adjusted = 0
                    for idx in sorted_nz:
                        if adjusted >= n_down:
                            break
                        new_count = floors_count[idx] - 1
                        if check_ratio_valid(idx, new_count):
                            floors_count[idx] = new_count
                            adjusted += 1
                    
                    # Force remaining if needed
                    if adjusted < n_down:
                        for idx in sorted_nz:
                            if adjusted >= n_down:
                                break
                            if floors_count[idx] > 0 and floors_count[idx] == np.floor(original_count[idx]):
                                floors_count[idx] -= 1
                                adjusted += 1
            
            # CRITICAL: Scale amount and cards PROPORTIONALLY to count changes
            with np.errstate(divide='ignore', invalid='ignore'):
                scale_factor = np.where(
                    original_count > 0,
                    floors_count.astype(np.float64) / original_count,
                    1.0
                )
            
            # Scale amount and cards
            scaled_amount = original_amount * scale_factor
            scaled_cards = original_cards * scale_factor
            
            # Round to integers
            rounded_amount = np.round(scaled_amount).astype(np.int64)
            rounded_cards = np.round(scaled_cards).astype(np.int64)
            
            # Enforce consistency constraints
            zero_mask = floors_count == 0
            rounded_cards[zero_mask] = 0
            rounded_amount[zero_mask] = 0
            
            active_mask = floors_count > 0
            # Cards must be >= 1 and <= count
            rounded_cards[active_mask] = np.maximum(1, rounded_cards[active_mask])
            rounded_cards[active_mask] = np.minimum(floors_count[active_mask], rounded_cards[active_mask])
            # Amount must be >= 0
            rounded_amount[active_mask] = np.maximum(0, rounded_amount[active_mask])
            
            return pd.DataFrame({
                'province_idx': pdf['province_idx'].values.astype(np.int32),
                'city_idx': pdf['city_idx'].values.astype(np.int32),
                'mcc_idx': pdf['mcc_idx'].values.astype(np.int32),
                'day_idx': pdf['day_idx'].values.astype(np.int32),
                'weekday': pdf['weekday'].values.astype(np.int32),
                'transaction_count': floors_count,
                'unique_cards': rounded_cards,
                'total_amount': rounded_amount
            })
        
        df_rounded = df_for_rounding.groupBy('province_idx').applyInPandas(
            round_province, schema=output_schema
        )
        
        return df_rounded
    
    def _final_validation(self, df_rounded: DataFrame) -> None:
        """Perform final validation and log statistics."""
        
        # Check COUNT invariant
        actual_counts = df_rounded.groupBy('province_idx').agg(
            F.sum('transaction_count').alias('actual_count'),
            F.sum('total_amount').alias('actual_amount')
        )
        
        comparison = self._invariants.join(actual_counts, 'province_idx', 'inner')
        comparison = comparison.withColumn(
            'count_diff', F.col('actual_count') - F.col('invariant_count')
        ).withColumn(
            'amount_error_pct',
            F.abs(F.col('actual_amount') - F.col('invariant_amount')) / 
            F.greatest(F.col('invariant_amount'), F.lit(1)) * 100
        )
        
        count_mismatches = comparison.filter(F.col('count_diff') != 0).count()
        
        if count_mismatches > 0:
            logger.error(f"  ✗ COUNT invariant violated in {count_mismatches} provinces!")
        else:
            logger.info("  ✓ COUNT invariant exact in all provinces")
        
        # Amount error stats
        amount_stats = comparison.agg(
            F.max('amount_error_pct').alias('max_error'),
            F.mean('amount_error_pct').alias('mean_error')
        ).first()
        
        if amount_stats and amount_stats['max_error'] is not None:
            logger.info(f"  Amount error: max={amount_stats['max_error']:.2f}%, mean={amount_stats['mean_error']:.2f}%")
        
        # Check consistency
        inconsistent = df_rounded.filter(
            ((F.col('transaction_count') == 0) & (F.col('unique_cards') > 0)) |
            ((F.col('transaction_count') == 0) & (F.col('total_amount') > 0)) |
            ((F.col('transaction_count') > 0) & (F.col('unique_cards') == 0)) |
            (F.col('unique_cards') > F.col('transaction_count'))
        ).count()
        
        if inconsistent > 0:
            logger.error(f"  ✗ {inconsistent} inconsistent cells!")
        else:
            logger.info("  ✓ All cells logically consistent")
        
        # Check ratio bounds - join with bounds and verify ratios are within [min, max]
        logger.info("\n  Ratio Bounds Validation:")
        
        # Join rounded data with bounds
        df_with_bounds = df_rounded.join(
            F.broadcast(self._bounds_df.select(
                'mcc_idx', 'city_idx', 'weekday',
                'avg_amount_min', 'avg_amount_max',
                'tx_per_card_min', 'tx_per_card_max'
            )),
            ['mcc_idx', 'city_idx', 'weekday'],
            'left'
        )
        
        # Compute actual ratios
        df_with_ratios = df_with_bounds.filter(F.col('transaction_count') > 0).withColumn(
            'actual_avg_amount',
            F.col('total_amount') / F.col('transaction_count')
        ).withColumn(
            'actual_tx_per_card',
            F.col('transaction_count') / F.greatest(F.col('unique_cards'), F.lit(1))
        )
        
        # Check how many cells have ratios outside bounds
        ratio_violations = df_with_ratios.agg(
            F.sum(F.when(
                (F.col('actual_avg_amount') < F.col('avg_amount_min')) |
                (F.col('actual_avg_amount') > F.col('avg_amount_max')),
                1).otherwise(0)
            ).alias('avg_amt_violations'),
            F.sum(F.when(
                (F.col('actual_tx_per_card') < F.col('tx_per_card_min')) |
                (F.col('actual_tx_per_card') > F.col('tx_per_card_max')),
                1).otherwise(0)
            ).alias('tx_card_violations'),
            F.count('*').alias('total_active')
        ).first()
        
        if ratio_violations:
            total = ratio_violations['total_active'] or 1
            avg_amt_viol = ratio_violations['avg_amt_violations'] or 0
            tx_card_viol = ratio_violations['tx_card_violations'] or 0
            
            avg_amt_pct = 100 * avg_amt_viol / total
            tx_card_pct = 100 * tx_card_viol / total
            
            if avg_amt_viol > 0:
                logger.warning(f"  ⚠ avg_amount outside bounds: {avg_amt_viol:,} cells ({avg_amt_pct:.2f}%)")
            else:
                logger.info(f"  ✓ All avg_amount ratios within bounds")
            
            if tx_card_viol > 0:
                logger.warning(f"  ⚠ tx_per_card outside bounds: {tx_card_viol:,} cells ({tx_card_pct:.2f}%)")
            else:
                logger.info(f"  ✓ All tx_per_card ratios within bounds")
        
        # Ratio summary stats
        df_active = df_rounded.filter(F.col('transaction_count') > 0)
        ratio_stats = df_active.agg(
            F.mean(F.col('transaction_count') / F.col('unique_cards')).alias('mean_tx_per_card'),
            F.mean(F.col('total_amount') / F.col('transaction_count')).alias('mean_avg_amount'),
            F.count('*').alias('active_cells')
        ).first()
        
        if ratio_stats and ratio_stats['active_cells'] > 0:
            logger.info(f"\n  Summary - Active cells: {ratio_stats['active_cells']:,}")
            if ratio_stats['mean_tx_per_card'] is not None:
                logger.info(f"  Mean TX per card: {ratio_stats['mean_tx_per_card']:.2f}")
            if ratio_stats['mean_avg_amount'] is not None:
                logger.info(f"  Mean avg amount: {ratio_stats['mean_avg_amount']:,.2f}")
