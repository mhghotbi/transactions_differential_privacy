"""
Context-Aware Plausibility-Based Noise Engine.

STATISTICAL DISCLOSURE CONTROL implementation with:
1. Multiplicative jitter that preserves ratios naturally
2. Data-driven plausibility bounds per (Province, MCC, Weekday) context
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
    bounds_widening_factor: float = 2.0  # Expand bounds by this factor (2.0 = 2x wider)
    sparse_bounds_widening_factor: float = 3.0  # Extra widening for sparse contexts (< min_samples)


class TopDownSparkEngine:
    """
    Context-Aware Plausibility-Based Noise Engine.
    
    DESIGN PHILOSOPHY:
    - Statistical Disclosure Control (NOT formal DP)
    - Multiplicative jitter preserves ratios naturally
    - Data-driven bounds ensure realistic outputs per context
    - Province count invariants maintained exactly
    
    CONTEXT DEFINITION:
    - Context = (Province, MCC, Weekday)
    - Each context has its own plausibility bounds from historical data
    - Bounds are p5-p95 percentiles computed per context
    - Province-specific bounds align with province-specific scaling factors
    
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
            seed=getattr(config.privacy, 'noise_seed', 42),
            bounds_lower_percentile=getattr(config.privacy, 'bounds_lower_percentile', 0.05),
            bounds_upper_percentile=getattr(config.privacy, 'bounds_upper_percentile', 0.95),
            bounds_widening_factor=getattr(config.privacy, 'bounds_widening_factor', 2.0),
            sparse_bounds_widening_factor=getattr(config.privacy, 'sparse_bounds_widening_factor', 3.0)
        )
        
        # Statistics (small DataFrames, cached)
        self._invariants: Optional[DataFrame] = None
        self._bounds_df: Optional[DataFrame] = None
        
        # [VERBOSE] logger.info("=" * 60)
        # logger.info("Context-Aware Plausibility-Based Noise Engine")
        # logger.info("=" * 60)
        # logger.info(f"Noise level: {self.noise_config.noise_level:.1%}")
        # logger.info(f"Seed: {self.noise_config.seed}")
        # logger.info(f"Bounds: p{int(self.noise_config.bounds_lower_percentile*100)}-p{int(self.noise_config.bounds_upper_percentile*100)}")
        # logger.info(f"Strategy: Multiplicative jitter with data-driven bounds")
        # logger.info("=" * 60)
    
    def set_user_level_params(self, d_max: int, k_bound: int, winsorize_cap: float) -> None:
        """Set bounded contribution parameters (D_max, K, winsorize cap) from preprocessing."""
        # [DP] D_max is not used in SDC approach, but kept for compatibility
        # [VERBOSE] logger.info(f"Bounded contribution params: D_max={d_max}, K={k_bound}, Winsorize_cap={winsorize_cap:,.0f}")
    
    def run(self, histogram: SparkHistogram) -> SparkHistogram:
        """
        Apply context-aware plausibility-based noise.
        
        CRITICAL: Province-level totals for transaction_count and transaction_amount_sum
        are maintained EXACTLY (0% error) as they are publicly published data.
        Only MCC and daily breakdowns (which are NOT published) receive noise.
        
        Algorithm:
        1. Compute province invariants (count and amount are EXACT - no noise)
        2. Compute plausibility bounds per (Province, MCC, Weekday) context
        3. Store original ratios per cell
        4. Apply independent multiplicative jitter to all three values (count, cards, amount)
        5. Clamp all values to plausibility bounds and validate/adjust ratios
        6. Scale all three values to match province invariants exactly (0% error)
        7. Re-validate ratios after scaling and adjust if needed
        8. Finalize values and ensure consistency
        9. Controlled rounding (maintains province invariants exactly)
        10. Final validation (verifies 0% error in province totals)
        11. Post-drop verification (ensures invariants remain exact after weekday removal)
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
            # [VERBOSE] logger.info("  Using ORIGINAL (unwinsorized) amounts for invariants, bounds, ratios, and noise")
            self._amount_col = 'total_amount_original'
        else:
            logger.warning("  total_amount_original not found, using winsorized total_amount (legacy behavior)")
            self._amount_col = 'total_amount'
        
        # OPTIMIZATION: Repartition by province ONCE to minimize shuffles
        num_provinces = df.select('province_idx').distinct().count()
        num_partitions = max(31, num_provinces * 2)
        
        # [VERBOSE] logger.info(f"\nOptimization: Repartitioning to {num_partitions} partitions by province_idx")
        df = df.repartition(num_partitions, 'province_idx')
        
        # ========================================
        # PHASE 1: Compute Province Invariants
        # ========================================
        logger.info("\n" + "=" * 70)
        logger.info("PHASE 1: Computing Province Invariants")
        logger.info("=" * 70)
        
        # Use TRUE province invariants if provided, otherwise compute from histogram
        # CRITICAL: True invariants are computed from original raw data BEFORE bounded contribution
        if histogram.province_invariants is not None:
            # [VERBOSE] logger.info("  Using TRUE province invariants from original raw data (before preprocessing)")
            pass
            # Add original_cards_sum from histogram (needed for scaling)
            cards_sum = df.groupBy('province_idx').agg(
                F.sum('unique_cards').alias('original_cards_sum')
            )
            # Join with true invariants
            self._invariants = histogram.province_invariants.join(
                cards_sum, 'province_idx', 'inner'
            ).cache()
        else:
            logger.warning("  Province invariants not provided - computing from histogram (may be incorrect!)")
            logger.warning("  This means invariants are computed AFTER bounded contribution, which drops transactions!")
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
        
        # [VERBOSE] logger.info(f"  Total count: {inv_summary['total_count']:,}")
        # logger.info(f"  Total amount: {inv_summary['total_amount']:,}")
        # logger.info(f"  Provinces: {inv_summary['num_provinces']}")
        
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
        
        # [VERBOSE] logger.info("  ✓ Original ratios stored per cell")
        
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
        
        # [VERBOSE] logger.info(f"  Noise level: ±{noise_level:.0%} (applied to all three values)")
        # logger.info(f"  Seed: {seed} (count), {seed+1} (cards), {seed+2} (amount)")
        # logger.info(f"  Noise factor range: [{1-noise_level:.2f}, {1+noise_level:.2f}]")
        # if min_deviation > 0:
        #     logger.info(f"  Minimum noise deviation: ±{min_deviation:.1%} (prevents zero noise)")
        # logger.info("  ✓ Independent random noise applied to count, cards, and amount")
        
        # Drop intermediate noise columns
        df = df.drop('noise_uniform_count', 'noise_uniform_cards', 'noise_uniform_amount',
                     'noise_factor_count', 'noise_factor_cards', 'noise_factor_amount')
        
        # ========================================
        # PHASE 5: Clamp to Bounds and Validate/Adjust Ratios
        # ========================================
        logger.info("\n" + "=" * 70)
        logger.info("PHASE 5: Clamping to Bounds & Validating Ratios")
        logger.info("=" * 70)
        
        # Join with bounds (context = province_idx, mcc_idx, weekday)
        df = df.join(
            F.broadcast(self._bounds_df),
            ['province_idx', 'mcc_idx', 'weekday'],
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
        # [VERBOSE] logger.info(f"  Count clamped: {total_clamped:,} cells ({pct_clamped:.2f}%)")
        # logger.info("  ✓ Values clamped to bounds")
        
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
            # [VERBOSE] logger.info(f"  Avg amount adjusted: {ratio_stats['avg_amount_adjusted'] or 0:,} cells ({avg_amt_pct:.2f}%)")
            # logger.info(f"  TX per card adjusted: {ratio_stats['tx_per_card_adjusted'] or 0:,} cells ({tx_card_pct:.2f}%)")
        
        # [VERBOSE] logger.info("  ✓ Ratios validated and adjusted to stay within bounds")
        
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
        
        # [VERBOSE] logger.info("  ✓ All three values scaled to match province invariants")
        
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
            # [VERBOSE] logger.info(f"  Post-scaling avg amount adjusted: {post_scale_stats['post_scale_avg_amount_adjusted'] or 0:,} cells ({avg_amt_pct:.2f}%)")
            # logger.info(f"  Post-scaling TX per card adjusted: {post_scale_stats['post_scale_tx_per_card_adjusted'] or 0:,} cells ({tx_card_pct:.2f}%)")
        
        # [VERBOSE] logger.info("  ✓ Ratios re-validated after scaling")
        
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
        
        # [VERBOSE] logger.info(f"  Final total count: {final_stats['total_count']:,.0f}")
        # logger.info(f"  Final total cards: {final_stats['total_cards']:,.0f}")
        # logger.info(f"  Final total amount: {final_stats['total_amount']:,.0f}")
        # logger.info(f"  Total cells: {final_stats['total_cells']:,}")
        # logger.info("  ✓ Values finalized and consistency ensured")
        
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
        
        # [VERBOSE] logger.info("  ✓ Final validation complete")
        
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
        
        # [VERBOSE] logger.info("  ✓ Controlled rounding complete")
        
        # Enforce amount invariants exactly (like count invariants)
        df_rounded = self._enforce_amount_invariants_exact(df_rounded)
        
        # [VERBOSE] logger.info("  ✓ Amount invariants enforced exactly")
        
        # ========================================
        # PHASE 10: Final Validation
        # ========================================
        logger.info("\n" + "=" * 70)
        logger.info("PHASE 10: Final Validation")
        logger.info("=" * 70)
        
        self._final_validation(df_rounded)
        
        logger.info("\n" + "=" * 70)
        logger.info("Context-Aware Noise Processing Complete")
        logger.info("=" * 70)
        
        # Drop weekday column before returning - it's a processing dimension, not a structural dimension
        # The histogram structure is 4D (province, city, mcc, day), not 5D
        # Each (province, city, mcc, day) has exactly one weekday, so no aggregation needed
        # Simply dropping weekday preserves province invariants exactly
        df_final = df_rounded.drop('weekday')
        
        # Verify province invariants are still exact after dropping weekday
        logger.info("\n" + "=" * 70)
        logger.info("Post-Drop Invariant Verification")
        logger.info("=" * 70)
        self._verify_province_invariants_exact(df_final)
        
        # CRITICAL: Preserve province_invariants in returned histogram for downstream use
        # The invariants must be preserved so they can be verified against input data totals
        final_histogram = SparkHistogram(
            self.spark, 
            df_final, 
            histogram.dimensions, 
            histogram.city_codes, 
            histogram.min_date,
            province_invariants=histogram.province_invariants  # Preserve invariants from input
        )
        
        # Cleanup (after verification and histogram creation)
        if self._invariants is not None:
            self._invariants.unpersist()
        if self._bounds_df is not None:
            self._bounds_df.unpersist()
        
        return final_histogram
    
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
        
        # [VERBOSE] logger.info(f"  Computing bounds per context (Province, MCC, Weekday)")
        # logger.info(f"  Lower percentile: p{int(lower_pct*100)}")
        # logger.info(f"  Upper percentile: p{int(upper_pct*100)}")
        # logger.info(f"  Bounds widening factor: {self.noise_config.bounds_widening_factor}x (normal), {self.noise_config.sparse_bounds_widening_factor}x (sparse)")
        # logger.info(f"  Using {amount_col} for bounds computation")
        
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
        bounds_df = df_with_ratios.groupBy('province_idx', 'mcc_idx', 'weekday').agg(
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
        
        # Widen bounds by multiplying the range, keeping center fixed
        # Simple approach: range_new = range_old * factor, then adjust min/max around center
        widening_factor = self.noise_config.bounds_widening_factor
        sparse_widening = self.noise_config.sparse_bounds_widening_factor
        
        # Compute ranges and centers
        bounds_df = bounds_df.withColumn(
            'count_range', F.col('count_max') - F.col('count_min')
        ).withColumn(
            'avg_amount_range', F.col('avg_amount_max') - F.col('avg_amount_min')
        ).withColumn(
            'tx_per_card_range', F.col('tx_per_card_max') - F.col('tx_per_card_min')
        ).withColumn(
            'count_center', (F.col('count_min') + F.col('count_max')) / 2.0
        ).withColumn(
            'avg_amount_center', (F.col('avg_amount_min') + F.col('avg_amount_max')) / 2.0
        ).withColumn(
            'tx_per_card_center', (F.col('tx_per_card_min') + F.col('tx_per_card_max')) / 2.0
        )
        
        # Determine widening factor per context (more for sparse)
        bounds_df = bounds_df.withColumn(
            'effective_widening',
            F.when(F.col('sample_count') < min_samples, sparse_widening)
             .otherwise(widening_factor)
        )
        
        # Multiply range by factor, then adjust min/max around center
        # new_range = old_range * factor
        # new_min = center - new_range/2
        # new_max = center + new_range/2
        bounds_df = bounds_df.withColumn(
            'count_min',
            F.col('count_center') - (F.col('count_range') * F.col('effective_widening')) / 2.0
        ).withColumn(
            'count_max',
            F.col('count_center') + (F.col('count_range') * F.col('effective_widening')) / 2.0
        ).withColumn(
            'avg_amount_min',
            F.col('avg_amount_center') - (F.col('avg_amount_range') * F.col('effective_widening')) / 2.0
        ).withColumn(
            'avg_amount_max',
            F.col('avg_amount_center') + (F.col('avg_amount_range') * F.col('effective_widening')) / 2.0
        ).withColumn(
            'tx_per_card_min',
            F.col('tx_per_card_center') - (F.col('tx_per_card_range') * F.col('effective_widening')) / 2.0
        ).withColumn(
            'tx_per_card_max',
            F.col('tx_per_card_center') + (F.col('tx_per_card_range') * F.col('effective_widening')) / 2.0
        )
        
        # Drop intermediate columns
        bounds_df = bounds_df.drop(
            'count_range', 'avg_amount_range', 'tx_per_card_range',
            'count_center', 'avg_amount_center', 'tx_per_card_center', 'effective_widening'
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
        
        logger.info(f"  Computed bounds for {bounds_stats['num_contexts']:,} contexts")
        # [VERBOSE] logger.info(f"  Avg samples/context: {bounds_stats['avg_samples']:.1f}")
        # logger.info(f"  Sparse contexts (< {min_samples} samples): {bounds_stats['sparse_contexts']:,}")
        # logger.info("  ✓ Plausibility bounds computed")
        
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
    
    def _enforce_amount_invariants_exact(self, df: DataFrame) -> DataFrame:
        """
        Enforce province amount invariants to be exact (0% error) using controlled rounding.
        
        After controlled rounding, amounts may not match province invariants exactly.
        This method uses controlled rounding per province to distribute the difference
        across cells, ensuring the sum matches exactly (similar to count invariants).
        
        Args:
            df: DataFrame after controlled rounding (amounts are already integers)
            
        Returns:
            DataFrame with amounts adjusted to match province invariants exactly
        """
        # Prepare for rounding - need province_idx, amounts, and invariant
        df_for_rounding = df.select(
            'province_idx', 'city_idx', 'mcc_idx', 'day_idx', 'weekday',
            'transaction_count', 'unique_cards', 'total_amount'
        )
        
        # Join with invariants to get target amounts
        df_with_invariants = df_for_rounding.join(
            F.broadcast(self._invariants.select('province_idx', 'invariant_amount')),
            'province_idx',
            'left'
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
        
        def adjust_province_amounts(pdf):
            """
            Adjust amounts for one province to match invariant exactly.
            
            Uses controlled rounding: compute difference and distribute across cells.
            """
            import pandas as pd
            import numpy as np
            
            if len(pdf) == 0:
                return pd.DataFrame(columns=[
                    'province_idx', 'city_idx', 'mcc_idx', 'day_idx', 'weekday',
                    'transaction_count', 'unique_cards', 'total_amount'
                ])
            
            target_amount = int(pdf['invariant_amount'].iloc[0])
            current_amounts = pdf['total_amount'].values.copy().astype(np.int64)
            current_sum = current_amounts.sum()
            diff = target_amount - current_sum
            
            # If already exact, return as-is
            if diff == 0:
                return pdf[[
                    'province_idx', 'city_idx', 'mcc_idx', 'day_idx', 'weekday',
                    'transaction_count', 'unique_cards', 'total_amount'
                ]].copy()
            
            # For controlled rounding, we need to distribute the difference
            # Use a priority based on current amount (higher amounts get priority for increases,
            # lower amounts get priority for decreases, but we want to minimize changes)
            
            if diff > 0:
                # Need to add diff units total
                # Strategy: add 1 to cells with highest amounts (preserves relative distribution)
                # But we want to be fair, so use a random-like priority based on amount
                # Actually, let's use a deterministic approach: prioritize cells with higher amounts
                # but use fractional part of amount/transaction_count as tiebreaker
                
                # Compute priority: use amount itself as primary, count as secondary
                # This ensures we adjust cells proportionally
                priority = current_amounts.astype(np.float64)
                # Add small random-like component based on cell index for tiebreaking
                priority = priority + (np.arange(len(priority)) * 0.0001)
                
                # Sort by priority (descending) and add 1 to top diff cells
                sorted_indices = np.argsort(-priority)
                for i in range(min(diff, len(sorted_indices))):
                    idx = sorted_indices[i]
                    current_amounts[idx] += 1
                    
            elif diff < 0:
                # Need to subtract abs(diff) units total
                # Strategy: subtract 1 from cells with lowest amounts (but keep >= 0)
                n_down = int(-diff)
                
                # Only consider cells with amount > 0
                nonzero_mask = current_amounts > 0
                if nonzero_mask.sum() > 0:
                    nonzero_indices = np.where(nonzero_mask)[0]
                    # Sort by amount (ascending) - subtract from smallest first
                    priority = current_amounts[nonzero_indices].astype(np.float64)
                    # Add small component for tiebreaking
                    priority = priority - (np.arange(len(nonzero_indices)) * 0.0001)
                    sorted_nz = nonzero_indices[np.argsort(priority)]
                    
                    adjusted = 0
                    for idx in sorted_nz:
                        if adjusted >= n_down:
                            break
                        if current_amounts[idx] > 0:
                            current_amounts[idx] -= 1
                            adjusted += 1
                    
                    # If we still need to adjust more, continue with remaining cells
                    if adjusted < n_down:
                        for idx in sorted_nz:
                            if adjusted >= n_down:
                                break
                            if current_amounts[idx] > 0:
                                current_amounts[idx] -= 1
                                adjusted += 1
            
            # Ensure non-negativity
            current_amounts = np.maximum(0, current_amounts)
            
            # Ensure consistency: if count=0, amount must be 0
            counts = pdf['transaction_count'].values
            zero_count_mask = counts == 0
            current_amounts[zero_count_mask] = 0
            
            # Verify sum matches (should be exact now)
            final_sum = current_amounts.sum()
            if final_sum != target_amount:
                # This should not happen, but if it does, force exact match on one cell
                # (last resort - should be rare)
                if final_sum < target_amount:
                    # Add difference to largest cell
                    max_idx = np.argmax(current_amounts)
                    current_amounts[max_idx] += (target_amount - final_sum)
                elif final_sum > target_amount:
                    # Subtract difference from largest cell (but keep >= 0)
                    max_idx = np.argmax(current_amounts)
                    current_amounts[max_idx] = max(0, current_amounts[max_idx] - (final_sum - target_amount))
            
            return pd.DataFrame({
                'province_idx': pdf['province_idx'].values.astype(np.int32),
                'city_idx': pdf['city_idx'].values.astype(np.int32),
                'mcc_idx': pdf['mcc_idx'].values.astype(np.int32),
                'day_idx': pdf['day_idx'].values.astype(np.int32),
                'weekday': pdf['weekday'].values.astype(np.int32),
                'transaction_count': pdf['transaction_count'].values,
                'unique_cards': pdf['unique_cards'].values,
                'total_amount': current_amounts
            })
        
        df_adjusted = df_with_invariants.groupBy('province_idx').applyInPandas(
            adjust_province_amounts, schema=output_schema
        )
        
        return df_adjusted
    
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
            # Show details
            count_error_details = comparison.filter(F.col('count_diff') != 0).select(
                'province_idx', 'invariant_count', 'actual_count', 'count_diff'
            ).collect()
            for row in count_error_details[:5]:  # Show first 5
                logger.error(f"    Province {row['province_idx']}: "
                            f"Expected={row['invariant_count']}, "
                            f"Actual={row['actual_count']}, "
                            f"Error={row['count_diff']}")
            raise RuntimeError(
                f"COUNT invariant violated in {count_mismatches} provinces! "
                f"Province totals must be EXACT (0% error) as they are publicly published."
            )
        else:
            logger.info("  ✓ COUNT invariant exact in all provinces")
        
        # Check AMOUNT invariant (must be exact like count)
        comparison = comparison.withColumn(
            'amount_diff',
            F.col('actual_amount') - F.col('invariant_amount')
        )
        
        amount_mismatches = comparison.filter(F.col('amount_diff') != 0).count()
        
        if amount_mismatches > 0:
            logger.error(f"  ✗ AMOUNT invariant violated in {amount_mismatches} provinces!")
            # Show details
            amount_error_details = comparison.filter(F.col('amount_diff') != 0).select(
                'province_idx', 'invariant_amount', 'actual_amount', 'amount_diff'
            ).collect()
            for row in amount_error_details[:5]:  # Show first 5
                logger.error(f"    Province {row['province_idx']}: "
                            f"Expected={row['invariant_amount']}, "
                            f"Actual={row['actual_amount']}, "
                            f"Error={row['amount_diff']}")
            raise RuntimeError(
                f"AMOUNT invariant violated in {amount_mismatches} provinces! "
                f"Province totals must be EXACT (0% error) as they are publicly published."
            )
        else:
            logger.info("  ✓ AMOUNT invariant exact in all provinces")
        
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
        # [VERBOSE] logger.info("\n  Ratio Bounds Validation:")
        
        # Join rounded data with bounds
        df_with_bounds = df_rounded.join(
            F.broadcast(self._bounds_df.select(
                'province_idx', 'mcc_idx', 'weekday',
                'avg_amount_min', 'avg_amount_max',
                'tx_per_card_min', 'tx_per_card_max'
            )),
            ['province_idx', 'mcc_idx', 'weekday'],
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
            # [VERBOSE] logger.info(f"\n  Summary - Active cells: {ratio_stats['active_cells']:,}")
            # if ratio_stats['mean_tx_per_card'] is not None:
            #     logger.info(f"  Mean TX per card: {ratio_stats['mean_tx_per_card']:.2f}")
            # if ratio_stats['mean_avg_amount'] is not None:
            #     logger.info(f"  Mean avg amount: {ratio_stats['mean_avg_amount']:,.2f}")
            pass
    
    def _verify_province_invariants_exact(self, df: DataFrame) -> None:
        """
        Verify province invariants are EXACT (0% error).
        
        CRITICAL: Province-level totals for transaction_count and transaction_amount_sum
        must be EXACTLY equal to the original province totals from the INPUT DATA FRAME (0% error)
        because they are publicly published data.
        
        The invariants in self._invariants were computed from the original input data frame
        BEFORE any preprocessing (bounded contribution, winsorization, etc.). This verification
        ensures that after all processing (noise, scaling, rounding), the final output totals
        match the input data frame totals exactly.
        
        Args:
            df: DataFrame to verify (should have transaction_count and total_amount columns)
            
        Raises:
            RuntimeError: If any province invariant is violated (non-zero error)
        """
        # Compute actual province totals from final output
        actual = df.groupBy('province_idx').agg(
            F.sum('transaction_count').alias('actual_count'),
            F.sum('total_amount').alias('actual_amount')
        )
        
        # Join with expected invariants (computed from original input data frame)
        # CRITICAL: self._invariants contains the EXACT totals from the input data frame
        comparison = self._invariants.join(actual, 'province_idx', 'inner')
        comparison = comparison.withColumn(
            'count_error', F.col('actual_count') - F.col('invariant_count')
        ).withColumn(
            'amount_error', F.col('actual_amount') - F.col('invariant_amount')
        )
        
        # Check for any errors
        errors = comparison.filter(
            (F.col('count_error') != 0) | (F.col('amount_error') != 0)
        )
        
        error_count = errors.count()
        if error_count > 0:
            logger.error(f"  ✗ Province invariants violated in {error_count} provinces!")
            
            # Show details
            error_details = errors.select(
                'province_idx', 'invariant_count', 'actual_count', 'count_error',
                'invariant_amount', 'actual_amount', 'amount_error'
            ).collect()
            
            for row in error_details[:5]:  # Show first 5
                logger.error(f"    Province {row['province_idx']}: "
                            f"Count error={row['count_error']}, "
                            f"Amount error={row['amount_error']}")
            
            raise RuntimeError(
                f"Province invariants violated in {error_count} provinces. "
                f"Expected 0% error, but found differences. "
                f"Province totals must be EXACT (0% error) as they are publicly published."
            )
        else:
            logger.info("  ✓ Province invariants EXACT (0% error) - verification passed")
