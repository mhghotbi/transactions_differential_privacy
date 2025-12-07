"""
Spark-Native Top-Down DP Engine with Province-Month Level Invariants.

This module implements differential privacy operations using 100% PySpark,
with ZERO collect(), toLocalIterator(), or toPandas() calls.

All data remains distributed across Spark executors. The driver only holds
small metadata (~1 KB vs 50-200 GB of data).

KEY FEATURES:
1. Province-month totals are EXACT INVARIANTS (publicly published data)
2. All DP noise applied via Spark UDFs (distributed sampling)
3. NNLS post-processing via Window functions (no driver collection)
4. Controlled rounding via applyInPandas (per-province chunks)
5. MCC group processing via filter + union (no fancy indexing)
"""

import math
import logging
from typing import Dict, Optional, List, Tuple, Any
from fractions import Fraction
from functools import reduce

import numpy as np
from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, StructType, StructField, IntegerType

from core.config import Config
from schema.geography import Geography
from schema.histogram_spark import SparkHistogram
from core.budget import Budget, BudgetAllocator
from core.sensitivity import UserLevelSensitivity


logger = logging.getLogger(__name__)


class TopDownSparkEngine:
    """
    Spark-native top-down differential privacy engine.
    
    This implements Census 2020 DAS-style algorithm using 100% PySpark:
    1. Province-month totals as exact invariants (NO noise)
    2. Cell-level DP noise via Spark UDFs (distributed)
    3. NNLS via Window functions (no collect)
    4. Controlled rounding via applyInPandas (chunked)
    
    CRITICAL: ZERO numpy operations on full dataset.
    All data stays in Spark DataFrames (distributed).
    """
    
    def __init__(
        self,
        spark: SparkSession,
        config: Config,
        geography: Geography,
        budget: Budget
    ):
        """
        Initialize Spark-native DP engine.
        
        Args:
            spark: Active Spark session
            config: Configuration object
            geography: Geography instance
            budget: Privacy budget manager
        """
        self.spark = spark
        self.config = config
        self.geography = geography
        self.budget = budget
        self.allocator = BudgetAllocator(budget, config.data.num_days)
        
        # Store province invariants as small cached DataFrames (~32 rows)
        self._province_invariants: Dict[str, DataFrame] = {}
        
        # MCC group configuration
        self._mcc_group_caps = config.privacy.mcc_group_caps or {}
        self._mcc_to_group = config.privacy.mcc_to_group or {}
        self._mcc_grouping_enabled = config.privacy.mcc_grouping_enabled and bool(self._mcc_group_caps)
        
        # User-level sensitivity parameters
        self._d_max: Optional[int] = None
        self._user_level_sensitivities: Dict[str, UserLevelSensitivity] = {}
        self._winsorize_cap: float = 1.0
        
        logger.info(f"[TopDownSparkEngine] Initialized (100% Spark, ZERO numpy)")
        logger.info(f"[TopDownSparkEngine] MCC grouping enabled: {self._mcc_grouping_enabled}")
    
    def set_user_level_params(
        self,
        d_max: int,
        k_bound: int,
        winsorize_cap: float
    ) -> None:
        """Set user-level DP parameters."""
        self._d_max = d_max
        self._winsorize_cap = winsorize_cap
        
        sqrt_d = math.sqrt(d_max)
        
        self._user_level_sensitivities = {
            "transaction_count": UserLevelSensitivity(
                query_name="transaction_count",
                max_cells_per_user=d_max,
                per_cell_contribution=float(k_bound),
                l2_sensitivity=sqrt_d * k_bound,
                l1_sensitivity=d_max * k_bound,
                sensitivity_type="count"
            ),
            "unique_cards": UserLevelSensitivity(
                query_name="unique_cards",
                max_cells_per_user=d_max,
                per_cell_contribution=1.0,
                l2_sensitivity=sqrt_d,
                l1_sensitivity=float(d_max),
                sensitivity_type="unique"
            ),
            "total_amount": UserLevelSensitivity(
                query_name="total_amount",
                max_cells_per_user=d_max,
                per_cell_contribution=winsorize_cap,
                l2_sensitivity=sqrt_d * winsorize_cap,
                l1_sensitivity=d_max * winsorize_cap,
                sensitivity_type="sum"
            ),
        }
        
        logger.info("=" * 60)
        logger.info("User-Level DP Parameters Set (Spark Mode)")
        logger.info("=" * 60)
        logger.info(f"D_max: {d_max}, sqrt(D_max): {sqrt_d:.4f}, K: {k_bound}")
        for name, sens in self._user_level_sensitivities.items():
            logger.info(f"  {name}: L2={sens.l2_sensitivity:,.4f}")
        logger.info("=" * 60)
    
    def run(self, histogram: SparkHistogram) -> SparkHistogram:
        """
        Apply top-down DP with province-month invariants using 100% Spark.
        
        Algorithm:
        1. Compute province invariants (small Spark aggregation, cached)
        2. Drop total_amount_original column (frees memory)
        3. Apply cell-level noise (Spark UDFs, distributed)
        4. NNLS post-processing (Window functions)
        5. Controlled rounding (applyInPandas)
        6. Verify invariants
        
        Args:
            histogram: SparkHistogram with original values
            
        Returns:
            SparkHistogram with DP-protected values
        """
        logger.info("=" * 60)
        logger.info("Top-Down DP (100% Spark, ZERO collect)")
        logger.info("=" * 60)
        logger.info(f"Total privacy budget (rho): {self.budget.total_rho}")
        logger.info(f"Epsilon at delta={self.budget.delta}: {self.budget.total_epsilon:.4f}")
        
        # Verify user-level params
        if self._d_max is None:
            self._handle_missing_params()
        
        # Step 1: Compute province invariants (small aggregation, cacheable)
        logger.info("")
        logger.info("=" * 40)
        logger.info("Step 1: Compute Province Invariants")
        logger.info("=" * 40)
        self._compute_province_invariants(histogram)
        
        # Step 2: Drop total_amount_original (frees memory)
        if 'total_amount_original' in histogram.df.columns:
            histogram = histogram.drop_column('total_amount_original')
            logger.info("Dropped total_amount_original column (memory freed)")
        else:
            logger.info("total_amount_original column not present (already dropped or not created)")
        
        # Step 3: Apply cell-level noise
        logger.info("")
        logger.info("=" * 40)
        logger.info("Step 2: Cell-Level Noise (Spark UDFs)")
        logger.info("=" * 40)
        histogram = self._apply_cell_level_noise(histogram)
        
        # Step 4: NNLS post-processing
        logger.info("")
        logger.info("=" * 40)
        logger.info("Step 3: NNLS Post-Processing (Window Functions)")
        logger.info("=" * 40)
        histogram = self._nnls_post_process(histogram)
        
        # Step 5: Controlled rounding
        logger.info("")
        logger.info("=" * 40)
        logger.info("Step 4: Controlled Rounding (applyInPandas)")
        logger.info("=" * 40)
        histogram = self._controlled_rounding(histogram)
        
        # Step 6: Verify invariants
        logger.info("")
        logger.info("=" * 40)
        logger.info("Verification: Province Invariants Check")
        logger.info("=" * 40)
        self._verify_invariants(histogram)
        
        logger.info("")
        logger.info("=" * 60)
        logger.info("Top-Down DP Complete (100% Spark)")
        logger.info("=" * 60)
        
        return histogram
    
    def _handle_missing_params(self) -> None:
        """Handle missing user-level params."""
        logger.warning("WARNING: User-level params not set, using fallback")
        K = self.config.privacy.computed_contribution_bound or 1
        self.set_user_level_params(d_max=1, k_bound=K, winsorize_cap=1.0)
    
    def _compute_province_invariants(self, histogram: SparkHistogram) -> None:
        """
        Compute province-month invariants using Spark aggregations.
        
        These are PUBLIC DATA that must be matched exactly.
        Returns small DataFrames (~32 rows) that can be cached and broadcast.
        
        NO collect() - results stay in Spark.
        """
        queries = SparkHistogram.OUTPUT_QUERIES
        
        # Validate required columns exist
        available_cols = set(histogram.df.columns)
        for query in queries:
            if query not in available_cols:
                raise ValueError(
                    f"Required column '{query}' not found in histogram. "
                    f"Available columns: {sorted(available_cols)}"
                )
        
        for query in queries:
            # Use ORIGINAL amounts for total_amount invariants
            query_col = 'total_amount_original' if query == 'total_amount' else query
            
            if query_col not in histogram.df.columns:
                # For total_amount, fall back to the column itself if original not available
                if query == 'total_amount':
                    logger.warning(f"Column 'total_amount_original' not found, using 'total_amount' for invariants")
                    query_col = 'total_amount'
                else:
                    raise ValueError(
                        f"Required column '{query_col}' not found in histogram. "
                        f"Available columns: {sorted(available_cols)}"
                    )
            
            # Aggregate to province level (returns ~32 rows)
            # Cast to long to avoid Decimal/BigDecimal reflection warnings
            inv_df = histogram.df.groupBy('province_idx').agg(
                F.sum(F.col(query_col)).cast('long').alias('invariant')
            ).cache()  # Cache small result (~32 rows = ~1 KB)
            
            self._province_invariants[query] = inv_df
            
            # Log total (triggers cache materialization, but only 32 rows)
            total_row = inv_df.agg(F.sum(F.col('invariant')).alias('total')).first()
            total = total_row['total'] if total_row and total_row['total'] is not None else 0
            num_provinces = inv_df.count()
            
            logger.info(f"  {query}: total={total:,}, provinces={num_provinces}")
        
        logger.info("  [Province invariants cached as small DataFrames]")
    
    def _apply_cell_level_noise(self, histogram: SparkHistogram) -> SparkHistogram:
        """
        Apply DP noise to all cells using Spark UDFs (distributed sampling).
        
        MEMORY OPTIMIZATION: Replaces original columns immediately (no intermediate _noisy columns).
        NO collect() - noise sampled on executors, results stay in Spark.
        """
        queries = SparkHistogram.OUTPUT_QUERIES
        total_rho = float(self.budget.total_rho)
        
        # SCIENTIFIC VALIDATION: Verify budget allocation sums correctly
        query_split_sum = sum(self.config.privacy.query_split.values())
        if abs(query_split_sum - 1.0) > 1e-9:
            raise ValueError(
                f"Query budget split sums to {query_split_sum:.10f}, not 1.0. "
                f"This violates privacy budget accounting. "
                f"Splits: {self.config.privacy.query_split}"
            )
        logger.info(f"  Budget validation: Query split sums to {query_split_sum:.10f} (VALID)")
        
        # Start with current DataFrame
        df = histogram.df
        
        for query in queries:
            query_weight = self.config.privacy.query_split.get(query, 1.0 / len(queries))
            rho = Fraction(total_rho * query_weight).limit_denominator(10000)
            sensitivity = self._get_sensitivity(query)
            
            # Validate rho > 0
            if float(rho) <= 0:
                raise ValueError(f"Invalid privacy budget for {query}: rho={float(rho)} must be > 0")
            
            sigma = np.sqrt(sensitivity**2 / (2 * float(rho)))
            
            if sigma <= 0 or not np.isfinite(sigma):
                raise ValueError(f"Invalid sigma for {query}: sigma={sigma} (rho={float(rho)}, sensitivity={sensitivity})")
            
            logger.info(f"  {query}: ρ={float(rho):.4f}, Δ₂={sensitivity:.2f}, σ={sigma:.2f}")
            
            # MEMORY OPTIMIZATION: Replace column directly (no intermediate _noisy column)
            # This saves memory for billions of rows (3 queries × N rows × 8 bytes = 24N bytes saved)
            df = df.withColumn(
                query,
                self._discrete_gaussian_udf(F.col(query).cast('double'), sigma)
            )
        
        return SparkHistogram(self.spark, df, histogram.dimensions, histogram.city_codes)
    
    @staticmethod
    def _discrete_gaussian_udf(col, sigma: float):
        """
        Create UDF for exact Census 2020 discrete Gaussian noise.
        
        Uses the exact algorithm from US Census Bureau's 2020 DAS:
        - Rational arithmetic (no floating-point in core algorithm)
        - Discrete Laplace → Discrete Gaussian via acceptance test
        - Bernoulli(exp(-gamma)) for exact probabilities
        """
        # Convert sigma to sigma_sq as fraction (numerator, denominator)
        from fractions import Fraction
        sigma_sq_frac = Fraction(sigma * sigma).limit_denominator(1000000)
        ssq_n = sigma_sq_frac.numerator
        ssq_d = sigma_sq_frac.denominator
        
        @F.udf(returnType=DoubleType())
        def add_discrete_gaussian_noise(value):
            """
            Add exact discrete Gaussian noise (Census 2020 algorithm).
            """
            import random
            
            if value is None:
                return None
            
            # Convert to float
            try:
                val_float = float(value)
            except (TypeError, ValueError):
                val_float = 0.0
            
            # === Census 2020 Exact Discrete Gaussian Algorithm ===
            
            def floorsqrt(n, d):
                """Compute floor(sqrt(n/d)) exactly using only integer arithmetic."""
                if n <= 0 or d <= 0:
                    return 0
                # Binary search: maintain a^2 <= n/d < b^2
                a = 0
                b = 1
                # Double b until b^2 > n/d
                while b * b * d <= n:
                    b = 2 * b
                # Binary search
                while a + 1 < b:
                    c = (a + b) // 2
                    if c * c * d <= n:
                        a = c
                    else:
                        b = c
                return a
            
            def bernoulli_exp(gn, gd):
                """Sample Bernoulli(exp(-gn/gd)) exactly."""
                if gn <= 0:
                    return 1
                if 0 <= gn <= gd:
                    k = 1
                    a = True
                    while a:
                        a = random.randint(0, gd * k - 1) < gn
                        k = k + 1 if a else k
                    return k % 2
                else:
                    for _ in range(gn // gd):
                        if not bernoulli_exp(1, 1):
                            return 0
                    return bernoulli_exp(gn % gd, gd)
            
            def discrete_laplace(s, t):
                """Sample from Discrete Laplace with scale t/s."""
                while True:
                    d = False
                    while not d:
                        u = random.randint(0, t - 1)
                        d = bool(bernoulli_exp(u, t))
                    v = 0
                    a = True
                    while a:
                        a = bool(bernoulli_exp(1, 1))
                        v = v + 1 if a else v
                    x = u + t * v
                    y = x // s
                    b = random.randint(0, 1)  # Bernoulli(1/2): 0 or 1
                    if not (b == 1 and y == 0):
                        return (1 - 2 * b) * y
            
            def discrete_gaussian(ssq_n, ssq_d):
                """Sample from Discrete Gaussian with variance ssq_n/ssq_d."""
                t = floorsqrt(ssq_n, ssq_d) + 1
                while True:
                    y = discrete_laplace(1, t)
                    aux1n = abs(y) * t * ssq_d - ssq_n
                    gamma_n = aux1n * aux1n
                    gamma_d = t * ssq_d * t * ssq_n * 2
                    if gamma_d > 0 and bernoulli_exp(gamma_n, gamma_d):
                        return y
            
            # Sample noise and add to value
            noise = discrete_gaussian(ssq_n, ssq_d)
            return val_float + float(noise)
        
        return add_discrete_gaussian_noise(col)
    
    def _nnls_post_process(self, histogram: SparkHistogram) -> SparkHistogram:
        """
        NNLS post-processing using Spark Window functions.
        
        For each query:
        1. Join with province invariants (broadcast small table)
        2. Compute province sums using Window
        3. Scale to match invariants
        4. Ensure non-negativity
        
        NO collect() - all operations are Spark transformations.
        """
        queries = SparkHistogram.OUTPUT_QUERIES
        df = histogram.df
        
        for query in queries:
            adjusted_col = f'{query}_adjusted'
            
            if query not in self._province_invariants:
                logger.warning(f"No invariant for {query}, skipping NNLS")
                df = df.withColumn(adjusted_col, F.col(query))
                continue
            
            # Join with province invariants (broadcast join - small table)
            df = df.join(
                F.broadcast(
                    self._province_invariants[query].withColumnRenamed('invariant', f'{query}_target')
                ),
                on='province_idx',
                how='left'
            )
            
            # Window to compute province sums
            window = Window.partitionBy('province_idx')
            
            # NNLS: clip negative, then scale to match invariant
            # Use query column directly (already contains noisy values from _apply_cell_level_noise)
            df = df.withColumn(
                f'{query}_clipped',
                F.greatest(F.col(query).cast('double'), F.lit(0.0))
            ).withColumn(
                f'{query}_province_sum',
                F.sum(F.col(f'{query}_clipped')).over(window).cast('double')
            ).withColumn(
                adjusted_col,
                F.when(
                    F.col(f'{query}_province_sum') > 0,
                    F.col(f'{query}_clipped') * (F.col(f'{query}_target') / F.col(f'{query}_province_sum'))
                ).otherwise(
                    # If all clipped to zero, distribute target uniformly
                    F.col(f'{query}_target') / F.count('*').over(window)
                )
            ).drop(f'{query}_clipped', f'{query}_province_sum', f'{query}_target')
            
            logger.info(f"  {query}: NNLS adjustment applied")
        
        return SparkHistogram(self.spark, df, histogram.dimensions, histogram.city_codes)
    
    def _controlled_rounding(self, histogram: SparkHistogram) -> SparkHistogram:
        """
        Controlled rounding using applyInPandas (per-province chunks).
        
        Each province is processed independently on executors.
        Memory per executor = (cities × mccs × days) / num_executors.
        
        NO full dataset collection - pandas used only for per-province chunks.
        """
        queries = SparkHistogram.OUTPUT_QUERIES
        df = histogram.df
        
        # Define output schema
        output_schema = StructType([
            StructField('province_idx', IntegerType(), False),
            StructField('city_idx', IntegerType(), False),
            StructField('mcc_idx', IntegerType(), False),
            StructField('day_idx', IntegerType(), False),
        ] + [
            StructField(query, LongType(), False)
            for query in queries
        ])
        
        # MEMORY OPTIMIZATION: Consolidate all invariant joins into ONE operation
        # Instead of 3 separate joins (one per query), combine all invariants first
        if self._province_invariants:
            # Start with first invariant
            combined_invariants = None
            for query in queries:
                if query in self._province_invariants:
                    inv_df = self._province_invariants[query].withColumnRenamed('invariant', f'{query}_invariant')
                    if combined_invariants is None:
                        combined_invariants = inv_df
                    else:
                        combined_invariants = combined_invariants.join(inv_df, on='province_idx', how='outer')
            
            # Single broadcast join (saves memory and reduces shuffle)
            if combined_invariants is not None:
                df = df.join(F.broadcast(combined_invariants), on='province_idx', how='left')
        
        # Define rounding function (applied per province group)
        def round_province_group(pdf):
            """Round values in one province to match invariant."""
            import pandas as pd
            import random
            import math
            
            # Process each query
            for query in queries:
                adjusted_col = f'{query}_adjusted'
                invariant_col = f'{query}_invariant'
                
                if adjusted_col not in pdf.columns:
                    # If no adjusted column, use original value or 0
                    if query in pdf.columns:
                        pdf[query] = pdf[query].astype('int64')
                    else:
                        pdf[query] = [0] * len(pdf)
                    continue
                
                if invariant_col in pdf.columns:
                    # Get target sum for this province
                    target_sum = int(pdf[invariant_col].iloc[0])
                else:
                    # No invariant - simple rounding
                    pdf[query] = pdf[adjusted_col].round().clip(lower=0).astype('int64')
                    continue
                
                # MEMORY OPTIMIZATION: Use pandas Series operations (faster than Python lists)
                # Pandas is allowed in applyInPandas - this avoids list conversion overhead
                values_series = pdf[adjusted_col]
                n = len(values_series)
                
                if target_sum <= 0:
                    pdf[query] = pd.Series([0] * n, dtype='int64')
                    continue
                
                # Vectorized floor operation (pandas is much faster than list comprehension)
                floors_series = values_series.apply(lambda v: max(0, int(math.floor(v))))
                fracs_series = (values_series - floors_series).clip(lower=0.0)
                
                floor_sum = int(floors_series.sum())
                num_round_up = target_sum - floor_sum
                
                if num_round_up <= 0:
                    # Need to reduce - convert to list only when necessary
                    rounded = floors_series.tolist()
                    while sum(rounded) > target_sum and sum(1 for r in rounded if r > 0) > 0:
                        non_zero = [i for i, r in enumerate(rounded) if r > 0]
                        if not non_zero:
                            break
                        idx = random.choice(non_zero)
                        rounded[idx] -= 1
                    pdf[query] = pd.Series(rounded, dtype='int64')
                elif num_round_up >= n:
                    # Need to increase - vectorized operation
                    rounded_series = floors_series + 1
                    extra = num_round_up - n
                    if extra > 0:
                        # Add extra increments randomly
                        for _ in range(int(extra)):
                            idx = random.randint(0, n - 1)
                            rounded_series.iloc[idx] += 1
                    pdf[query] = rounded_series.astype('int64')
                else:
                    # Probabilistic rounding - use pandas operations
                    frac_sum = fracs_series.sum()
                    if frac_sum > 0:
                        probs_series = fracs_series / frac_sum
                    else:
                        probs_series = pd.Series([1.0 / n] * n)
                    
                    # Ensure valid probabilities (vectorized)
                    probs_series = probs_series.clip(lower=1e-10)
                    probs_series = probs_series / probs_series.sum()
                    
                    # Weighted sampling - use numpy for better precision
                    rounded_series = floors_series.copy()
                    try:
                        import numpy as np
                        probs_array = probs_series.to_numpy()
                        # Use numpy.random.choice with probabilities for precise weighted sampling
                        # This avoids precision loss from int conversion
                        round_up_idx = np.random.choice(
                            n,
                            size=int(num_round_up),
                            replace=False,
                            p=probs_array
                        )
                        for idx in round_up_idx:
                            rounded_series.iloc[idx] += 1
                    except (ValueError, Exception) as e:
                        # Fallback: use adaptive scaling for random.sample with counts
                        try:
                            probs_list = probs_series.tolist()
                            indices = list(range(n))
                            # Adaptive scaling: ensure no probability rounds to 0
                            scale_factor = max(1000000, int(num_round_up * 100))
                            round_up_idx = random.sample(
                                population=indices,
                                counts=[max(1, int(p * scale_factor)) for p in probs_list],
                                k=int(num_round_up)
                            )
                            for idx in round_up_idx:
                                rounded_series.iloc[idx] += 1
                        except (ValueError, Exception):
                            # Last resort: uniform random selection
                            if num_round_up > 0:
                                for _ in range(int(num_round_up)):
                                    idx = random.randint(0, n - 1)
                                    rounded_series.iloc[idx] += 1
                    
                    pdf[query] = rounded_series.astype('int64')
            
            # Keep only required columns
            keep_cols = ['province_idx', 'city_idx', 'mcc_idx', 'day_idx'] + list(queries)
            return pdf[keep_cols]
        
        # Apply rounding per province (distributed)
        logger.info("  Applying controlled rounding per province (distributed)")
        rounded_df = df.groupBy('province_idx').applyInPandas(
            round_province_group,
            schema=output_schema
        )
        
        return SparkHistogram(self.spark, rounded_df, histogram.dimensions, histogram.city_codes)
    
    def _verify_invariants(self, histogram: SparkHistogram) -> None:
        """
        Verify province invariants match exactly.
        
        Uses Spark aggregations - NO collect of full data.
        Only collects small comparison results (~32 rows).
        """
        queries = SparkHistogram.OUTPUT_QUERIES
        all_valid = True
        
        for query in queries:
            if query not in self._province_invariants:
                continue
            
            # Compute actual province sums
            # Cast to long to avoid Decimal/BigDecimal reflection warnings
            actual = histogram.df.groupBy('province_idx').agg(
                F.sum(F.col(query)).cast('long').alias('actual_sum')
            )
            
            # Join with expected invariants
            comparison = actual.join(
                self._province_invariants[query].withColumnRenamed('invariant', 'expected_sum'),
                on='province_idx'
            ).withColumn(
                'diff',
                F.col('actual_sum') - F.col('expected_sum')
            )
            
            # Check for mismatches (only collect small result)
            mismatches = comparison.filter(F.col('diff') != 0).count()
            
            if mismatches > 0:
                all_valid = False
                logger.error(f"  {query}: ✗ {mismatches} provinces with mismatches")
                # Show first few mismatches
                comparison.filter(F.col('diff') != 0).show(5)
            else:
                logger.info(f"  {query}: ✓ All provinces match exactly")
        
        if all_valid:
            logger.info("  [All invariants verified]")
        else:
            logger.error("  [CRITICAL: Invariant mismatches detected]")
    
    def _get_sensitivity(self, query: str) -> float:
        """Get L2 sensitivity for a query."""
        if self._user_level_sensitivities and query in self._user_level_sensitivities:
            return self._user_level_sensitivities[query].l2_sensitivity
        
        # Fallback
        K = self.config.privacy.computed_contribution_bound or 1
        d_max = self._d_max or 1
        sqrt_d = math.sqrt(d_max)
        
        if query == 'total_amount':
            return sqrt_d * self._winsorize_cap
        elif query == 'unique_cards':
            return sqrt_d
        else:
            return sqrt_d * K

