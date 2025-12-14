"""
Transaction Data Preprocessor.

Handles:
- Winsorization of transaction amounts
- Day index calculation
- Aggregation to histogram structure
"""

import logging
from typing import Dict, List, Tuple, Optional
from datetime import date, timedelta

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructType, StructField, DoubleType
from pyspark.sql.window import Window

from core.config import Config
from schema.geography import Geography
from schema.histogram import TransactionHistogram


logger = logging.getLogger(__name__)


class TransactionPreprocessor:
    """
    Preprocesses transaction data for SDC processing.
    
    Steps:
    1. Compute per-MCC winsorization caps to handle outliers
    2. Apply bounded contribution (K) to limit cards' per-cell contributions
    3. Apply winsorization to transaction amounts
    4. Calculate day and weekday indices
    5. Create dimension indices (province, city, mcc)
    6. Aggregate to histogram structure per (province, city, mcc, day, weekday)
    """
    
    def __init__(
        self,
        spark: SparkSession,
        config: Config,
        geography: Geography
    ):
        """
        Initialize preprocessor.
        
        Args:
            spark: Active Spark session
            config: Configuration object
            geography: Geography instance
        """
        self.spark = spark
        self.config = config
        self.geography = geography
        
        self._winsorize_cap = None
        self._mcc_to_idx: Dict[str, int] = {}
        self._city_to_idx: Dict[str, int] = {}
        self._min_date: Optional[date] = None
        self._mcc_caps = None  # Dict[str, float]: mcc_code -> cap
        self._d_max: Optional[int] = None  # Max cells per card (for bounded contribution analysis)
        self._province_invariants_df: Optional[DataFrame] = None  # TRUE province invariants from original data
    
    def process(self, df: DataFrame) -> TransactionHistogram:
        """
        Process transaction data into histogram.
        
        Args:
            df: Raw transaction DataFrame (with province columns added)
            
        Returns:
            TransactionHistogram with aggregated data
        """
        logger.info("Starting preprocessing...")
        
        # Step 0A: Compute TRUE province invariants from ORIGINAL raw data
        # CRITICAL: Must be done BEFORE bounded contribution drops transactions
        province_invariants = self._compute_province_invariants(df)
        self._province_invariants_df = province_invariants['province_df']
        
        # Step 0: Compute per-MCC winsorization caps
        # Each MCC is processed separately (parallel composition)
        df = self._compute_per_mcc_caps(df)
        
        # Step 1: Apply bounded contribution (can now use MCC groups for memory efficiency)
        df = self._apply_bounded_contribution(df)
        
        # Step 3: Compute and apply winsorization (per-group if enabled)
        df = self._apply_winsorization(df)
        
        # Step 4: Create dimension indices
        df = self._create_indices(df)
        
        # Step 5: Aggregate to histogram
        histogram = self._aggregate_to_histogram(df)
        
        logger.info("Preprocessing complete")
        logger.info(histogram.summary())
        
        return histogram
    
    def _compute_province_invariants(self, df: DataFrame) -> Dict[str, any]:
        """
        Compute TRUE province invariants from original raw data.
        
        CRITICAL: Must be called BEFORE bounded contribution or any modifications.
        These are the publicly published province totals that must be preserved exactly.
        
        Args:
            df: Raw transaction DataFrame (before any preprocessing)
            
        Returns:
            Dict with province_invariants DataFrame and summary stats
        """
        logger.info("Computing TRUE province invariants from original raw data...")
        
        # Group by province_code and compute totals from ORIGINAL data
        # Note: province_code is already added by the reader before preprocessing
        invariants_df = df.groupBy('province_code').agg(
            F.count('*').cast('long').alias('invariant_count'),
            F.sum('amount').cast('long').alias('invariant_amount')
        )
        
        # Also compute national total for logging
        totals = df.agg(
            F.count('*').cast('long').alias('total_count'),
            F.sum('amount').cast('long').alias('total_amount')
        ).first()
        
        total_count = totals['total_count'] if totals else 0
        total_amount = totals['total_amount'] if totals else 0
        
        logger.info(f"  True total count: {total_count:,}")
        logger.info(f"  True total amount: {total_amount:,}")
        
        return {
            'province_df': invariants_df,
            'total_count': total_count,
            'total_amount': total_amount
        }
    
    def _compute_per_mcc_caps(self, df: DataFrame) -> DataFrame:
        """
        Compute winsorization cap per individual MCC.
        
        Each MCC is processed separately for more granular outlier handling.
        This provides better utility than global winsorization across all MCCs.
        """
        logger.info("Computing per-MCC winsorization caps...")
        logger.info(f"Percentile: {self.config.privacy.mcc_cap_percentile}")
        
        # Compute cap per MCC using Spark
        mcc_caps = df.groupBy('mcc').agg(
            F.expr(f'percentile_approx(amount, {self.config.privacy.mcc_cap_percentile / 100.0})').alias('cap')
        ).collect()
        
        # Store in config
        self._mcc_caps = {row['mcc']: float(row['cap']) for row in mcc_caps}
        self.config.privacy.mcc_caps = self._mcc_caps
        
        logger.info(f"Computed caps for {len(self._mcc_caps)} individual MCCs")
        if self._mcc_caps:
            logger.info(f"Cap range: [{min(self._mcc_caps.values()):,.0f}, {max(self._mcc_caps.values()):,.0f}]")
        else:
            logger.warning("No MCC caps computed - empty or all-null MCC data")
        
        return df
    
    def _apply_bounded_contribution(self, df: DataFrame) -> DataFrame:
        """
        Apply bounded contribution - limit transactions per card-cell.
        
        Uses IQR method by default to compute K, then clips contributions.
        Also computes D_max (max cells per card) for user-level sensitivity.
        """
        from core.bounded_contribution import BoundedContributionCalculator
        from core.sensitivity import GlobalSensitivityCalculator
        
        logger.info("Applying bounded contribution...")
        
        calculator = BoundedContributionCalculator(
            method=self.config.privacy.contribution_bound_method,
            iqr_multiplier=self.config.privacy.contribution_bound_iqr_multiplier,
            fixed_k=self.config.privacy.contribution_bound_fixed,
            percentile=self.config.privacy.contribution_bound_percentile,
            compute_per_group=self.config.privacy.contribution_bound_per_group
        )
        
        # Need day_idx for cell definition, compute it temporarily
        date_stats = df.agg(F.min(F.col('transaction_date')).alias('min_date')).first()
        min_date = date_stats.min_date
        
        df_with_day = df.withColumn(
            'day_idx_temp', 
            F.datediff(F.col('transaction_date'), F.lit(min_date))
        )
        
        # Compute K using configured method
        k = calculator.compute_k_from_spark(
            df_with_day,
            card_col='card_number',
            city_col='acceptor_city',
            mcc_col='mcc',
            day_col='day_idx_temp'
        )
        
        # Store K in config for sensitivity calculation
        self.config.privacy.computed_contribution_bound = k
        
        # Clip contributions
        df_clipped, result = calculator.clip_contributions_spark(
            df_with_day,
            k=k,
            card_col='card_number',
            city_col='acceptor_city',
            mcc_col='mcc',
            day_col='day_idx_temp',
            order_col='transaction_date'
        )
        
        # === Compute D_max (max cells per card for bounded contribution analysis) ===
        logger.info("")
        logger.info("Computing D_max (max cells per card)...")
        sensitivity_calc = GlobalSensitivityCalculator(
            spark=self.spark,
            method="user_level"
        )
        d_max = sensitivity_calc.compute_max_cells_per_individual(
            df_clipped,
            individual_column='card_number',
            cell_columns=['acceptor_city', 'mcc', 'day_idx_temp']
        )
        
        # Store D_max in config for engine
        self._d_max = d_max
        self.config.privacy.computed_d_max = d_max
        
        logger.info(f"Bounded Contribution Parameters:")
        logger.info(f"  K (per-cell bound): {k}")
        logger.info(f"  D_max (max cells per card): {d_max}")
        logger.info(f"  sqrt(D_max): {d_max**0.5:.4f}")
        
        # Remove temporary day_idx column
        df_clipped = df_clipped.drop('day_idx_temp')
        
        logger.info(result.summary())
        
        return df_clipped
    
    def _compute_winsorize_cap(self, df: DataFrame) -> float:
        """
        Compute global winsorization cap from data.
        
        Uses configured percentile or fixed cap.
        Only used when MCC grouping is disabled.
        """
        if self.config.data.winsorize_cap is not None:
            cap = self.config.data.winsorize_cap
            logger.info(f"Using fixed winsorize cap: {cap:,.2f}")
            return cap
        
        percentile = self.config.data.winsorize_percentile / 100.0
        
        # Use approxQuantile for efficiency
        cap = df.stat.approxQuantile(
            'amount',
            [percentile],
            0.001  # relative error
        )[0]
        
        logger.info(f"Computed winsorize cap at {self.config.data.winsorize_percentile}%: {cap:,.2f}")
        return cap
    
    def _apply_winsorization(self, df: DataFrame) -> DataFrame:
        """
        Apply winsorization to transaction amounts.
        
        Uses per-MCC caps computed earlier.
        Each MCC is processed separately (parallel composition).
        """
        if self._mcc_caps is None or len(self._mcc_caps) == 0:
            logger.error("Per-MCC caps not computed, cannot apply winsorization")
            raise RuntimeError("Per-MCC caps must be computed before winsorization")
        
        return self._apply_per_mcc_winsorization(df)
    
    def _apply_global_winsorization(self, df: DataFrame) -> DataFrame:
        """Apply single global winsorization cap."""
        self._winsorize_cap = self._compute_winsorize_cap(df)
        
        # Cap amounts at the winsorization threshold (using Spark SQL, no UDF)
        df = df.withColumn(
            'amount_winsorized',
            F.when(F.col('amount') > self._winsorize_cap, self._winsorize_cap)
            .otherwise(F.col('amount'))
        )
        
        # Statistics after winsorization
        stats = df.agg(
            F.count(F.when(F.col('amount') > self._winsorize_cap, 1)).alias('capped_count'),
            F.sum(F.col('amount')).alias('original_sum'),
            F.sum(F.col('amount_winsorized')).alias('winsorized_sum')
        ).first()
        
        logger.info(f"Global Winsorization: {stats.capped_count:,} transactions capped")
        logger.info(f"Amount sum: {stats.original_sum:,.2f} -> {stats.winsorized_sum:,.2f}")
        
        return df
    
    def _apply_per_mcc_winsorization(self, df: DataFrame) -> DataFrame:
        """
        Apply winsorization with per-MCC caps.
        
        Each individual MCC has its own cap based on its transaction amount distribution.
        This provides better utility than grouping MCCs together.
        """
        logger.info("Applying per-MCC winsorization...")
        
        # Store max cap for reference
        self._winsorize_cap = max(self._mcc_caps.values()) if self._mcc_caps else 0.0
        
        # Create cap lookup DataFrame
        cap_data = [(mcc, float(cap)) for mcc, cap in self._mcc_caps.items()]
        cap_schema = StructType([
            StructField("cap_mcc", StringType(), False),
            StructField("mcc_cap", DoubleType(), False),
        ])
        cap_df = self.spark.createDataFrame(cap_data, schema=cap_schema)
        
        # Broadcast small cap lookup table (~100-500 MCCs) for efficient join
        df = df.join(F.broadcast(cap_df), df.mcc == cap_df.cap_mcc, "left").drop("cap_mcc")
        df = df.fillna({'mcc_cap': self._winsorize_cap})  # Unknown MCCs get max cap
        
        # Apply per-MCC winsorization
        df = df.withColumn(
            'amount_winsorized',
            F.when(F.col('amount') > F.col('mcc_cap'), F.col('mcc_cap'))
            .otherwise(F.col('amount'))
        )
        
        # Overall statistics
        total_stats = df.agg(
            F.count(F.when(F.col('amount') > F.col('mcc_cap'), 1)).alias('capped_count'),
            F.count('*').alias('total_count')
        ).first()
        
        pct_capped = 100 * total_stats.capped_count / total_stats.total_count if total_stats.total_count > 0 else 0
        logger.info(f"Per-MCC winsorization: {total_stats.capped_count:,} / {total_stats.total_count:,} transactions capped ({pct_capped:.2f}%)")
        if self._mcc_caps:
            logger.info(f"Cap range: [{min(self._mcc_caps.values()):,.0f}, {max(self._mcc_caps.values()):,.0f}]")
        
        return df
    
    def _create_indices(self, df: DataFrame) -> DataFrame:
        """Create numeric indices for dimensions using Spark SQL (no UDFs)."""
        # Get date range
        date_stats = df.agg(
            F.min(F.col('transaction_date')).alias('min_date'),
            F.max(F.col('transaction_date')).alias('max_date')
        ).first()
        
        self._min_date = date_stats.min_date
        max_date = date_stats.max_date
        
        # Handle both date objects and strings
        if isinstance(self._min_date, str) or isinstance(max_date, str):
            from datetime import datetime
            if isinstance(self._min_date, str):
                self._min_date = datetime.strptime(self._min_date, '%Y-%m-%d').date()
            if isinstance(max_date, str):
                max_date = datetime.strptime(max_date, '%Y-%m-%d').date()
        
        date_range = (max_date - self._min_date).days + 1
        logger.info(f"Date range: {self._min_date} to {max_date} ({date_range} days)")
        
        if date_range > self.config.data.num_days:
            logger.warning(f"Data spans {date_range} days, configured for {self.config.data.num_days}")
        
        # Create day index using datediff (no UDF!)
        min_date_lit = F.lit(self._min_date)
        df = df.withColumn('day_idx', F.datediff(F.col('transaction_date'), min_date_lit))
        
        # Create weekday column (0=Monday, 6=Sunday)
        # Spark's dayofweek returns 1=Sunday, 2=Monday, ..., 7=Saturday
        # We convert to 0=Monday, 1=Tuesday, ..., 6=Sunday
        # CRITICAL: Use F.pmod() not % operator - Spark's % follows Java semantics (can be negative)
        # For Sunday: (1-2) % 7 = -1 (wrong), but F.pmod(1-2, 7) = 6 (correct)
        df = df.withColumn('weekday', F.pmod(F.dayofweek(F.col('transaction_date')) - 2, 7))
        
        # Filter to configured number of days
        df = df.filter(F.col('day_idx') < self.config.data.num_days)
        df = df.filter(F.col('day_idx') >= 0)
        
        # Create MCC index mapping using join (no UDF!)
        unique_mccs = sorted([row.mcc for row in df.select('mcc').distinct().collect()])
        self._mcc_to_idx = {mcc: idx for idx, mcc in enumerate(unique_mccs)}
        
        mcc_mapping_data = [(mcc, idx) for mcc, idx in self._mcc_to_idx.items()]
        mcc_schema = StructType([
            StructField("mcc_key", StringType(), False),
            StructField("mcc_idx", IntegerType(), False),
        ])
        mcc_df = self.spark.createDataFrame(mcc_mapping_data, schema=mcc_schema)
        
        # Broadcast small MCC index lookup table (~300 MCCs) for efficient join
        df = df.join(F.broadcast(mcc_df), df.mcc == mcc_df.mcc_key, "left").drop("mcc_key")
        
        logger.info(f"Unique MCCs: {len(self._mcc_to_idx)}")
        
        # Create city index mapping using join (no UDF!)
        # Stream distinct city-province pairs (1500+ pairs) instead of collecting all at once
        city_data_df = df.select('province_code', 'acceptor_city').distinct()
        city_data = list(city_data_df.toLocalIterator())
        
        # Global city index
        unique_cities = sorted(set(row.acceptor_city for row in city_data))
        self._city_to_idx = {city: idx for idx, city in enumerate(unique_cities)}
        
        city_mapping_data = [(city, idx) for city, idx in self._city_to_idx.items()]
        city_schema = StructType([
            StructField("city_key", StringType(), False),
            StructField("city_idx", IntegerType(), False),
        ])
        city_df = self.spark.createDataFrame(city_mapping_data, schema=city_schema)
        
        # Broadcast small city index lookup table (~1500 cities) for efficient join
        df = df.join(F.broadcast(city_df), df.acceptor_city == city_df.city_key, "left").drop("city_key")
        
        logger.info(f"Unique cities: {len(self._city_to_idx)}")
        
        return df
    
    def _aggregate_to_histogram(self, df: DataFrame):
        """
        Aggregate data to SparkHistogram structure (100% Spark, ZERO collect).
        
        CRITICAL: Computes BOTH winsorized and original amounts:
        - total_amount (winsorized): For SDC processing (outliers handled)
        - total_amount_original: For computing invariants that match public data
        
        Returns SparkHistogram that keeps all data distributed (no driver memory).
        """
        from schema.histogram_spark import SparkHistogram, HistogramDimension
        
        # Compute aggregations (4 queries: transaction_count, unique_cards, total_amount, total_amount_original)
        # CRITICAL: Cast sums to LongType to avoid Java BigDecimal reflection warnings
        # Include weekday in groupBy for context-aware bounds
        agg_df = df.groupBy(
            'province_code', 'city_idx', 'mcc_idx', 'day_idx', 'weekday'
        ).agg(
            F.count('*').cast('long').alias('transaction_count'),
            F.countDistinct(F.col('card_number')).cast('long').alias('unique_cards'),
            F.sum(F.col('amount_winsorized')).cast('long').alias('total_amount'),         # Winsorized (outliers handled)
            F.sum(F.col('amount')).cast('long').alias('total_amount_original')             # Original (for invariants)
        )
        
        # Rename province_code to province_idx for consistency
        agg_df = agg_df.withColumnRenamed('province_code', 'province_idx')
        
        # Get count first (for logging) - use count() instead of collect()
        num_cells = agg_df.count()
        logger.info(f"Aggregated to {num_cells:,} non-zero cells (Spark DataFrame, NO collect)")
        
        # Create dimension metadata (small lists in driver, ~1 KB total)
        num_provinces = max(self.geography.province_codes) + 1
        num_cities = len(self._city_to_idx)
        num_mccs = len(self._mcc_to_idx)
        num_days = self.config.data.num_days
        
        # Create label lists (small metadata)
        province_labels = [''] * num_provinces
        for code in self.geography.province_codes:
            province = self.geography.get_province(code)
            if province:
                province_labels[code] = province.name
        
        city_labels = [''] * num_cities
        city_codes = [0] * num_cities
        for city, idx in self._city_to_idx.items():
            city_labels[idx] = city
            city_code = self.geography.get_city_code(city)
            city_codes[idx] = city_code if city_code is not None else Geography.UNKNOWN_CITY_CODE
        
        mcc_labels = [''] * num_mccs
        for mcc, idx in self._mcc_to_idx.items():
            mcc_labels[idx] = mcc
        
        # Build dimensions dictionary
        dimensions = {
            'province': HistogramDimension('province', num_provinces, province_labels),
            'city': HistogramDimension('city', num_cities, city_labels),
            'mcc': HistogramDimension('mcc', num_mccs, mcc_labels),
            'day': HistogramDimension('day', num_days, [str(i) for i in range(num_days)])
        }
        
        # Prepare province invariants DataFrame (convert province_code to province_idx)
        province_invariants_df = None
        if self._province_invariants_df is not None:
            # Convert province_code to province_idx (province_code IS the province_idx)
            # The invariants were computed with province_code, which is the same as province_idx
            province_invariants_df = self._province_invariants_df.withColumnRenamed(
                'province_code', 'province_idx'
            ).select(
                'province_idx',
                'invariant_count',
                'invariant_amount'
            )
            logger.info("  Passing TRUE province invariants to histogram (from original raw data)")
        else:
            logger.warning("  No province invariants computed - histogram will compute from aggregated data (may be incorrect!)")
        
        # Create SparkHistogram (data stays in Spark, NO collection to driver)
        logger.info("Creating SparkHistogram (100% Spark, data remains distributed)")
        histogram = SparkHistogram(
            spark=self.spark,
            df=agg_df,
            dimensions=dimensions,
            city_codes=city_codes,
            min_date=self._min_date,
            province_invariants=province_invariants_df
        )
        
        return histogram
    
    @property
    def winsorize_cap(self) -> float:
        """Get the computed winsorization cap."""
        return self._winsorize_cap
    
    @property
    def mcc_mapping(self) -> Dict[str, int]:
        """Get MCC to index mapping."""
        return self._mcc_to_idx.copy()
    
    @property
    def city_mapping(self) -> Dict[str, int]:
        """Get city to index mapping."""
        return self._city_to_idx.copy()
    
    @property
    def min_date(self) -> Optional[date]:
        """Get minimum date in data."""
        return self._min_date
    
    @property
    def mcc_caps(self):
        """Get per-MCC caps dictionary."""
        return self._mcc_caps
    
    @property
    def d_max(self) -> Optional[int]:
        """Get D_max (max cells per card) for bounded contribution analysis."""
        return self._d_max
