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
    Preprocesses transaction data for DP processing.
    
    Steps:
    1. Compute winsorization cap from percentile
    2. Apply winsorization to amounts
    3. Calculate day indices
    4. Create dimension indices (city, mcc)
    5. Aggregate to histogram structure
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
        self._mcc_grouping = None  # MCCGroupingResult
        self._d_max: Optional[int] = None  # Max cells per card for user-level DP
    
    def process(self, df: DataFrame) -> TransactionHistogram:
        """
        Process transaction data into histogram.
        
        Args:
            df: Raw transaction DataFrame (with province columns added)
            
        Returns:
            TransactionHistogram with aggregated data
        """
        logger.info("Starting preprocessing...")
        
        # Step 0: Compute MCC groups FIRST (needed for per-group K computation and noise)
        if self.config.privacy.mcc_grouping_enabled:
            df = self._compute_mcc_groups(df)
        
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
    
    @staticmethod
    def _compute_mcc_groups_static(
        df: DataFrame,
        mcc_col: str = 'mcc',
        amount_col: str = 'amount',
        num_groups: int = 3,
        cap_percentile: float = 99.0,
        column_name: str = 'mcc_group_k'
    ) -> DataFrame:
        """
        Static method to compute MCC groups without needing instance state.
        
        This is used for K computation grouping (separate from main DP grouping).
        
        Args:
            df: Input DataFrame
            mcc_col: MCC column name
            amount_col: Amount column name
            num_groups: Number of groups to create
            cap_percentile: Percentile for winsorization cap
            column_name: Name for the output group column
            
        Returns:
            DataFrame with added group column
        """
        from core.mcc_groups import compute_mcc_groups_spark
        
        # Get Spark session from DataFrame
        spark = df.sql_ctx.sparkSession
        
        logger.info(f"Computing {num_groups}-group MCC stratification for {column_name}...")
        
        mcc_grouping = compute_mcc_groups_spark(
            df=df,
            mcc_col=mcc_col,
            amount_col=amount_col,
            num_groups=num_groups,
            cap_percentile=cap_percentile
        )
        
        # Add MCC group column to DataFrame
        mcc_group_data = [
            (mcc, group_id) 
            for mcc, group_id in mcc_grouping.mcc_to_group.items()
        ]
        mcc_group_schema = StructType([
            StructField("mcc_key_temp", StringType(), False),
            StructField(column_name, IntegerType(), False),
        ])
        mcc_group_df = spark.createDataFrame(mcc_group_data, schema=mcc_group_schema)
        
        # Broadcast small MCC group lookup table (~300 MCCs) for efficient join
        df = df.join(F.broadcast(mcc_group_df), df[mcc_col] == mcc_group_df.mcc_key_temp, "left").drop("mcc_key_temp")
        
        # Fill unknown MCCs with highest group (conservative)
        max_group = max(mcc_grouping.group_info.keys()) if mcc_grouping.group_info else 0
        df = df.fillna({column_name: max_group})
        
        logger.info(f"Created {mcc_grouping.num_groups} groups for {column_name}")
        
        return df
    
    def _compute_mcc_groups(self, df: DataFrame) -> DataFrame:
        """
        Compute MCC groups for stratified sensitivity (main DP grouping).
        
        Groups MCCs by order of magnitude of typical transaction amounts.
        Uses parallel composition - each group gets full privacy budget.
        """
        from core.mcc_groups import compute_mcc_groups_spark
        
        logger.info("Computing MCC groups for stratified sensitivity...")
        
        self._mcc_grouping = compute_mcc_groups_spark(
            df=df,
            mcc_col='mcc',
            amount_col='amount',
            num_groups=self.config.privacy.mcc_num_groups,
            cap_percentile=self.config.privacy.mcc_group_cap_percentile
        )
        
        # Store in config for use by TopDownEngine
        self.config.privacy.mcc_to_group = self._mcc_grouping.mcc_to_group
        self.config.privacy.mcc_group_caps = {
            gid: info.cap for gid, info in self._mcc_grouping.group_info.items()
        }
        
        logger.info(f"Created {self._mcc_grouping.num_groups} MCC groups")
        
        # Add MCC group column to DataFrame for per-group processing
        mcc_group_data = [
            (mcc, group_id) 
            for mcc, group_id in self._mcc_grouping.mcc_to_group.items()
        ]
        mcc_group_schema = StructType([
            StructField("mcc_grp_key", StringType(), False),
            StructField("mcc_group", IntegerType(), False),
        ])
        mcc_group_df = self.spark.createDataFrame(mcc_group_data, schema=mcc_group_schema)
        
        # Broadcast small MCC group lookup table (~300 MCCs) for efficient join
        df = df.join(F.broadcast(mcc_group_df), df.mcc == mcc_group_df.mcc_grp_key, "left").drop("mcc_grp_key")
        
        # Fill unknown MCCs with highest group (conservative - highest cap)
        max_group = max(self._mcc_grouping.group_info.keys()) if self._mcc_grouping.group_info else 0
        df = df.fillna({'mcc_group': max_group})
        
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
            compute_per_group=self.config.privacy.contribution_bound_per_group,
            num_groups_for_k=self.config.privacy.mcc_num_groups_for_k
        )
        
        # Need day_idx for cell definition, compute it temporarily
        date_stats = df.agg(F.min('transaction_date').alias('min_date')).first()
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
        
        # === USER-LEVEL DP: Compute D_max (max cells per card) ===
        logger.info("")
        logger.info("Computing D_max for user-level sensitivity...")
        sensitivity_calc = GlobalSensitivityCalculator(
            spark=self.spark,
            method="user_level"
        )
        d_max = sensitivity_calc.compute_max_cells_per_individual(
            df_clipped,
            individual_column='card_number',
            cell_columns=['acceptor_city', 'mcc', 'day_idx_temp']
        )
        
        # Store D_max in config for TopDownEngine
        self._d_max = d_max
        self.config.privacy.computed_d_max = d_max
        
        logger.info(f"User-Level DP Parameters:")
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
        
        If MCC grouping is enabled, uses per-group caps.
        Otherwise, uses a single global cap.
        """
        if self.config.privacy.mcc_grouping_enabled and self._mcc_grouping is not None:
            return self._apply_per_group_winsorization(df)
        else:
            return self._apply_global_winsorization(df)
    
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
            F.sum('amount').alias('original_sum'),
            F.sum('amount_winsorized').alias('winsorized_sum')
        ).first()
        
        logger.info(f"Global Winsorization: {stats.capped_count:,} transactions capped")
        logger.info(f"Amount sum: {stats.original_sum:,.2f} -> {stats.winsorized_sum:,.2f}")
        
        return df
    
    def _apply_per_group_winsorization(self, df: DataFrame) -> DataFrame:
        """
        Apply per-MCC-group winsorization.
        
        Each MCC group has its own cap based on typical transaction amounts.
        """
        logger.info("Applying per-MCC-group winsorization...")
        
        # Build case-when expression for per-group caps
        # Start with a base case (highest cap for unknown groups)
        max_cap = max(self.config.privacy.mcc_group_caps.values())
        self._winsorize_cap = max_cap  # Store max for reference
        
        # Create cap lookup DataFrame
        cap_data = [
            (group_id, float(cap)) 
            for group_id, cap in self.config.privacy.mcc_group_caps.items()
        ]
        cap_schema = StructType([
            StructField("cap_group_id", IntegerType(), False),
            StructField("group_cap", DoubleType(), False),
        ])
        cap_df = self.spark.createDataFrame(cap_data, schema=cap_schema)
        
        # Broadcast small cap lookup table (~20 MCC groups) for efficient join
        df = df.join(F.broadcast(cap_df), df.mcc_group == cap_df.cap_group_id, "left").drop("cap_group_id")
        df = df.fillna({'group_cap': max_cap})
        
        # Apply per-group winsorization
        df = df.withColumn(
            'amount_winsorized',
            F.when(F.col('amount') > F.col('group_cap'), F.col('group_cap'))
            .otherwise(F.col('amount'))
        )
        
        # Statistics per group
        group_stats = df.groupBy('mcc_group').agg(
            F.count(F.when(F.col('amount') > F.col('group_cap'), 1)).alias('capped_count'),
            F.count('*').alias('total_count'),
            F.first('group_cap').alias('cap')
        ).collect()
        
        logger.info("Per-group winsorization statistics:")
        for row in sorted(group_stats, key=lambda x: x.mcc_group):
            pct = 100 * row.capped_count / row.total_count if row.total_count > 0 else 0
            logger.info(f"  Group {row.mcc_group}: cap={row.cap:,.0f}, capped={row.capped_count:,} ({pct:.2f}%)")
        
        return df
    
    def _create_indices(self, df: DataFrame) -> DataFrame:
        """Create numeric indices for dimensions using Spark SQL (no UDFs)."""
        # Get date range
        date_stats = df.agg(
            F.min('transaction_date').alias('min_date'),
            F.max('transaction_date').alias('max_date')
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
        - total_amount (winsorized): For DP noise calibration
        - total_amount_original: For computing invariants that match public data
        
        Returns SparkHistogram that keeps all data distributed (no driver memory).
        """
        from schema.histogram_spark import SparkHistogram, HistogramDimension
        
        # Compute aggregations (4 queries: transaction_count, unique_cards, total_amount, total_amount_original)
        agg_df = df.groupBy(
            'province_code', 'city_idx', 'mcc_idx', 'day_idx'
        ).agg(
            F.count('*').alias('transaction_count'),
            F.countDistinct('card_number').alias('unique_cards'),
            F.sum('amount_winsorized').alias('total_amount'),         # Winsorized (for DP sensitivity)
            F.sum('amount').alias('total_amount_original')             # Original (for invariants)
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
        
        # Create SparkHistogram (data stays in Spark, NO collection to driver)
        logger.info("Creating SparkHistogram (100% Spark, data remains distributed)")
        histogram = SparkHistogram(
            spark=self.spark,
            df=agg_df,
            dimensions=dimensions,
            city_codes=city_codes
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
    def mcc_grouping(self):
        """Get MCC grouping result."""
        return self._mcc_grouping
    
    @property
    def d_max(self) -> Optional[int]:
        """Get D_max (max cells per card) for user-level DP sensitivity."""
        return self._d_max
