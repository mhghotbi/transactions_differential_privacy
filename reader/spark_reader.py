"""
Spark-based Transaction Data Reader.

Handles reading transaction data from various formats (Parquet, CSV)
and validates against the geographic hierarchy.
"""

import logging
from typing import Optional, Dict, Any

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, 
    DoubleType, DateType, IntegerType
)

from core.config import Config
from schema.geography import Geography


logger = logging.getLogger(__name__)


class SparkTransactionReader:
    """
    Reads transaction data using Spark.
    
    Expected source columns (user's data):
    - pspiin: PSP identifier (optional, not used in DP)
    - acceptorid: Acceptor/merchant identifier
    - card_number: Card identifier
    - transaction_date: Date of transaction
    - transaction_amount: Transaction amount
    - city: City of the acceptor
    - mcc: Merchant Category Code
    
    Columns are renamed to internal names via config.columns mapping.
    """
    
    # Default schema (internal names after column mapping)
    DEFAULT_SCHEMA = StructType([
        StructField("amount", DoubleType(), False),
        StructField("transaction_date", DateType(), False),
        StructField("card_number", StringType(), False),
        StructField("acceptor_id", StringType(), False),
        StructField("acceptor_city", StringType(), False),
        StructField("mcc", StringType(), False),
    ])
    
    def __init__(
        self,
        spark: SparkSession,
        config: Config,
        geography: Geography
    ):
        """
        Initialize reader.
        
        Args:
            spark: Active Spark session
            config: Configuration object
            geography: Geography instance for validation
        """
        self.spark = spark
        self.config = config
        self.geography = geography
        self.column_mapping = config.columns
    
    def read(self) -> DataFrame:
        """
        Read transaction data from configured path.
        
        Returns:
            DataFrame with transaction data
        """
        input_path = self.config.data.input_path
        input_format = self.config.data.input_format.lower()
        
        logger.info(f"Reading transactions from: {input_path} (format: {input_format})")
        
        if input_format == 'parquet':
            df = self._read_parquet(input_path)
        elif input_format == 'csv':
            df = self._read_csv(input_path)
        else:
            raise ValueError(f"Unsupported input format: {input_format}")
        
        # Rename columns to standard names
        df = self._rename_columns(df)
        
        # Validate and filter
        df = self._validate_and_filter(df)
        
        return df
    
    def _read_parquet(self, path: str) -> DataFrame:
        """Read from Parquet format."""
        return self.spark.read.parquet(path)
    
    def _read_csv(self, path: str) -> DataFrame:
        """Read from CSV format."""
        return (
            self.spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .option("encoding", "UTF-8")
            .csv(path)
        )
    
    def _rename_columns(self, df: DataFrame) -> DataFrame:
        """Rename columns to standard names based on config mapping."""
        for standard_name, source_name in self.column_mapping.items():
            if source_name in df.columns and source_name != standard_name:
                df = df.withColumnRenamed(source_name, standard_name)
        
        return df
    
    def _validate_and_filter(self, df: DataFrame) -> DataFrame:
        """
        Validate data and filter invalid records.
        
        - Checks for required columns
        - Filters null values
        - Converts transaction_date to DateType if needed
        - Validates cities against geography
        - Adds province information
        """
        # Check required columns (internal names after column mapping)
        required_columns = [
            'amount', 'transaction_date',
            'card_number', 'acceptor_id', 'acceptor_city', 'mcc'
        ]
        
        missing = set(required_columns) - set(df.columns)
        if missing:
            raise ValueError(f"Missing required columns: {missing}")
        
        initial_count = df.count()
        logger.info(f"Initial record count: {initial_count:,}")
        
        # Convert transaction_date to DateType if it's a string
        date_field = [f for f in df.schema.fields if f.name == 'transaction_date'][0]
        if isinstance(date_field.dataType, StringType):
            logger.info("Converting transaction_date from StringType to DateType")
            # Try to parse date (supports ISO format YYYY-MM-DD)
            df = df.withColumn(
                'transaction_date',
                F.to_date(F.col('transaction_date'), 'yyyy-MM-dd')
            )
            # Filter out any null dates (parsing failures)
            df = df.filter(F.col('transaction_date').isNotNull())
            logger.info("Date conversion complete")
        
        # Filter null values
        for col in required_columns:
            df = df.filter(F.col(col).isNotNull())
        
        after_null_filter = df.count()
        logger.info(f"After null filter: {after_null_filter:,} "
                   f"(dropped {initial_count - after_null_filter:,})")
        
        # Create city-province lookup DataFrame (no UDFs!)
        city_province_data = [
            (city, info[0], info[1], info[2])  # city_name, province_code, province_name, city_code
            for city, info in self.geography.city_to_province_broadcast().items()
        ]
        
        city_province_schema = StructType([
            StructField("city_name", StringType(), False),
            StructField("province_code", IntegerType(), False),
            StructField("province_name", StringType(), False),
            StructField("city_code", IntegerType(), False),
        ])
        
        city_province_df = self.spark.createDataFrame(city_province_data, schema=city_province_schema)
        
        # Join to add province info (instead of UDF)
        # Use LEFT join to keep unknown cities, assign them to "Unknown" province
        df = df.join(
            city_province_df,
            df.acceptor_city == city_province_df.city_name,
            "left"  # Keep all cities, even if not in city_province file
        ).drop("city_name")
        
        # Count unknown cities before filling
        unknown_count = df.filter(F.col('province_code').isNull()).count()
        
        # Assign unknown cities to "Unknown" province and unknown city code
        df = df.fillna({
            'province_code': Geography.UNKNOWN_PROVINCE_CODE,
            'province_name': Geography.UNKNOWN_PROVINCE_NAME,
            'city_code': Geography.UNKNOWN_CITY_CODE
        })
        
        after_city_join = df.count()
        logger.info(f"After city mapping: {after_city_join:,} records")
        if unknown_count > 0:
            logger.warning(f"Found {unknown_count:,} transactions with unknown cities - assigned to 'Unknown' province")
        
        # Filter positive amounts
        df = df.filter(F.col('amount') > 0)
        
        final_count = df.count()
        logger.info(f"Final record count: {final_count:,}")
        
        return df
    
    def get_unique_mccs(self, df: DataFrame) -> list:
        """Get list of unique MCC codes from data."""
        return [row.mcc for row in df.select('mcc').distinct().collect()]
    
    def get_date_range(self, df: DataFrame) -> tuple:
        """Get min and max dates from data."""
        stats = df.agg(
            F.min('transaction_date').alias('min_date'),
            F.max('transaction_date').alias('max_date')
        ).first()
        
        return stats.min_date, stats.max_date
    
    def get_statistics(self, df: DataFrame) -> Dict[str, Any]:
        """
        Compute basic statistics about the data.
        
        Returns:
            Dictionary with statistics
        """
        stats = df.agg(
            F.count('*').alias('total_transactions'),
            F.countDistinct('card_number').alias('unique_cards'),
            F.countDistinct('acceptor_city').alias('unique_cities'),
            F.countDistinct('mcc').alias('unique_mccs'),
            F.sum('amount').alias('total_amount'),
            F.avg('amount').alias('avg_amount'),
            F.min('amount').alias('min_amount'),
            F.max('amount').alias('max_amount'),
            F.min('transaction_date').alias('min_date'),
            F.max('transaction_date').alias('max_date')
        ).first()
        
        return {
            'total_transactions': stats.total_transactions,
            'unique_cards': stats.unique_cards,
            'unique_cities': stats.unique_cities,
            'unique_mccs': stats.unique_mccs,
            'total_amount': stats.total_amount,
            'avg_amount': stats.avg_amount,
            'min_amount': stats.min_amount,
            'max_amount': stats.max_amount,
            'min_date': str(stats.min_date),
            'max_date': str(stats.max_date)
        }
