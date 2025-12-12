"""
Parquet Writer for DP-Protected Data.

Writes the protected histogram data to Parquet format.
"""

import logging
import os
from typing import Dict, List, Any, Optional
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType,
    IntegerType, DoubleType
)

from core.config import Config
from schema.histogram import TransactionHistogram


logger = logging.getLogger(__name__)


class ParquetWriter:
    """
    Writes protected transaction data to Parquet format.
    
    Output structure:
    - One Parquet file with all protected data
    - Columns: province_code, city_code, mcc, day, transaction_count, unique_cards, 
               transaction_amount_sum
    - Partitioned by province_code and city_code for efficient queries
    """
    
    # Output schema
    OUTPUT_SCHEMA = StructType([
        StructField("province_code", IntegerType(), False),
        StructField("city_code", IntegerType(), False),
        StructField("mcc", StringType(), True),
        StructField("day_idx", IntegerType(), False),
        StructField("transaction_date", StringType(), True),
        StructField("transaction_count", LongType(), False),
        StructField("unique_cards", LongType(), False),
        StructField("transaction_amount_sum", LongType(), False),
    ])
    
    def __init__(
        self,
        spark: SparkSession,
        config: Config,
        partition_by: Optional[List[str]] = None
    ):
        """
        Initialize writer.
        
        Args:
            spark: Active Spark session
            config: Configuration object
            partition_by: Optional list of columns to partition by
        """
        self.spark = spark
        self.config = config
        self.partition_by = partition_by or ['province_code', 'city_code']
    
    def write(
        self,
        histogram,  # Union[TransactionHistogram, SparkHistogram]
        output_path: Optional[str] = None
    ) -> str:
        """
        Write protected histogram to Parquet.
        
        Handles both TransactionHistogram (old) and SparkHistogram (new).
        For SparkHistogram, writes directly without collecting to driver (100% Spark).
        
        Args:
            histogram: Protected histogram (TransactionHistogram or SparkHistogram)
            output_path: Optional override for output path
            
        Returns:
            Path where data was written
        """
        from schema.histogram_spark import SparkHistogram
        
        output_path = output_path or self.config.data.output_path
        
        # Ensure output directory exists
        os.makedirs(output_path, exist_ok=True)
        
        logger.info(f"Writing protected data to: {output_path}")
        
        # Check if this is a SparkHistogram (new 100% Spark path)
        if isinstance(histogram, SparkHistogram):
            logger.info("Writing SparkHistogram (100% Spark, ZERO collect)")
            df = self._create_dataframe_from_spark_histogram(histogram)
            # Count triggers action but doesn't collect data
            num_records = df.count()
        else:
            # Old TransactionHistogram path (collects to driver)
            logger.info("Writing TransactionHistogram (legacy numpy path)")
            try:
                df = self._create_dataframe_fast(histogram)
                num_records = df.count()
            except Exception as e:
                logger.warning(f"Fast conversion failed, using fallback: {e}")
                records = histogram.to_records()
                logger.info(f"Converting {len(records):,} records to DataFrame")
                
                if not records:
                    logger.warning("No records to write!")
                    return output_path
                
                df = self._create_dataframe(records)
                num_records = len(records)
        
        if num_records == 0:
            logger.warning("No records to write!")
            return output_path
        
        logger.info(f"Writing {num_records:,} records to Parquet (distributed write)")
        
        # Write to Parquet (distributed, no driver collection)
        parquet_path = os.path.join(output_path, "protected_data")
        
        writer = df.write.mode("overwrite")
        
        if self.partition_by:
            writer = writer.partitionBy(*self.partition_by)
        
        writer.parquet(parquet_path)
        
        logger.info(f"Data written to: {parquet_path}")
        
        # Write metadata
        self._write_metadata(output_path, histogram, num_records)
        
        return output_path
    
    def write_comparison(
        self,
        original_histogram,  # Union[TransactionHistogram, SparkHistogram]
        protected_histogram,  # Union[TransactionHistogram, SparkHistogram]
        output_path: Optional[str] = None
    ) -> str:
        """
        Write both original and DP-protected data with suffixes for comparison.
        
        Output columns:
        - Dimension columns: province_code, city_code, mcc, day_idx
        - Real data: transaction_count_real, unique_cards_real, transaction_amount_sum_real
        - DP-protected: transaction_count_dp, unique_cards_dp, transaction_amount_sum_dp
        
        Args:
            original_histogram: Original histogram before DP
            protected_histogram: Protected histogram after DP
            output_path: Optional override for output path
            
        Returns:
            Path where data was written
        """
        from schema.histogram_spark import SparkHistogram
        
        output_path = output_path or self.config.data.output_path
        comparison_path = os.path.join(output_path, "comparison_data")
        os.makedirs(comparison_path, exist_ok=True)
        
        logger.info(f"Writing comparison data (original + protected) to: {comparison_path}")
        
        # Convert both histograms to DataFrames
        if isinstance(original_histogram, SparkHistogram):
            logger.info("Converting SparkHistogram original data...")
            df_original = self._create_dataframe_from_spark_histogram(original_histogram)
        else:
            logger.info("Converting TransactionHistogram original data...")
            df_original = self._create_dataframe_fast(original_histogram)
        
        if isinstance(protected_histogram, SparkHistogram):
            logger.info("Converting SparkHistogram protected data...")
            df_protected = self._create_dataframe_from_spark_histogram(protected_histogram)
        else:
            logger.info("Converting TransactionHistogram protected data...")
            df_protected = self._create_dataframe_fast(protected_histogram)
        
        # Rename columns with suffixes
        logger.info("Renaming columns with _real and _dp suffixes...")
        
        # Original data: rename value columns to _real, drop transaction_date to avoid ambiguity
        for col in ['transaction_count', 'unique_cards', 'transaction_amount_sum']:
            if col in df_original.columns:
                df_original = df_original.withColumnRenamed(col, f'{col}_real')
        
        # Keep transaction_date from original for later
        transaction_date_col = None
        if 'transaction_date' in df_original.columns:
            transaction_date_col = 'transaction_date'
            # Temporarily store it as a different name in original
            df_original = df_original.withColumnRenamed('transaction_date', 'transaction_date_temp')
        
        # Protected data: rename value columns to _dp, drop transaction_date to avoid ambiguity
        for col in ['transaction_count', 'unique_cards', 'transaction_amount_sum']:
            if col in df_protected.columns:
                df_protected = df_protected.withColumnRenamed(col, f'{col}_dp')
        
        # Drop transaction_date from protected (we'll use original's)
        if 'transaction_date' in df_protected.columns:
            df_protected = df_protected.drop('transaction_date')
        
        # Join on dimension columns
        logger.info("Joining original and protected data...")
        join_cols = ['province_code', 'city_code', 'mcc', 'day_idx']
        
        df_comparison = df_original.join(
            df_protected,
            on=join_cols,
            how='outer'  # outer join to catch any missing cells
        )
        
        # Restore transaction_date column
        if transaction_date_col:
            df_comparison = df_comparison.withColumnRenamed('transaction_date_temp', 'transaction_date')
        else:
            df_comparison = df_comparison.withColumn('transaction_date', F.col('day_idx').cast('string'))
        
        # Reorder columns for clarity
        ordered_cols = join_cols + ['transaction_date']
        ordered_cols += [f'{col}_real' for col in ['transaction_count', 'unique_cards', 'transaction_amount_sum']]
        ordered_cols += [f'{col}_dp' for col in ['transaction_count', 'unique_cards', 'transaction_amount_sum']]
        
        # Select only existing columns
        existing_cols = [col for col in ordered_cols if col in df_comparison.columns]
        df_comparison = df_comparison.select(existing_cols)
        
        # Write to Parquet
        logger.info(f"Writing comparison data partitioned by {self.partition_by}...")
        df_comparison.write.mode("overwrite").partitionBy(*self.partition_by).parquet(comparison_path)
        
        num_records = df_comparison.count()
        logger.info(f"Comparison data written successfully: {num_records:,} records")
        logger.info(f"  Columns: {', '.join(existing_cols)}")
        
        return comparison_path
    
    def _create_dataframe_from_spark_histogram(self, histogram) -> DataFrame:
        """
        Convert SparkHistogram to output DataFrame (100% Spark, NO collect).
        
        Args:
            histogram: SparkHistogram with DP-protected data
            
        Returns:
            Spark DataFrame ready for writing (data stays distributed)
        """
        from pyspark.sql import functions as F
        
        # SparkHistogram already contains a Spark DataFrame
        # Just select and rename columns for output schema
        df = histogram.df.select(
            F.col('province_idx').cast('int').alias('province_code'),
            # Get city_code from histogram metadata
            F.col('city_idx').cast('int'),  # We'll join with city codes
            F.col('mcc_idx').cast('int'),
            F.col('day_idx').cast('int'),
            F.col('transaction_count').cast('long'),
            F.col('unique_cards').cast('long'),
            F.col('total_amount').cast('long').alias('transaction_amount_sum')
        )
        
        # Add city_code column (need to join with city code mapping)
        # Create small city code DataFrame for broadcast join
        if histogram.city_codes:
            city_code_data = [
                {'city_idx': idx, 'city_code': code}
                for idx, code in enumerate(histogram.city_codes)
            ]
            city_code_df = self.spark.createDataFrame(city_code_data)
            df = df.join(F.broadcast(city_code_df), on='city_idx', how='left')
        else:
            # Fallback: use city_idx as city_code
            df = df.withColumn('city_code', F.col('city_idx'))
        
        # Add MCC label (get from histogram dimensions)
        if 'mcc' in histogram.dimensions and histogram.dimensions['mcc'].labels:
            mcc_labels = histogram.dimensions['mcc'].labels
            mcc_data = [
                {'mcc_idx': idx, 'mcc': label}
                for idx, label in enumerate(mcc_labels)
            ]
            mcc_df = self.spark.createDataFrame(mcc_data)
            df = df.join(F.broadcast(mcc_df), on='mcc_idx', how='left')
        else:
            df = df.withColumn('mcc', F.col('mcc_idx').cast('string'))
        
        # Add transaction_date (format day_idx as string for now)
        df = df.withColumn('transaction_date', F.col('day_idx').cast('string'))
        
        # Select final columns in output schema order
        df = df.select(
            'province_code',
            'city_code',
            'mcc',
            'day_idx',
            'transaction_date',
            'transaction_count',
            'unique_cards',
            'transaction_amount_sum'
        )
        
        logger.info("SparkHistogram converted to output DataFrame (100% Spark, NO collect)")
        return df
    
    def _create_dataframe_fast(self, histogram: TransactionHistogram) -> DataFrame:
        """
        Convert histogram to Spark DataFrame directly from records.
        
        Uses Spark RDD for efficient distributed conversion without pandas.
        """
        # Get records from histogram (this is memory-efficient)
        records = histogram.to_records()
        
        if not records:
            return self.spark.createDataFrame([], schema=self.OUTPUT_SCHEMA)
        
        # Convert to Spark DataFrame directly
        # Map records to output schema format
        mapped_records = [
            {
                'province_code': r['province_idx'],
                'city_code': r['city_code'],
                'mcc': r['mcc'],
                'day_idx': r['day_idx'],
                'transaction_date': r.get('day', str(r['day_idx'])),
                'transaction_count': int(r['transaction_count']),
                'unique_cards': int(r['unique_cards']),
                'transaction_amount_sum': int(r['total_amount']),
            }
            for r in records
        ]
        
        # Create Spark DataFrame from records
        # For large datasets, create RDD first then convert to DataFrame
        if len(mapped_records) > 100000:
            # Use RDD for better memory management
            rdd = self.spark.sparkContext.parallelize(mapped_records)
            return self.spark.createDataFrame(rdd, schema=self.OUTPUT_SCHEMA)
        else:
            # Direct conversion for smaller datasets
            return self.spark.createDataFrame(mapped_records, schema=self.OUTPUT_SCHEMA)
    
    def _create_dataframe(self, records: List[Dict[str, Any]]) -> DataFrame:
        """Convert records to Spark DataFrame efficiently."""
        # Direct mapping - avoid creating intermediate list
        # Spark can handle the dict format directly
        mapped_records = [
            {
                'province_code': r['province_idx'],
                'city_code': r['city_code'],
                'mcc': r['mcc'],
                'day_idx': r['day_idx'],
                'transaction_date': r.get('day', str(r['day_idx'])),
                'transaction_count': int(r['transaction_count']),
                'unique_cards': int(r['unique_cards']),
                'transaction_amount_sum': int(r['total_amount']),
            }
            for r in records
        ]
        
        return self.spark.createDataFrame(mapped_records, schema=self.OUTPUT_SCHEMA)
    
    def _write_metadata(
        self,
        output_path: str,
        histogram,  # Union[TransactionHistogram, SparkHistogram]
        num_records: int
    ) -> None:
        """Write metadata file with processing information."""
        import json
        
        metadata = {
            "created_at": datetime.now().isoformat(),
            "privacy_budget": {
                "total_rho": str(self.config.privacy.total_rho),
                "delta": self.config.privacy.delta,
                "geographic_split": self.config.privacy.geographic_split,
                "query_split": self.config.privacy.query_split
            },
            "dimensions": {
                "num_provinces": histogram.shape[0],
                "num_cities": histogram.shape[1],
                "num_mccs": histogram.shape[2],
                "num_days": histogram.shape[3]
            },
            "statistics": {
                "total_cells": histogram.total_cells,
                "non_zero_cells": histogram.non_zero_cells,
                "output_records": num_records
            },
            "queries": TransactionHistogram.QUERIES
        }
        
        metadata_path = os.path.join(output_path, "metadata.json")
        with open(metadata_path, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Metadata written to: {metadata_path}")


class CSVWriter:
    """
    Alternative writer that outputs to CSV format.
    
    Useful for debugging and smaller datasets.
    """
    
    def __init__(self, config: Config):
        """Initialize CSV writer."""
        self.config = config
    
    def write(
        self,
        histogram: TransactionHistogram,
        output_path: Optional[str] = None
    ) -> str:
        """Write histogram to CSV."""
        import csv
        
        output_path = output_path or self.config.data.output_path
        os.makedirs(output_path, exist_ok=True)
        
        csv_path = os.path.join(output_path, "protected_data.csv")
        
        records = histogram.to_records()
        
        if not records:
            logger.warning("No records to write!")
            return output_path
        
        # Write CSV
        # Map internal field names to output column names
        mapped_records = [
            {
                'province_idx': r['province_idx'],
                'city_code': r['city_code'],
                'mcc': r['mcc'],
                'day_idx': r['day_idx'],
                'transaction_count': r['transaction_count'],
                'unique_cards': r['unique_cards'],
                'transaction_amount_sum': r['total_amount']  # Map total_amount to transaction_amount_sum
            }
            for r in records
        ]
        
        fieldnames = [
            'province_idx', 'city_code', 'mcc', 'day_idx',
            'transaction_count', 'unique_cards', 'transaction_amount_sum'
        ]
        
        with open(csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(mapped_records)
        
        logger.info(f"CSV written to: {csv_path} ({len(records):,} records)")
        
        return output_path

