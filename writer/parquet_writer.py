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
        histogram: TransactionHistogram,
        output_path: Optional[str] = None
    ) -> str:
        """
        Write protected histogram to Parquet.
        
        Uses fast pandas-based conversion when available.
        
        Args:
            histogram: Protected TransactionHistogram
            output_path: Optional override for output path
            
        Returns:
            Path where data was written
        """
        output_path = output_path or self.config.data.output_path
        
        # Ensure output directory exists
        os.makedirs(output_path, exist_ok=True)
        
        logger.info(f"Writing protected data to: {output_path}")
        
        # Convert histogram to Spark DataFrame (always use Spark, no pandas)
        try:
            df = self._create_dataframe_fast(histogram)
            num_records = df.count()
        except Exception as e:
            logger.warning(f"Fast conversion failed, using fallback: {e}")
            # Fallback to record-based conversion
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
        
        logger.info(f"Writing {num_records:,} records to Parquet")
        
        # Write to Parquet
        parquet_path = os.path.join(output_path, "protected_data")
        
        writer = df.write.mode("overwrite")
        
        if self.partition_by:
            writer = writer.partitionBy(*self.partition_by)
        
        writer.parquet(parquet_path)
        
        logger.info(f"Data written to: {parquet_path}")
        
        # Write metadata
        self._write_metadata(output_path, histogram, num_records)
        
        return output_path
    
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
        histogram: TransactionHistogram,
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
        fieldnames = [
            'province_idx', 'city_code', 'mcc', 'day_idx',
            'transaction_count', 'unique_cards', 'transaction_amount_sum'
        ]
        
        with open(csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(records)
        
        logger.info(f"CSV written to: {csv_path} ({len(records):,} records)")
        
        return output_path

