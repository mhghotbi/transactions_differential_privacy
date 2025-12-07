"""
Spark-Native Histogram for Transaction Data.

This module implements a histogram structure that keeps ALL data in Spark DataFrames,
NEVER collecting to driver memory. Designed for datasets that exceed driver RAM (50-200+ GB).

CRITICAL RULES:
- NO numpy arrays for data storage
- NO .collect(), .toLocalIterator(), .toPandas() calls
- ALL operations use Spark transformations (lazy evaluation)
- Metadata only in driver memory (~1 KB vs 50-200 GB)
"""

import logging
from typing import Dict, List, Optional, Any
from dataclasses import dataclass

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, StringType

logger = logging.getLogger(__name__)


@dataclass
class HistogramDimension:
    """Dimension metadata (labels only, no data arrays)."""
    name: str
    size: int
    labels: Optional[List[str]] = None
    
    def get_label(self, index: int) -> str:
        """Get label for an index."""
        if self.labels and 0 <= index < len(self.labels):
            return self.labels[index]
        return str(index)


class SparkHistogram:
    """
    Spark-native histogram that keeps ALL data distributed.
    
    Core design:
    - Data stored as Spark DataFrame (distributed across executors)
    - Dimension metadata stored in driver (~1 KB)
    - NO data collection to driver (can handle datasets larger than driver RAM)
    
    Schema:
        - province_idx: int (0-31 for provinces)
        - city_idx: int (0-1500 for cities)
        - mcc_idx: int (0-300 for MCC codes)
        - day_idx: int (0-29 for days)
        - transaction_count: long
        - unique_cards: long
        - total_amount: long
        - total_amount_original: long (for invariants, dropped after DP)
    
    All operations return NEW SparkHistogram (immutable, functional style).
    """
    
    # Query names
    QUERIES = ['transaction_count', 'unique_cards', 'total_amount', 'total_amount_original']
    OUTPUT_QUERIES = ['transaction_count', 'unique_cards', 'total_amount']
    
    def __init__(
        self,
        spark: SparkSession,
        df: DataFrame,
        dimensions: Dict[str, HistogramDimension],
        city_codes: Optional[List[int]] = None
    ):
        """
        Initialize Spark-native histogram.
        
        Args:
            spark: Active Spark session
            df: DataFrame with histogram data (cells × queries)
            dimensions: Dimension metadata (province, city, mcc, day)
            city_codes: Optional mapping from city_idx to city_code
        """
        self._spark = spark
        self._df = df
        self.dimensions = dimensions
        self.city_codes = city_codes or []
        
        # Compute shape from dimensions (metadata only)
        self.shape = (
            dimensions['province'].size,
            dimensions['city'].size,
            dimensions['mcc'].size,
            dimensions['day'].size
        )
        
        logger.debug(f"SparkHistogram created with shape {self.shape}")
    
    @property
    def df(self) -> DataFrame:
        """Get underlying Spark DataFrame (NEVER collects to driver)."""
        return self._df
    
    def filter_mcc_group(self, mcc_indices: List[int]) -> 'SparkHistogram':
        """
        Filter to specific MCC indices.
        
        This is LAZY - no data movement until an action is triggered.
        Replaces numpy fancy indexing that creates full copies.
        
        Args:
            mcc_indices: List of MCC indices to keep
            
        Returns:
            New SparkHistogram with filtered data
        """
        filtered_df = self._df.filter(F.col('mcc_idx').isin(mcc_indices))
        return SparkHistogram(self._spark, filtered_df, self.dimensions, self.city_codes)
    
    def filter_province(self, province_idx: int) -> 'SparkHistogram':
        """Filter to specific province (LAZY)."""
        filtered_df = self._df.filter(F.col('province_idx') == province_idx)
        return SparkHistogram(self._spark, filtered_df, self.dimensions, self.city_codes)
    
    def with_column(self, col_name: str, col_expr) -> 'SparkHistogram':
        """Add or replace a column (LAZY)."""
        new_df = self._df.withColumn(col_name, col_expr)
        return SparkHistogram(self._spark, new_df, self.dimensions, self.city_codes)
    
    def select(self, *cols) -> DataFrame:
        """Select columns (returns DataFrame for final operations)."""
        return self._df.select(*cols)
    
    def aggregate_to_province(self, query: str) -> DataFrame:
        """
        Aggregate query to province level (sum over cities, mccs, days).
        
        Returns small DataFrame (~32 rows for 32 provinces) that can be cached.
        
        Args:
            query: Query name to aggregate
            
        Returns:
            DataFrame with columns: province_idx, <query>_sum
        """
        return self._df.groupBy('province_idx').agg(
            F.sum(F.col(query)).alias(f'{query}_sum')
        )
    
    def drop_column(self, col_name: str) -> 'SparkHistogram':
        """Drop a column (e.g., total_amount_original after DP)."""
        new_df = self._df.drop(col_name)
        return SparkHistogram(self._spark, new_df, self.dimensions, self.city_codes)
    
    def count_cells(self) -> int:
        """
        Count non-zero cells.
        
        WARNING: This triggers a Spark action (.count()) but does NOT collect data.
        Only returns a single integer.
        """
        return self._df.count()
    
    def write_parquet(self, path: str, mode: str = 'overwrite') -> None:
        """
        Write histogram to Parquet (distributed write, no driver memory).
        
        Args:
            path: Output path
            mode: Write mode ('overwrite', 'append', etc.)
        """
        self._df.write.mode(mode).parquet(path)
        logger.info(f"Histogram written to {path} (distributed, no driver collection)")
    
    def summary_stats(self) -> Dict[str, Any]:
        """
        Compute summary statistics WITHOUT collecting full data.
        
        Uses Spark aggregations to compute totals and return only small results.
        
        Returns:
            Dict with summary statistics
        """
        # Aggregate all queries in one pass (efficient)
        agg_exprs = []
        for query in self.QUERIES:
            if query in self._df.columns:
                agg_exprs.append(F.sum(F.col(query)).alias(f'{query}_total'))
        
        # Count is separate (distinct from sum)
        result = self._df.agg(
            F.count('*').alias('num_cells'),
            *agg_exprs
        ).first()  # Only ONE row - safe to collect
        
        stats = {
            'shape': self.shape,
            'num_cells': result['num_cells'] if result else 0,
        }
        
        for query in self.QUERIES:
            if query in self._df.columns:
                stats[f'{query}_total'] = result[f'{query}_total'] if result else 0
        
        return stats
    
    def summary(self) -> str:
        """
        Generate a summary string of the histogram.
        
        Compatible with TransactionHistogram.summary() for drop-in replacement.
        Uses Spark aggregations - only collects summary statistics (~1 row).
        
        Returns:
            Formatted summary string
        """
        stats = self.summary_stats()
        
        lines = [
            "=" * 60,
            "Spark Transaction Histogram Summary (100% Spark)",
            "=" * 60,
            f"Shape: {self.shape}",
            f"  Provinces: {self.shape[0]}",
            f"  Cities: {self.shape[1]}",
            f"  MCCs: {self.shape[2]}",
            f"  Days: {self.shape[3]}",
            f"Non-Zero Cells: {stats['num_cells']:,}",
            "",
            "Query Totals:"
        ]
        
        # Show all queries that exist in data
        for query in self.QUERIES:
            total_key = f'{query}_total'
            if total_key in stats:
                total = stats[total_key]
                # Add annotation for temporary fields
                if query == 'total_amount_original':
                    lines.append(f"  {query}: {total:,} (original unwinsorized, for invariants)")
                elif query == 'total_amount' and f'total_amount_original_total' in stats:
                    lines.append(f"  {query}: {total:,} (winsorized, for DP noise)")
                else:
                    lines.append(f"  {query}: {total:,}")
        
        lines.append("=" * 60)
        return "\n".join(lines)
    
    def show_sample(self, n: int = 20) -> None:
        """Show sample rows (for debugging only, collects small sample)."""
        logger.info(f"Sample of {n} rows from histogram:")
        self._df.show(n, truncate=False)
    
    @classmethod
    def from_aggregated_df(
        cls,
        spark: SparkSession,
        df: DataFrame,
        province_labels: Optional[List[str]] = None,
        city_labels: Optional[List[str]] = None,
        mcc_labels: Optional[List[str]] = None,
        city_codes: Optional[List[int]] = None
    ) -> 'SparkHistogram':
        """
        Create SparkHistogram from aggregated DataFrame.
        
        This is the primary constructor used by the preprocessor.
        
        Args:
            spark: Active Spark session
            df: Aggregated DataFrame with required columns
            province_labels: Province names
            city_labels: City names
            mcc_labels: MCC code labels
            city_codes: City code mapping
            
        Returns:
            SparkHistogram instance
        """
        # Infer dimensions from data (requires small aggregation, not full collect)
        # Use approxCountDistinct for large datasets (faster than countDistinct)
        dims_result = df.agg(
            F.countDistinct(F.col('province_idx')).alias('n_prov'),
            F.countDistinct(F.col('city_idx')).alias('n_city'),
            F.countDistinct(F.col('mcc_idx')).alias('n_mcc'),
            F.countDistinct(F.col('day_idx')).alias('n_day')
        ).first()
        
        n_prov = dims_result['n_prov'] if dims_result else 32
        n_city = dims_result['n_city'] if dims_result else 1500
        n_mcc = dims_result['n_mcc'] if dims_result else 300
        n_day = dims_result['n_day'] if dims_result else 30
        
        # Build dimension metadata (small lists in driver memory)
        dimensions = {
            'province': HistogramDimension('province', n_prov, province_labels),
            'city': HistogramDimension('city', n_city, city_labels),
            'mcc': HistogramDimension('mcc', n_mcc, mcc_labels),
            'day': HistogramDimension('day', n_day, [str(i) for i in range(n_day)])
        }
        
        return cls(spark, df, dimensions, city_codes)


def create_empty_histogram(
    spark: SparkSession,
    province_dim: int,
    city_dim: int,
    mcc_dim: int,
    day_dim: int = 30
) -> SparkHistogram:
    """
    Create an empty SparkHistogram with specified dimensions.
    
    Useful for testing or initialization.
    """
    schema = StructType([
        StructField('province_idx', IntegerType(), False),
        StructField('city_idx', IntegerType(), False),
        StructField('mcc_idx', IntegerType(), False),
        StructField('day_idx', IntegerType(), False),
        StructField('transaction_count', LongType(), False),
        StructField('unique_cards', LongType(), False),
        StructField('total_amount', LongType(), False),
        StructField('total_amount_original', LongType(), False),
    ])
    
    empty_df = spark.createDataFrame([], schema)
    
    dimensions = {
        'province': HistogramDimension('province', province_dim),
        'city': HistogramDimension('city', city_dim),
        'mcc': HistogramDimension('mcc', mcc_dim),
        'day': HistogramDimension('day', day_dim, [str(i) for i in range(day_dim)])
    }
    
    return SparkHistogram(spark, empty_df, dimensions)

