"""
Memory monitoring for Spark-based DP pipeline.

Tracks memory usage without collecting data to driver.
Uses Spark metrics and sampling for distributed memory estimation.
"""

import logging
from typing import Dict, Optional, Any
from pyspark.sql import SparkSession, DataFrame
import psutil

logger = logging.getLogger(__name__)


class SparkMemoryMonitor:
    """
    Monitor memory usage in Spark-based DP pipeline.
    
    Tracks both driver and executor memory without collecting full datasets.
    Uses Spark metrics API and sampling for estimation.
    """
    
    def __init__(self, spark: SparkSession):
        """
        Initialize memory monitor.
        
        Args:
            spark: Active Spark session
        """
        self.spark = spark
        self._checkpoints: Dict[str, Dict[str, Any]] = {}
    
    def log_driver_memory(self, label: str) -> Dict[str, float]:
        """
        Log current driver memory usage.
        
        Args:
            label: Description of current checkpoint
            
        Returns:
            Dict with memory statistics (GB and %)
        """
        vm = psutil.virtual_memory()
        mem_used_gb = vm.used / (1024**3)
        mem_total_gb = vm.total / (1024**3)
        mem_percent = vm.percent
        mem_available_gb = vm.available / (1024**3)
        
        stats = {
            'label': label,
            'used_gb': mem_used_gb,
            'total_gb': mem_total_gb,
            'percent': mem_percent,
            'available_gb': mem_available_gb
        }
        
        logger.info(
            f"[DRIVER MEMORY] {label}: "
            f"{mem_used_gb:.2f}/{mem_total_gb:.2f} GB ({mem_percent:.1f}%), "
            f"available={mem_available_gb:.2f} GB"
        )
        
        self._checkpoints[label] = stats
        return stats
    
    def estimate_dataframe_size(
        self,
        df: DataFrame,
        label: str,
        sample_rows: int = 1000
    ) -> Dict[str, Any]:
        """
        Estimate DataFrame size without collecting all data.
        
        Uses sampling to estimate total size.
        WARNING: This triggers a Spark action (.count()) but doesn't collect data.
        
        Args:
            df: DataFrame to estimate
            label: Description of DataFrame
            sample_rows: Number of rows to sample for estimation
            
        Returns:
            Dict with size estimates
        """
        try:
            # Count total rows (Spark action, but no data collection)
            total_rows = df.count()
            
            # Get number of partitions (no action)
            num_partitions = df.rdd.getNumPartitions()
            
            # Get column count (no action)
            num_columns = len(df.columns)
            
            # Estimate size per row (sample small subset)
            # Use LIMIT instead of sample for deterministic results
            if total_rows > sample_rows:
                # Sample rows WITHOUT collect (still estimates)
                sample_df = df.limit(sample_rows)
                sample_count = sample_df.count()
                
                # Rough estimate: assume ~8 bytes per numeric column
                # This is VERY rough - actual size depends on data types
                estimated_bytes_per_row = num_columns * 8
                estimated_total_bytes = total_rows * estimated_bytes_per_row
                estimated_gb = estimated_total_bytes / (1024**3)
            else:
                # Small DataFrame
                estimated_bytes_per_row = num_columns * 8
                estimated_total_bytes = total_rows * estimated_bytes_per_row
                estimated_gb = estimated_total_bytes / (1024**3)
            
            stats = {
                'label': label,
                'total_rows': total_rows,
                'num_columns': num_columns,
                'num_partitions': num_partitions,
                'estimated_gb': estimated_gb,
                'estimated_bytes_per_row': estimated_bytes_per_row
            }
            
            logger.info(
                f"[DF SIZE] {label}: "
                f"{total_rows:,} rows × {num_columns} cols = "
                f"~{estimated_gb:.2f} GB ({num_partitions} partitions)"
            )
            
            return stats
            
        except Exception as e:
            logger.warning(f"Could not estimate DataFrame size for {label}: {e}")
            return {'label': label, 'error': str(e)}
    
    def log_spark_metrics(self, label: str) -> Dict[str, Any]:
        """
        Log Spark executor metrics if available.
        
        Args:
            label: Description of current checkpoint
            
        Returns:
            Dict with Spark metrics (or empty if not available)
        """
        try:
            sc = self.spark.sparkContext
            
            # Get basic Spark context info
            app_id = sc.applicationId
            app_name = sc.appName
            master = sc.master
            
            # Get executor info
            executor_count = len(sc._jsc.sc().statusTracker().getExecutorInfos())
            
            # Get memory config
            executor_memory = sc.getConf().get('spark.executor.memory', 'unknown')
            driver_memory = sc.getConf().get('spark.driver.memory', 'unknown')
            
            stats = {
                'label': label,
                'app_id': app_id,
                'app_name': app_name,
                'master': master,
                'executor_count': executor_count,
                'executor_memory': executor_memory,
                'driver_memory': driver_memory
            }
            
            logger.info(
                f"[SPARK METRICS] {label}: "
                f"executors={executor_count}, "
                f"executor_mem={executor_memory}, "
                f"driver_mem={driver_memory}"
            )
            
            return stats
            
        except Exception as e:
            logger.debug(f"Could not get Spark metrics: {e}")
            return {'label': label, 'error': str(e)}
    
    def checkpoint(
        self,
        label: str,
        df: Optional[DataFrame] = None
    ) -> Dict[str, Any]:
        """
        Create a memory checkpoint with driver + optional DataFrame stats.
        
        Args:
            label: Description of checkpoint
            df: Optional DataFrame to estimate size
            
        Returns:
            Dict with all statistics
        """
        stats = {
            'driver': self.log_driver_memory(label),
            'spark': self.log_spark_metrics(label)
        }
        
        if df is not None:
            stats['dataframe'] = self.estimate_dataframe_size(df, label)
        
        return stats
    
    def summary(self) -> str:
        """
        Generate summary of all checkpoints.
        
        Returns:
            Formatted summary string
        """
        if not self._checkpoints:
            return "No memory checkpoints recorded"
        
        lines = [
            "=" * 60,
            "Memory Monitoring Summary",
            "=" * 60,
            ""
        ]
        
        for label, stats in self._checkpoints.items():
            lines.append(f"{label}:")
            lines.append(f"  Used: {stats['used_gb']:.2f} GB ({stats['percent']:.1f}%)")
            lines.append(f"  Available: {stats['available_gb']:.2f} GB")
            lines.append("")
        
        lines.append("=" * 60)
        return "\n".join(lines)


def create_monitor(spark: SparkSession) -> SparkMemoryMonitor:
    """Create a memory monitor instance."""
    return SparkMemoryMonitor(spark)

