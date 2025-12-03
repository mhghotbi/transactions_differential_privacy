"""
Suppression Manager for Differential Privacy.

Implements cell suppression rules similar to Census 2020 DAS.
Cells with noisy counts below a threshold are suppressed to prevent
disclosure of small populations.

This is a complementary protection to DP noise, not a replacement.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from typing import List, Optional
import logging

logger = logging.getLogger(__name__)


class SuppressionManager:
    """
    Manages cell suppression based on noisy counts.
    
    Census 2020 Context:
    - Census suppresses cells with very small populations
    - This prevents disclosure even after DP noise
    - Suppression is applied AFTER noise addition (on noisy values)
    
    Suppression Methods:
    1. "flag": Add a suppression flag column
    2. "null": Set suppressed values to null
    3. "value": Set suppressed values to a sentinel value (e.g., -1)
    """
    
    SUPPRESSION_METHODS = ["flag", "null", "value"]
    
    def __init__(
        self,
        threshold: int = 10,
        method: str = "flag",
        sentinel_value: int = -1,
        primary_count_column: str = "transaction_count_protected"
    ):
        """
        Initialize the SuppressionManager.
        
        Args:
            threshold: Minimum noisy count to release. Cells below this are suppressed.
            method: How to handle suppression ("flag", "null", "value").
            sentinel_value: Value to use for suppressed cells if method="value".
            primary_count_column: Column name to check for suppression threshold.
        """
        if threshold < 0:
            raise ValueError("Suppression threshold must be non-negative")
        if method not in self.SUPPRESSION_METHODS:
            raise ValueError(f"Method must be one of {self.SUPPRESSION_METHODS}")
        
        self.threshold = threshold
        self.method = method
        self.sentinel_value = sentinel_value
        self.primary_count_column = primary_count_column
        
        logger.info(f"SuppressionManager initialized: threshold={threshold}, method={method}")
    
    def apply(
        self,
        df: DataFrame,
        protected_columns: Optional[List[str]] = None
    ) -> DataFrame:
        """
        Apply suppression rules to a DataFrame.
        
        Args:
            df: DataFrame with protected (noisy) values.
            protected_columns: List of columns to suppress. If None, suppresses all
                              columns ending with "_protected".
        
        Returns:
            DataFrame with suppression applied.
        """
        if self.threshold == 0:
            logger.info("Suppression threshold is 0, no suppression applied")
            return df
        
        # Identify columns to suppress
        if protected_columns is None:
            protected_columns = [c for c in df.columns if c.endswith("_protected")]
        
        if not protected_columns:
            logger.warning("No protected columns found for suppression")
            return df
        
        logger.info(f"Applying suppression to {len(protected_columns)} columns with threshold={self.threshold}")
        
        # Check if primary count column exists
        if self.primary_count_column not in df.columns:
            logger.warning(f"Primary count column '{self.primary_count_column}' not found. "
                          f"Using first protected column: {protected_columns[0]}")
            self.primary_count_column = protected_columns[0]
        
        # Create suppression condition
        suppress_condition = F.col(self.primary_count_column) < self.threshold
        
        if self.method == "flag":
            # Add suppression flag column
            df = df.withColumn(
                "is_suppressed",
                F.when(suppress_condition, F.lit(True)).otherwise(F.lit(False))
            )
            
            # Also add suppression reason
            df = df.withColumn(
                "suppression_reason",
                F.when(suppress_condition, F.lit(f"count < {self.threshold}")).otherwise(F.lit(None))
            )
            
        elif self.method == "null":
            # Set all protected columns to null when suppressed
            for col_name in protected_columns:
                df = df.withColumn(
                    col_name,
                    F.when(suppress_condition, F.lit(None)).otherwise(F.col(col_name))
                )
            
            # Also null out confidence interval columns if they exist
            ci_columns = [c for c in df.columns if "_moe_" in c or "_ci_" in c]
            for col_name in ci_columns:
                df = df.withColumn(
                    col_name,
                    F.when(suppress_condition, F.lit(None)).otherwise(F.col(col_name))
                )
                
        elif self.method == "value":
            # Set all protected columns to sentinel value when suppressed
            for col_name in protected_columns:
                df = df.withColumn(
                    col_name,
                    F.when(suppress_condition, F.lit(self.sentinel_value)).otherwise(F.col(col_name))
                )
        
        # Count suppressed cells
        suppressed_count = df.filter(suppress_condition).count()
        total_count = df.count()
        suppression_rate = (suppressed_count / total_count * 100) if total_count > 0 else 0
        
        logger.info(f"Suppression complete: {suppressed_count}/{total_count} cells suppressed ({suppression_rate:.2f}%)")
        
        return df
    
    def get_suppression_stats(self, df: DataFrame) -> dict:
        """
        Get statistics about suppression in a DataFrame.
        
        Args:
            df: DataFrame with suppression already applied.
        
        Returns:
            Dictionary with suppression statistics.
        """
        stats = {
            "threshold": self.threshold,
            "method": self.method,
        }
        
        if "is_suppressed" in df.columns:
            suppressed = df.filter(F.col("is_suppressed") == True).count()
            total = df.count()
            stats["suppressed_count"] = suppressed
            stats["total_count"] = total
            stats["suppression_rate"] = suppressed / total if total > 0 else 0
        
        return stats


class ComplementarySuppression:
    """
    Implements complementary suppression for consistency.
    
    When one cell is suppressed, related cells may also need suppression
    to prevent differencing attacks.
    
    Example:
    - If Province A has 3 cities and City 1 is suppressed
    - An attacker knowing Province total and Cities 2,3 could infer City 1
    - Solution: Suppress at least one more city (secondary suppression)
    """
    
    def __init__(self, primary_manager: SuppressionManager):
        self.primary = primary_manager
    
    def apply_with_complementary(
        self,
        df: DataFrame,
        group_columns: List[str],
        count_column: str
    ) -> DataFrame:
        """
        Apply primary and complementary suppression.
        
        Args:
            df: DataFrame with protected values.
            group_columns: Columns defining the group (e.g., ['province_code', 'day_idx']).
            count_column: Column with count values.
        
        Returns:
            DataFrame with complementary suppression applied.
        """
        # First apply primary suppression
        df = self.primary.apply(df, [count_column])
        
        if "is_suppressed" not in df.columns:
            return df
        
        # For each group, check if complementary suppression is needed
        from pyspark.sql.window import Window
        
        window = Window.partitionBy(*group_columns)
        
        # Count suppressed and non-suppressed in each group
        df = df.withColumn(
            "_suppressed_in_group",
            F.sum(F.when(F.col("is_suppressed"), 1).otherwise(0)).over(window)
        )
        df = df.withColumn(
            "_total_in_group",
            F.count("*").over(window)
        )
        df = df.withColumn(
            "_non_suppressed_in_group",
            F.col("_total_in_group") - F.col("_suppressed_in_group")
        )
        
        # If only one cell is suppressed and there are other cells,
        # we need complementary suppression
        needs_complementary = (
            (F.col("_suppressed_in_group") >= 1) &
            (F.col("_non_suppressed_in_group") == 1) &
            (~F.col("is_suppressed"))
        )
        
        # Apply complementary suppression
        df = df.withColumn(
            "is_suppressed",
            F.when(needs_complementary, F.lit(True)).otherwise(F.col("is_suppressed"))
        )
        df = df.withColumn(
            "suppression_reason",
            F.when(needs_complementary, F.lit("complementary suppression"))
             .otherwise(F.col("suppression_reason"))
        )
        
        # Clean up temporary columns
        df = df.drop("_suppressed_in_group", "_total_in_group", "_non_suppressed_in_group")
        
        return df

