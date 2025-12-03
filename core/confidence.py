"""
Confidence Interval Calculator for Differential Privacy.

Provides margin of error and confidence intervals for DP-protected values.
This helps data users understand the uncertainty in released statistics.

Census 2020 Context:
- Census releases variance estimates alongside noisy counts
- Users can construct confidence intervals
- Helps assess reliability of small-area estimates
"""

import math
from typing import Dict, List, Optional, Tuple
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
import logging

logger = logging.getLogger(__name__)


# Z-scores for common confidence levels
Z_SCORES = {
    0.80: 1.282,
    0.90: 1.645,
    0.95: 1.960,
    0.99: 2.576,
}


class ConfidenceCalculator:
    """
    Calculates confidence intervals for DP-protected values.
    
    For Discrete Gaussian mechanism with variance sigma^2:
    - Standard deviation = sigma
    - Margin of Error at confidence level c = z_c * sigma
    - Confidence Interval = [value - MOE, value + MOE]
    
    Note: These intervals reflect ONLY the DP noise, not sampling error.
    """
    
    def __init__(self, default_confidence_level: float = 0.90):
        """
        Initialize the ConfidenceCalculator.
        
        Args:
            default_confidence_level: Default confidence level (e.g., 0.90 for 90% CI).
        """
        if not 0 < default_confidence_level < 1:
            raise ValueError("Confidence level must be between 0 and 1")
        
        self.default_confidence_level = default_confidence_level
        logger.info(f"ConfidenceCalculator initialized with {default_confidence_level*100}% confidence level")
    
    def get_z_score(self, confidence_level: float) -> float:
        """
        Get z-score for a given confidence level.
        
        Args:
            confidence_level: Confidence level (e.g., 0.90).
        
        Returns:
            Z-score for the confidence level.
        """
        if confidence_level in Z_SCORES:
            return Z_SCORES[confidence_level]
        
        # For non-standard levels, compute from normal distribution
        # Using approximation: z = sqrt(2) * erfinv(confidence_level)
        from scipy.stats import norm
        return norm.ppf((1 + confidence_level) / 2)
    
    def compute_margin_of_error(
        self,
        sigma: float,
        confidence_level: Optional[float] = None
    ) -> float:
        """
        Compute margin of error for a given sigma.
        
        Args:
            sigma: Standard deviation of the DP noise.
            confidence_level: Confidence level. Uses default if None.
        
        Returns:
            Margin of error.
        """
        if confidence_level is None:
            confidence_level = self.default_confidence_level
        
        z = self.get_z_score(confidence_level)
        moe = z * sigma
        
        return moe
    
    def compute_confidence_interval(
        self,
        value: float,
        sigma: float,
        confidence_level: Optional[float] = None
    ) -> Tuple[float, float]:
        """
        Compute confidence interval for a single value.
        
        Args:
            value: The DP-protected value.
            sigma: Standard deviation of the DP noise.
            confidence_level: Confidence level. Uses default if None.
        
        Returns:
            Tuple of (lower_bound, upper_bound).
        """
        moe = self.compute_margin_of_error(sigma, confidence_level)
        return (value - moe, value + moe)
    
    def add_intervals_to_dataframe(
        self,
        df: DataFrame,
        sigmas: Dict[str, float],
        confidence_levels: List[float] = [0.90],
        protected_suffix: str = "_protected"
    ) -> DataFrame:
        """
        Add confidence interval columns to a DataFrame.
        
        Args:
            df: DataFrame with protected values.
            sigmas: Dictionary mapping query names to their sigma values.
            confidence_levels: List of confidence levels to compute.
            protected_suffix: Suffix for protected column names.
        
        Returns:
            DataFrame with additional MOE and CI columns.
        
        Output columns (for each query and confidence level):
            - {query}_moe_{level}: Margin of error
            - {query}_ci_lower_{level}: Lower bound
            - {query}_ci_upper_{level}: Upper bound
        """
        logger.info(f"Adding confidence intervals for {len(sigmas)} queries at levels {confidence_levels}")
        
        for query_name, sigma in sigmas.items():
            protected_col = f"{query_name}{protected_suffix}"
            
            if protected_col not in df.columns:
                logger.warning(f"Column {protected_col} not found, skipping")
                continue
            
            for level in confidence_levels:
                level_str = str(int(level * 100))  # e.g., 90 for 0.90
                moe = self.compute_margin_of_error(sigma, level)
                
                # Add MOE column (constant for all rows with same sigma)
                moe_col = f"{query_name}_moe_{level_str}"
                df = df.withColumn(moe_col, F.lit(moe))
                
                # Add lower bound (max with 0 for non-negativity)
                lower_col = f"{query_name}_ci_lower_{level_str}"
                df = df.withColumn(
                    lower_col,
                    F.greatest(F.col(protected_col) - F.lit(moe), F.lit(0.0))
                )
                
                # Add upper bound
                upper_col = f"{query_name}_ci_upper_{level_str}"
                df = df.withColumn(
                    upper_col,
                    F.col(protected_col) + F.lit(moe)
                )
                
                logger.debug(f"Added CI columns for {query_name} at {level_str}%: MOE={moe:.2f}")
        
        return df
    
    def compute_relative_moe(
        self,
        value: float,
        sigma: float,
        confidence_level: Optional[float] = None
    ) -> float:
        """
        Compute relative margin of error (as percentage of value).
        
        Args:
            value: The DP-protected value.
            sigma: Standard deviation of the DP noise.
            confidence_level: Confidence level.
        
        Returns:
            Relative MOE as a decimal (e.g., 0.05 for 5%).
        """
        if value == 0:
            return float('inf')
        
        moe = self.compute_margin_of_error(sigma, confidence_level)
        return moe / abs(value)
    
    def add_relative_moe_to_dataframe(
        self,
        df: DataFrame,
        sigmas: Dict[str, float],
        confidence_level: float = 0.90,
        protected_suffix: str = "_protected"
    ) -> DataFrame:
        """
        Add relative MOE columns to a DataFrame.
        
        Args:
            df: DataFrame with protected values.
            sigmas: Dictionary mapping query names to their sigma values.
            confidence_level: Confidence level.
            protected_suffix: Suffix for protected column names.
        
        Returns:
            DataFrame with additional relative MOE columns.
        """
        level_str = str(int(confidence_level * 100))
        
        for query_name, sigma in sigmas.items():
            protected_col = f"{query_name}{protected_suffix}"
            
            if protected_col not in df.columns:
                continue
            
            moe = self.compute_margin_of_error(sigma, confidence_level)
            
            # Add relative MOE column
            rel_moe_col = f"{query_name}_rel_moe_{level_str}"
            df = df.withColumn(
                rel_moe_col,
                F.when(F.col(protected_col) != 0, F.lit(moe) / F.abs(F.col(protected_col)))
                 .otherwise(F.lit(None))
            )
        
        return df


class VarianceEstimator:
    """
    Estimates variance components for DP-protected statistics.
    
    Total variance = DP noise variance + sampling variance (if applicable)
    
    For counting queries with Discrete Gaussian:
        Var(M(x)) = sigma^2
    
    For sums with sensitivity Delta:
        Var(M(x)) = sigma^2 where sigma^2 = Delta^2 / (2 * rho)
    """
    
    def __init__(self):
        pass
    
    def compute_dp_variance(self, sigma: float) -> float:
        """Compute variance from DP noise."""
        return sigma ** 2
    
    def compute_total_variance(
        self,
        dp_sigma: float,
        sampling_variance: float = 0.0
    ) -> float:
        """
        Compute total variance including both DP and sampling.
        
        Args:
            dp_sigma: Standard deviation of DP noise.
            sampling_variance: Variance from sampling (if data is a sample).
        
        Returns:
            Total variance.
        """
        return self.compute_dp_variance(dp_sigma) + sampling_variance
    
    def compute_effective_sample_size(
        self,
        true_count: int,
        dp_sigma: float
    ) -> float:
        """
        Compute effective sample size given DP noise.
        
        This measures how much information is "lost" due to DP noise.
        Higher noise = lower effective sample size.
        
        Args:
            true_count: The true count (before noise).
            dp_sigma: Standard deviation of DP noise.
        
        Returns:
            Effective sample size.
        """
        if dp_sigma == 0:
            return true_count
        
        # Effective n such that SE(mean) would equal DP noise
        # SE = sigma / sqrt(n), so n_eff = true_count / (1 + sigma^2 / true_count)
        return true_count / (1 + (dp_sigma ** 2) / true_count)


def sigma_to_confidence_summary(sigma: float, value: float = 100) -> Dict[str, float]:
    """
    Generate a summary of confidence metrics for documentation.
    
    Args:
        sigma: Standard deviation of DP noise.
        value: Reference value for relative metrics.
    
    Returns:
        Dictionary with various confidence metrics.
    """
    calc = ConfidenceCalculator()
    
    return {
        "sigma": sigma,
        "variance": sigma ** 2,
        "moe_80": calc.compute_margin_of_error(sigma, 0.80),
        "moe_90": calc.compute_margin_of_error(sigma, 0.90),
        "moe_95": calc.compute_margin_of_error(sigma, 0.95),
        "moe_99": calc.compute_margin_of_error(sigma, 0.99),
        "relative_moe_90_at_100": calc.compute_relative_moe(value, sigma, 0.90),
        "ci_90_at_100": calc.compute_confidence_interval(value, sigma, 0.90),
    }
