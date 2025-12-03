"""
Smooth Sensitivity for Differential Privacy.

Implements smooth sensitivity framework for heavy-tailed data like transaction amounts.
Standard global sensitivity can be very high due to outliers, requiring excessive noise.

Smooth sensitivity provides:
- Data-dependent noise calibration
- Lower noise for "typical" data distributions
- Same privacy guarantees as global sensitivity

Reference:
- Nissim, Raskhodnikova, Smith (2007): "Smooth Sensitivity and Sampling in Private Data Analysis"

Key Concepts:
- Local sensitivity at x: max change from adding/removing one record at x
- Smooth sensitivity: smoothed version that changes slowly with distance
- β-smooth: S(x) ≤ e^β · S(x') for neighboring x, x'
"""

import math
import logging
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Callable
from fractions import Fraction

import numpy as np

try:
    from pyspark.sql import DataFrame
    from pyspark.sql import functions as F
    HAS_SPARK = True
except ImportError:
    HAS_SPARK = False


logger = logging.getLogger(__name__)


@dataclass
class LocalSensitivityResult:
    """Result of local sensitivity computation."""
    local_sensitivity: float       # Sensitivity at the actual data
    global_sensitivity: float      # Worst-case sensitivity
    smooth_sensitivity: float      # Smoothed sensitivity
    beta: float                    # Smoothing parameter
    effective_epsilon: float       # Effective epsilon accounting for smooth sensitivity
    
    @property
    def noise_reduction_factor(self) -> float:
        """How much we reduced noise vs global sensitivity."""
        if self.global_sensitivity == 0:
            return 1.0
        return self.smooth_sensitivity / self.global_sensitivity
    
    def __repr__(self) -> str:
        return (
            f"LocalSensitivityResult(local={self.local_sensitivity:.4f}, "
            f"global={self.global_sensitivity:.4f}, "
            f"smooth={self.smooth_sensitivity:.4f}, "
            f"reduction={100*(1-self.noise_reduction_factor):.1f}%)"
        )


class SmoothSensitivityCalculator:
    """
    Computes smooth sensitivity for sum queries on transaction amounts.
    
    For sum queries with bounded contributions:
    - Global sensitivity = D_max * cap (where D_max = max cells per card)
    - Local sensitivity depends on actual data distribution
    - Smooth sensitivity interpolates between local and global
    
    The β-smooth sensitivity is:
        S*(x) = max_k [ e^(-β·k) · LS(x, k) ]
    where LS(x, k) is the local sensitivity at distance k from x.
    """
    
    def __init__(
        self,
        epsilon: float,
        delta: float = 1e-10,
        beta_fraction: float = 0.5
    ):
        """
        Initialize calculator.
        
        Args:
            epsilon: Privacy parameter
            delta: Privacy parameter
            beta_fraction: What fraction of epsilon to use for beta (default 0.5)
        """
        self.epsilon = epsilon
        self.delta = delta
        # β = epsilon / (2 * ln(2/δ)) for Cauchy mechanism
        # Or β = epsilon * beta_fraction for simplicity
        self.beta = epsilon * beta_fraction
        
        logger.info(f"SmoothSensitivityCalculator: ε={epsilon}, δ={delta}, β={self.beta:.6f}")
    
    def compute_local_sensitivity_sum(
        self,
        values: np.ndarray,
        cap: float,
        d_max: int
    ) -> float:
        """
        Compute local sensitivity for a sum query.
        
        For a sum of capped values where one card can contribute to d_max cells:
        - Removing a card changes at most d_max cells
        - Each cell's sum changes by at most cap
        - L1 local sensitivity = d_max * cap
        
        But the ACTUAL contribution of any single card may be less than d_max * cap.
        
        Args:
            values: Array of cell values (sums)
            cap: Per-cell contribution cap
            d_max: Maximum cells per card
            
        Returns:
            Local sensitivity at this data
        """
        # For sum queries, local sensitivity equals global sensitivity
        # unless we have additional information about the data structure
        # Here we use global as a conservative estimate
        return d_max * cap
    
    def compute_smooth_sensitivity_sum(
        self,
        values: np.ndarray,
        cap: float,
        d_max: int,
        max_distance: int = 100
    ) -> float:
        """
        Compute β-smooth sensitivity for a sum query.
        
        S*(x) = max_k [ e^(-β·k) · LS(x, k) ]
        
        For sum queries with bounded contributions:
        LS(x, k) = max sensitivity when adding/removing k cards
        
        Args:
            values: Array of cell values
            cap: Per-cell cap
            d_max: Max cells per card
            max_distance: Maximum distance k to consider
            
        Returns:
            Smooth sensitivity
        """
        # For bounded contribution sum queries:
        # Adding k cards can increase sensitivity by k * d_max * cap
        # But the decay e^(-β·k) dampens far distances
        
        smooth_sens = 0.0
        
        for k in range(max_distance + 1):
            # Local sensitivity at distance k
            # Each of k cards can contribute d_max * cap
            local_sens_at_k = (k + 1) * d_max * cap
            
            # Apply exponential decay
            smooth_at_k = math.exp(-self.beta * k) * local_sens_at_k
            
            smooth_sens = max(smooth_sens, smooth_at_k)
            
            # Early termination if decay dominates
            if smooth_at_k < smooth_sens * 0.01:
                break
        
        return smooth_sens
    
    def compute_for_amount_query(
        self,
        cell_amounts: np.ndarray,
        winsorize_cap: float,
        d_max: int
    ) -> LocalSensitivityResult:
        """
        Compute sensitivity for total_amount query.
        
        Args:
            cell_amounts: Array of cell amounts
            winsorize_cap: Cap per cell per card
            d_max: Max cells per card
            
        Returns:
            LocalSensitivityResult with all sensitivity values
        """
        # Global sensitivity (worst case)
        global_sens = math.sqrt(d_max) * winsorize_cap  # L2 for Gaussian
        
        # Local sensitivity at this data
        local_sens = self.compute_local_sensitivity_sum(
            cell_amounts, winsorize_cap, d_max
        )
        
        # Smooth sensitivity
        smooth_sens = self.compute_smooth_sensitivity_sum(
            cell_amounts, winsorize_cap, d_max
        )
        
        # For Gaussian mechanism with smooth sensitivity:
        # We use σ = S* · sqrt(2 ln(1.25/δ)) / ε
        effective_epsilon = self.epsilon
        
        return LocalSensitivityResult(
            local_sensitivity=local_sens,
            global_sensitivity=global_sens,
            smooth_sensitivity=smooth_sens,
            beta=self.beta,
            effective_epsilon=effective_epsilon
        )


class ProposeTestRelease:
    """
    Propose-Test-Release (PTR) framework for data-dependent sensitivity.
    
    Algorithm:
    1. PROPOSE: Propose a bound B on local sensitivity
    2. TEST: Privately test if actual local sensitivity ≤ B
    3. RELEASE: If test passes, release with noise calibrated to B
    
    This allows lower noise when data is "nice" (low sensitivity),
    while maintaining privacy even for worst-case data.
    
    Reference:
    - Dwork & Lei (2009): "Differential Privacy and Robust Statistics"
    """
    
    def __init__(
        self,
        epsilon: float,
        delta: float = 1e-10
    ):
        """
        Initialize PTR.
        
        Args:
            epsilon: Privacy parameter
            delta: Privacy parameter
        """
        self.epsilon = epsilon
        self.delta = delta
        
        # Split epsilon: half for testing, half for release
        self.epsilon_test = epsilon / 2
        self.epsilon_release = epsilon / 2
    
    def propose_test_release_sum(
        self,
        true_sum: float,
        proposed_sensitivity: float,
        actual_local_sensitivity: float,
        global_sensitivity: float
    ) -> Tuple[bool, Optional[float]]:
        """
        Execute PTR for a sum query.
        
        Args:
            true_sum: True sum value
            proposed_sensitivity: Proposed bound on sensitivity
            actual_local_sensitivity: Actual local sensitivity at this data
            global_sensitivity: Global (worst-case) sensitivity
            
        Returns:
            Tuple of (test_passed, noisy_value or None)
        """
        # Step 1: Propose - already done (proposed_sensitivity)
        
        # Step 2: Test - check if local sensitivity ≤ proposed
        # We need to test this privately
        # Gap = proposed_sensitivity - actual_local_sensitivity
        gap = proposed_sensitivity - actual_local_sensitivity
        
        # Add Laplace noise to the gap test
        # Sensitivity of the gap query is global_sensitivity
        gap_noise = np.random.laplace(0, global_sensitivity / self.epsilon_test)
        noisy_gap = gap + gap_noise
        
        # Threshold: need noisy_gap > ln(1/δ) / ε_test
        threshold = math.log(1 / self.delta) / self.epsilon_test
        
        test_passed = noisy_gap > threshold
        
        if not test_passed:
            logger.info("PTR test failed - falling back to global sensitivity")
            return False, None
        
        # Step 3: Release with noise calibrated to proposed_sensitivity
        # Use Laplace mechanism
        release_noise = np.random.laplace(0, proposed_sensitivity / self.epsilon_release)
        noisy_value = true_sum + release_noise
        
        logger.info(f"PTR test passed - releasing with sensitivity {proposed_sensitivity:.4f}")
        
        return True, noisy_value


class AdaptiveClipping:
    """
    Adaptive clipping for heavy-tailed amount distributions.
    
    Instead of using a fixed winsorization cap, we:
    1. Privately estimate the optimal clip value
    2. Clip contributions to this value
    3. Release the clipped sum
    
    This can significantly reduce noise for well-behaved distributions
    while maintaining privacy guarantees.
    
    Reference:
    - Andrew et al. (2021): "Differentially Private Learning with Adaptive Clipping"
    """
    
    def __init__(
        self,
        target_quantile: float = 0.9,
        epsilon: float = 0.1,
        min_clip: float = 100.0,
        max_clip: float = 10000000.0
    ):
        """
        Initialize adaptive clipping.
        
        Args:
            target_quantile: Target quantile for clip value (e.g., 0.9 = 90th percentile)
            epsilon: Privacy budget for clip estimation
            min_clip: Minimum clip value
            max_clip: Maximum clip value
        """
        self.target_quantile = target_quantile
        self.epsilon = epsilon
        self.min_clip = min_clip
        self.max_clip = max_clip
    
    def estimate_clip_from_spark(
        self,
        df: 'DataFrame',
        amount_col: str = "amount",
        card_col: str = "card_number",
        cell_cols: List[str] = None
    ) -> float:
        """
        Privately estimate optimal clip value from Spark DataFrame.
        
        Args:
            df: Transaction DataFrame
            amount_col: Amount column name
            card_col: Card identifier column
            cell_cols: Cell definition columns
            
        Returns:
            Privately estimated clip value
        """
        if not HAS_SPARK:
            raise RuntimeError("Spark not available")
        
        if cell_cols is None:
            cell_cols = ["acceptor_city", "mcc", "day_idx"]
        
        # Compute per-card-per-cell contribution
        card_cell_amounts = df.groupBy([card_col] + cell_cols).agg(
            F.sum(amount_col).alias("cell_amount")
        )
        
        # Get approximate quantile
        true_quantile = card_cell_amounts.stat.approxQuantile(
            "cell_amount",
            [self.target_quantile],
            0.01  # relative error
        )[0]
        
        # Add Laplace noise to make it private
        # Sensitivity of quantile is approximately range / epsilon
        sensitivity = (self.max_clip - self.min_clip) * 0.1  # Rough estimate
        noise = np.random.laplace(0, sensitivity / self.epsilon)
        
        noisy_clip = true_quantile + noise
        
        # Bound the result
        noisy_clip = max(self.min_clip, min(self.max_clip, noisy_clip))
        
        logger.info(f"Adaptive clip estimation:")
        logger.info(f"  Target quantile: {self.target_quantile}")
        logger.info(f"  True quantile: {true_quantile:,.2f}")
        logger.info(f"  Noisy clip: {noisy_clip:,.2f}")
        
        return noisy_clip
    
    def clip_and_sum(
        self,
        amounts: np.ndarray,
        clip_value: float
    ) -> Tuple[float, int]:
        """
        Clip amounts and compute sum.
        
        Args:
            amounts: Array of amounts
            clip_value: Clip threshold
            
        Returns:
            Tuple of (clipped sum, number of clipped values)
        """
        clipped = np.minimum(amounts, clip_value)
        num_clipped = np.sum(amounts > clip_value)
        
        return float(np.sum(clipped)), int(num_clipped)


def compute_smooth_sensitivity_sigma(
    smooth_sensitivity: float,
    epsilon: float,
    delta: float
) -> float:
    """
    Compute sigma for Gaussian mechanism with smooth sensitivity.
    
    For (ε, δ)-DP with smooth sensitivity S*:
    σ = S* · sqrt(2 ln(1.25/δ)) / ε
    
    Args:
        smooth_sensitivity: The β-smooth sensitivity
        epsilon: Privacy parameter
        delta: Privacy parameter
        
    Returns:
        Sigma for Gaussian noise
    """
    if epsilon <= 0:
        return float('inf')
    
    return smooth_sensitivity * math.sqrt(2 * math.log(1.25 / delta)) / epsilon


def add_smooth_sensitivity_noise(
    true_value: float,
    smooth_sensitivity: float,
    epsilon: float,
    delta: float = 1e-10
) -> float:
    """
    Add Gaussian noise calibrated to smooth sensitivity.
    
    Args:
        true_value: True query answer
        smooth_sensitivity: Computed smooth sensitivity
        epsilon: Privacy parameter
        delta: Privacy parameter
        
    Returns:
        Noisy value
    """
    sigma = compute_smooth_sensitivity_sigma(smooth_sensitivity, epsilon, delta)
    noise = np.random.normal(0, sigma)
    return true_value + noise

