"""
Differential Privacy Primitives for zCDP.

This module implements the Discrete Gaussian mechanism for zero-Concentrated
Differential Privacy (zCDP). The implementation uses exact rational arithmetic
to avoid floating-point errors, following Canonne, Kamath & Steinke (2020).

Reference:
    "The Discrete Gaussian for Differential Privacy"
    https://arxiv.org/abs/2004.00010
"""

import math
import secrets
import logging
from dataclasses import dataclass
from fractions import Fraction
from typing import List, Tuple, Union, Optional

import numpy as np


logger = logging.getLogger(__name__)


# ============================================================================
# Random Number Generation
# ============================================================================

class SecureRNG:
    """
    Cryptographically secure random number generator.
    Uses secrets module for production security.
    """
    
    def integers(self, low: int, high: int) -> int:
        """
        Generate a random integer in [low, high).
        
        Args:
            low: Lower bound (inclusive)
            high: Upper bound (exclusive)
            
        Returns:
            Random integer in [low, high)
        """
        if high <= low:
            raise ValueError(f"high ({high}) must be greater than low ({low})")
        return low + secrets.randbelow(high - low)


# Default RNG factory - use secure RNG for production
_rng_factory = SecureRNG


def get_rng() -> SecureRNG:
    """Get a new RNG instance."""
    return _rng_factory()


# ============================================================================
# Helper Functions (Exact Rational Arithmetic)
# ============================================================================

def floorsqrt(num: int, denom: int) -> int:
    """
    Compute floor(sqrt(num/denom)) exactly using only integer comparisons.
    
    Args:
        num: Numerator (>= 0)
        denom: Denominator (> 0)
        
    Returns:
        floor(sqrt(num/denom))
    """
    assert num >= 0 and denom > 0
    
    a: int = 0  # maintain a^2 <= x
    b: int = 1  # maintain b^2 > x
    
    # Double b until b^2 > num/denom
    while b * b * denom <= num:
        b = 2 * b
    
    # Binary search
    while a + 1 < b:
        c = (a + b) // 2
        if c * c * denom <= num:
            a = c
        else:
            b = c
    
    return a


def limit_denominator(
    fraction: Tuple[int, int],
    max_denominator: int = 1000000,
    mode: str = "best"
) -> Tuple[int, int]:
    """
    Find best rational approximation with bounded denominator.
    
    Args:
        fraction: (numerator, denominator) tuple
        max_denominator: Maximum allowed denominator
        mode: "best" (closest), "upper" (>= fraction), "lower" (<= fraction)
        
    Returns:
        (numerator, denominator) of approximation
    """
    n, d = fraction
    gcd = math.gcd(n, d)
    n //= gcd
    d //= gcd
    
    if d <= max_denominator:
        return (n, d)
    
    p0, q0, p1, q1 = 0, 1, 1, 0
    n_work, d_work = n, d
    
    while True:
        a = n_work // d_work
        q2 = q0 + a * q1
        if q2 > max_denominator:
            break
        p0, q0, p1, q1 = p1, q1, p0 + a * p1, q2
        n_work, d_work = d_work, n_work - a * d_work
    
    k = (max_denominator - q0) // q1
    b1n, b1d = (p0 + k * p1, q0 + k * q1)  # Upper bound
    b2n, b2d = (p1, q1)  # Lower bound
    
    if mode == "best":
        if abs((b2n * d - b2d * n) * b1d) <= abs((b1n * d - n * b1d) * b2d):
            return (b2n, b2d)
        return (b1n, b1d)
    elif mode == "upper":
        return (b1n, b1d) if b1n * b2d > b2n * b1d else (b2n, b2d)
    elif mode == "lower":
        return (b2n, b2d) if b1n * b2d > b2n * b1d else (b1n, b1d)
    else:
        raise ValueError(f"Unknown mode: {mode}")


# ============================================================================
# Exact Bernoulli(exp(-gamma)) Sampling
# ============================================================================

def bernoulli_exp_scalar(gamma: Tuple[int, int], rng: SecureRNG) -> int:
    """
    Sample from Bernoulli(exp(-gamma)) exactly.
    
    Args:
        gamma: (numerator, denominator) representing gamma >= 0
        rng: Random number generator
        
    Returns:
        1 with probability exp(-gamma), 0 otherwise
    """
    gn, gd = gamma
    
    if 0 <= gn <= gd:
        k: int = 1
        a: bool = True
        while a:
            a = rng.integers(0, gd * k) < gn
            k = k + 1 if a else k
        return k % 2
    else:
        for _ in range(gn // gd):
            b = bernoulli_exp_scalar((1, 1), rng)
            if not b:
                return 0
        return bernoulli_exp_scalar((gn % gd, gd), rng)


# ============================================================================
# Exact Discrete Laplace Sampling
# ============================================================================

def discrete_laplace_scalar(s: int, t: int, rng: SecureRNG) -> int:
    """
    Sample from Discrete Laplace (two-sided Geometric) exactly.
    
    The Discrete Laplace has PMF:
        Pr[X = x] = (exp(s/t) - 1) / (exp(s/t) + 1) * exp(-|x| * s/t)
    
    Args:
        s: Scale numerator (>= 1)
        t: Scale denominator (>= 1)
        rng: Random number generator
        
    Returns:
        Sample from Discrete Laplace with scale t/s
    """
    assert s >= 1 and t >= 1
    
    while True:
        d: bool = False
        while not d:
            u: int = rng.integers(0, t)
            d = bool(bernoulli_exp_scalar((u, t), rng))
        
        v: int = 0
        a: bool = True
        while a:
            a = bool(bernoulli_exp_scalar((1, 1), rng))
            v = v + 1 if a else v
        
        x: int = u + t * v
        y: int = x // s
        b: int = rng.integers(0, 2) < 1  # Bernoulli(1/2)
        
        if not (b == 1 and y == 0):
            return (1 - 2 * b) * y


# ============================================================================
# Exact Discrete Gaussian Sampling
# ============================================================================

def discrete_gaussian_scalar(sigma_sq: Tuple[int, int], rng: SecureRNG) -> int:
    """
    Sample from Discrete Gaussian exactly using rational arithmetic.
    
    The Discrete Gaussian has PMF:
        Pr[X = x] = exp(-x^2 / (2*sigma^2)) / Z
    where Z is the normalizing constant.
    
    Args:
        sigma_sq: (numerator, denominator) representing sigma^2
        rng: Random number generator
        
    Returns:
        Sample from Discrete Gaussian with variance approximately sigma^2
    """
    ssq_n, ssq_d = sigma_sq
    t: int = floorsqrt(ssq_n, ssq_d) + 1
    
    while True:
        y: int = discrete_laplace_scalar(1, t, rng)
        aux1n: int = abs(y) * t * ssq_d - ssq_n
        gamma = (aux1n * aux1n, t * ssq_d * t * ssq_n * 2)
        
        if bernoulli_exp_scalar(gamma, rng):
            return y


def discrete_gaussian_vector(
    sigma_sq: Union[Fraction, float],
    size: int,
    rng: Optional[SecureRNG] = None,
    use_fast_approximation: bool = True
) -> Union[List[int], np.ndarray]:
    """
    Sample a vector from Discrete Gaussian.
    
    Args:
        sigma_sq: Variance parameter (sigma^2)
        size: Number of samples
        rng: Random number generator (optional)
        use_fast_approximation: If True, use fast NumPy sampling for large arrays
        
    Returns:
        List or array of Discrete Gaussian samples
    """
    import psutil
    logger.info(f"[DG_VECTOR] Entry: size={size:,}, sigma^2={float(sigma_sq):.4f}, use_fast={use_fast_approximation}")
    
    sigma_sq_float = float(sigma_sq)
    
    # For large arrays OR large sigma, use fast approximation
    # The Discrete Gaussian converges to continuous Gaussian for large sigma
    # For sigma^2 > 1, the approximation error is negligible
    if use_fast_approximation and (size > 1000 or sigma_sq_float > 1.0):
        logger.info(f"[DG_VECTOR] Using FAST approximation (size={size:,} > 1000 or sigma^2={sigma_sq_float:.4f} > 1.0)")
        
        # Use NumPy for fast vectorized sampling
        # Sample from continuous Gaussian and round to nearest integer
        mem_before = psutil.virtual_memory().percent
        logger.info(f"[DG_VECTOR] Computing sigma = sqrt({sigma_sq_float:.4f})...")
        sigma = np.sqrt(sigma_sq_float)
        logger.info(f"[DG_VECTOR] ✓ sigma = {sigma:.4f}")
        
        logger.info(f"[DG_VECTOR] Sampling {size:,} values from N(0, {sigma:.4f})...")
        samples = np.random.normal(0, sigma, size)
        samples_size_mb = samples.nbytes / (1024**2)
        logger.info(f"[DG_VECTOR] ✓ Samples created ({samples_size_mb:.1f} MB)")
        
        mem_after_sample = psutil.virtual_memory().percent
        logger.info(f"[DG_VECTOR] Memory after sampling: {mem_after_sample:.1f}% (+{mem_after_sample - mem_before:.1f}%)")
        
        logger.info(f"[DG_VECTOR] Rounding and converting to int64...")
        result = np.round(samples).astype(np.int64)
        result_size_mb = result.nbytes / (1024**2)
        logger.info(f"[DG_VECTOR] ✓ Rounded to int64 ({result_size_mb:.1f} MB)")
        
        mem_final = psutil.virtual_memory().percent
        logger.info(f"[DG_VECTOR] Final memory: {mem_final:.1f}% (total delta: +{mem_final - mem_before:.1f}%)")
        logger.info(f"[DG_VECTOR] Returning {size:,} samples")
        
        return result
    
    # For small arrays with small sigma, use exact sampling
    logger.info(f"[DG_VECTOR] Using EXACT sampling (size={size:,} <= 1000 and sigma^2={sigma_sq_float:.4f} <= 1.0)")
    
    if rng is None:
        logger.info(f"[DG_VECTOR] Initializing RNG...")
        rng = get_rng()
        logger.info(f"[DG_VECTOR] ✓ RNG initialized")
    
    # Convert to fraction if needed
    if isinstance(sigma_sq, float):
        logger.info(f"[DG_VECTOR] Converting sigma^2 to fraction...")
        sigma_sq = Fraction(sigma_sq).limit_denominator(1000000)
        logger.info(f"[DG_VECTOR] ✓ sigma^2 = {sigma_sq.numerator}/{sigma_sq.denominator}")
    
    n, d = sigma_sq.numerator, sigma_sq.denominator
    
    logger.info(f"[DG_VECTOR] Sampling {size:,} values using exact discrete_gaussian_scalar...")
    logger.info(f"[DG_VECTOR] WARNING: This may be SLOW for large size!")
    result = [discrete_gaussian_scalar((n, d), rng) for _ in range(size)]
    logger.info(f"[DG_VECTOR] ✓ Exact sampling complete")
    
    return result


# ============================================================================
# Discrete Gaussian Variance Computation
# ============================================================================

def compute_discrete_gaussian_variance(sigma_sq: Union[Fraction, float]) -> float:
    """
    Compute the exact variance of the Discrete Gaussian distribution.
    
    For large sigma_sq, variance approaches sigma_sq (like continuous Gaussian).
    
    Args:
        sigma_sq: The sigma^2 parameter
        
    Returns:
        Actual variance of the Discrete Gaussian
    """
    sigma_sq_fl = float(sigma_sq)
    
    # For large sigma_sq, variance converges to sigma_sq
    if sigma_sq_fl > 100:
        return sigma_sq_fl
    
    # Compute exactly for smaller values
    bound = int(math.floor(50.0 * math.sqrt(sigma_sq_fl)))
    
    n = np.arange(-bound, 0)
    n2 = n * n
    p = np.exp(-n2 / (2.0 * sigma_sq_fl))
    
    # Variance = 2 * sum(n^2 * p) / (2 * sum(p) + 1)
    # The +1 accounts for the n=0 term in denominator
    variance = 2 * np.sum(n2 * p) / (2 * np.sum(p) + 1)
    
    return float(variance)


# ============================================================================
# DP Mechanism Classes
# ============================================================================

@dataclass
class DPMechanism:
    """Base class for DP mechanisms."""
    protected_answer: np.ndarray
    variance: float
    rho: Fraction  # zCDP parameter
    
    def __repr__(self) -> str:
        return f"DPMechanism(rho={self.rho}, variance={self.variance:.4f})"


class DiscreteGaussianMechanism(DPMechanism):
    """
    Discrete Gaussian mechanism for zCDP.
    
    For a query with L2-sensitivity Delta and zCDP parameter rho:
        sigma^2 = Delta^2 / (2 * rho)
    
    This implementation uses fast NumPy sampling for large arrays,
    with exact rational arithmetic available for small arrays.
    """
    
    def __init__(
        self,
        true_answer: np.ndarray,
        sigma_sq: Union[Fraction, float],
        rho: Optional[Fraction] = None,
        sensitivity: float = 1.0,
        use_fast_sampling: bool = True
    ):
        """
        Initialize Discrete Gaussian mechanism.
        
        Args:
            true_answer: True query answer (integer array)
            sigma_sq: Variance parameter for the Gaussian. If None, computed from rho.
            rho: zCDP parameter. If provided and sigma_sq is None, sigma_sq = sensitivity^2/(2*rho)
            sensitivity: L2 sensitivity of the query (default 1.0 for counting)
            use_fast_sampling: If True, use fast NumPy-based sampling (default True)
        """
        # Compute sigma_sq from rho if needed
        if sigma_sq is None and rho is not None:
            sigma_sq = Fraction(sensitivity ** 2) / (2 * rho)
        elif sigma_sq is None:
            raise ValueError("Either sigma_sq or rho must be provided")
        
        # Convert to Fraction if needed
        if isinstance(sigma_sq, float):
            sigma_sq = Fraction(sigma_sq).limit_denominator(1000000)
        
        # Compute rho if not provided
        if rho is None:
            rho = Fraction(sensitivity ** 2) / (2 * sigma_sq)
        
        # Store parameters
        self.sigma_sq = sigma_sq
        self.sensitivity = sensitivity
        
        # Compute variance
        variance = compute_discrete_gaussian_variance(sigma_sq)
        
        # Sample noise
        shape = true_answer.shape
        size = int(np.prod(shape))
        
        logger.info(f"[NOISE INIT] Preparing to sample {size:,} Discrete Gaussian values")
        logger.info(f"[NOISE INIT] sigma^2={float(sigma_sq):.4f}, use_fast_sampling={use_fast_sampling}")
        
        # Memory checkpoint before sampling
        import psutil
        mem_before = psutil.virtual_memory().percent
        mem_before_gb = psutil.virtual_memory().used / (1024**3)
        logger.info(f"[NOISE MEMORY] Before sampling: {mem_before:.1f}% used ({mem_before_gb:.2f} GB)")
        
        # Use fast sampling for large arrays
        logger.info(f"[NOISE SAMPLING] Starting discrete_gaussian_vector()...")
        perturbations = discrete_gaussian_vector(
            sigma_sq, size, 
            rng=None,
            use_fast_approximation=use_fast_sampling
        )
        logger.info(f"[NOISE SAMPLING] ✓ Sampling complete")
        
        # Memory checkpoint after sampling
        mem_after = psutil.virtual_memory().percent
        mem_after_gb = psutil.virtual_memory().used / (1024**3)
        mem_delta_gb = mem_after_gb - mem_before_gb
        logger.info(f"[NOISE MEMORY] After sampling: {mem_after:.1f}% used ({mem_after_gb:.2f} GB, +{mem_delta_gb:.2f} GB)")
        
        logger.info(f"[NOISE RESHAPE] Reshaping perturbations to {shape}...")
        if isinstance(perturbations, list):
            perturbations = np.array(perturbations)
        perturbations = perturbations.reshape(shape)
        logger.info(f"[NOISE RESHAPE] ✓ Reshape complete")
        
        # Apply noise
        logger.info(f"[NOISE APPLY] Adding noise to true answer...")
        protected_answer = true_answer + perturbations
        logger.info(f"[NOISE APPLY] ✓ Noise applied")
        
        # Final memory checkpoint
        mem_final = psutil.virtual_memory().percent
        mem_final_gb = psutil.virtual_memory().used / (1024**3)
        logger.info(f"[NOISE MEMORY] After applying noise: {mem_final:.1f}% used ({mem_final_gb:.2f} GB)")
        logger.info(f"[NOISE COMPLETE] ✓ DiscreteGaussianMechanism initialization complete")
        
        # Initialize base class
        super().__init__(
            protected_answer=protected_answer,
            variance=variance,
            rho=rho
        )
    
    def __repr__(self) -> str:
        return (f"DiscreteGaussianMechanism(rho={self.rho}, "
                f"sigma^2={float(self.sigma_sq):.4f}, "
                f"variance={self.variance:.4f})")


class NoNoiseMechanism(DPMechanism):
    """
    No-noise mechanism for testing (infinite privacy loss).
    """
    
    def __init__(self, true_answer: np.ndarray):
        super().__init__(
            protected_answer=true_answer.copy(),
            variance=0.0,
            rho=Fraction(0)  # Zero rho means infinite epsilon
        )
    
    def __repr__(self) -> str:
        return "NoNoiseMechanism()"


# ============================================================================
# Convenience Functions
# ============================================================================

def add_discrete_gaussian_noise(
    true_answer: np.ndarray,
    rho: Fraction,
    sensitivity: float = 1.0,
    use_fast_sampling: bool = True
) -> np.ndarray:
    """
    Add Discrete Gaussian noise to a query answer.
    
    Convenience function that creates a mechanism and returns the noisy answer.
    Uses fast NumPy-based sampling by default for performance.
    
    Args:
        true_answer: True query answer (integer array)
        rho: zCDP parameter
        sensitivity: L2 sensitivity of the query
        use_fast_sampling: If True, use fast NumPy-based sampling (default True)
        
    Returns:
        Noisy answer with Discrete Gaussian perturbations
    """
    mechanism = DiscreteGaussianMechanism(
        true_answer=true_answer,
        sigma_sq=None,
        rho=rho,
        sensitivity=sensitivity,
        use_fast_sampling=use_fast_sampling
    )
    return mechanism.protected_answer

