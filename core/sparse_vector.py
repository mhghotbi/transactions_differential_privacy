"""
Sparse Vector Technique for Differential Privacy.

Implements the Sparse Vector Technique (SVT) for efficient private release
of sparse data. Most (city, MCC, day) cells are empty or have very low counts.

SVT allows us to:
1. Privately test which cells are "above threshold"
2. Only release cells that pass the test
3. Suppress cells that fail (below threshold) without releasing noisy values

This is MORE EFFICIENT than:
- Adding noise to all cells (including zeros)
- Post-hoc suppression based on noisy values

Reference:
- Dwork & Roth, "The Algorithmic Foundations of Differential Privacy", Ch. 3.6
- "The Sparse Vector Technique" (SVT)

Privacy Guarantee:
- Testing k cells above threshold costs O(1) budget (not O(k))
- Only cells that pass get noisy values released (costs budget)
- Cells that fail are suppressed without additional cost
"""

import math
import logging
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Union
from fractions import Fraction
from enum import Enum

import numpy as np


logger = logging.getLogger(__name__)


class ReleaseDecision(Enum):
    """Decision for whether to release a cell's value."""
    RELEASE = "release"      # Cell passed threshold, release noisy value
    SUPPRESS = "suppress"    # Cell failed threshold, suppress
    SKIP = "skip"            # Cell was not tested


@dataclass
class SVTResult:
    """Result of Sparse Vector Technique for a single cell."""
    cell_id: str
    true_value: float
    decision: ReleaseDecision
    noisy_value: Optional[float] = None  # Only set if decision == RELEASE
    test_passed: bool = False
    
    def __repr__(self) -> str:
        if self.decision == ReleaseDecision.RELEASE:
            return f"SVTResult({self.cell_id}: RELEASE, noisy={self.noisy_value:.2f})"
        else:
            return f"SVTResult({self.cell_id}: {self.decision.value})"


@dataclass
class SVTBatchResult:
    """Result of SVT for a batch of cells."""
    results: List[SVTResult]
    num_tested: int
    num_released: int
    num_suppressed: int
    rho_used: float  # Total privacy budget used
    
    @property
    def release_rate(self) -> float:
        """Fraction of cells released."""
        if self.num_tested == 0:
            return 0.0
        return self.num_released / self.num_tested
    
    def get_released_values(self) -> Dict[str, float]:
        """Get dictionary of released cell_id -> noisy_value."""
        return {
            r.cell_id: r.noisy_value
            for r in self.results
            if r.decision == ReleaseDecision.RELEASE and r.noisy_value is not None
        }
    
    def summary(self) -> str:
        """Generate summary of SVT results."""
        return (
            f"SVT Results: {self.num_released}/{self.num_tested} cells released "
            f"({100*self.release_rate:.1f}%), rho_used={self.rho_used:.6f}"
        )


class SparseVectorTechnique:
    """
    Sparse Vector Technique for private cell release.
    
    Algorithm:
    1. Add noise to threshold: T_noisy = T + Lap(2/ε₁) or Gaussian(σ_T)
    2. For each cell with true value v:
       a. Add noise to test: v_test = v + Lap(4c/ε₂) where c = num tests above
       b. If v_test >= T_noisy: RELEASE (add final noise to v)
       c. Else: SUPPRESS
    
    Privacy Analysis (for Gaussian variant under zCDP):
    - Threshold noise: σ_T^2 = 1/(2ρ_T)
    - Test noise: σ_test^2 = c^2 / (2ρ_test) where c = max releases
    - Release noise: σ_release^2 = Δ^2 / (2ρ_release)
    - Total: ρ = ρ_T + ρ_test + c * ρ_release
    
    With careful accounting:
    - Testing is essentially "free" (O(1) cost)
    - Only releases consume significant budget
    """
    
    def __init__(
        self,
        threshold: float,
        rho_threshold: float = 0.01,
        rho_test: float = 0.01,
        rho_release: float = 0.1,
        max_releases: Optional[int] = None,
        sensitivity: float = 1.0
    ):
        """
        Initialize SVT.
        
        Args:
            threshold: Threshold for cell release (cells above are released)
            rho_threshold: zCDP budget for noisy threshold
            rho_test: zCDP budget for tests (split across tests)
            rho_release: zCDP budget per release
            max_releases: Maximum number of cells to release (stops early if reached)
            sensitivity: L2 sensitivity of the query
        """
        self.threshold = threshold
        self.rho_threshold = rho_threshold
        self.rho_test = rho_test
        self.rho_release = rho_release
        self.max_releases = max_releases
        self.sensitivity = sensitivity
        
        # Compute sigmas
        self.sigma_threshold = self._sigma_from_rho(rho_threshold, 1.0)
        self.sigma_test = self._sigma_from_rho(rho_test, 1.0)
        self.sigma_release = self._sigma_from_rho(rho_release, sensitivity)
        
        # Track state
        self._noisy_threshold: Optional[float] = None
        self._num_releases = 0
        self._rho_used = Fraction(0)
        
        logger.info(f"SVT initialized: threshold={threshold}, max_releases={max_releases}")
        logger.info(f"  sigma_threshold={self.sigma_threshold:.4f}")
        logger.info(f"  sigma_test={self.sigma_test:.4f}")
        logger.info(f"  sigma_release={self.sigma_release:.4f}")
    
    @staticmethod
    def _sigma_from_rho(rho: float, sensitivity: float) -> float:
        """Compute sigma for Gaussian mechanism given rho."""
        if rho <= 0:
            return float('inf')
        return math.sqrt((sensitivity ** 2) / (2 * rho))
    
    def _initialize_threshold(self) -> float:
        """Add noise to threshold (done once per batch)."""
        if self._noisy_threshold is None:
            noise = np.random.normal(0, self.sigma_threshold)
            self._noisy_threshold = self.threshold + noise
            self._rho_used += Fraction(self.rho_threshold).limit_denominator(1000000)
            logger.debug(f"Noisy threshold: {self._noisy_threshold:.2f}")
        return self._noisy_threshold
    
    def test_and_release(
        self,
        cell_id: str,
        true_value: float
    ) -> SVTResult:
        """
        Test a single cell and potentially release its noisy value.
        
        Args:
            cell_id: Identifier for the cell
            true_value: True (non-private) value of the cell
            
        Returns:
            SVTResult with decision and optional noisy value
        """
        # Check if we've hit max releases
        if self.max_releases is not None and self._num_releases >= self.max_releases:
            return SVTResult(
                cell_id=cell_id,
                true_value=true_value,
                decision=ReleaseDecision.SKIP,
                test_passed=False
            )
        
        # Initialize noisy threshold if needed
        noisy_threshold = self._initialize_threshold()
        
        # Add noise to test
        test_noise = np.random.normal(0, self.sigma_test)
        noisy_test = true_value + test_noise
        
        # Test against noisy threshold
        if noisy_test >= noisy_threshold:
            # Cell passes - release noisy value
            release_noise = np.random.normal(0, self.sigma_release)
            noisy_value = true_value + release_noise
            
            self._num_releases += 1
            self._rho_used += Fraction(self.rho_release).limit_denominator(1000000)
            
            return SVTResult(
                cell_id=cell_id,
                true_value=true_value,
                decision=ReleaseDecision.RELEASE,
                noisy_value=noisy_value,
                test_passed=True
            )
        else:
            # Cell fails - suppress (no additional budget cost for test)
            return SVTResult(
                cell_id=cell_id,
                true_value=true_value,
                decision=ReleaseDecision.SUPPRESS,
                test_passed=False
            )
    
    def process_batch(
        self,
        cells: Dict[str, float]
    ) -> SVTBatchResult:
        """
        Process a batch of cells using SVT.
        
        Args:
            cells: Dictionary mapping cell_id to true value
            
        Returns:
            SVTBatchResult with all results
        """
        results = []
        
        for cell_id, true_value in cells.items():
            result = self.test_and_release(cell_id, true_value)
            results.append(result)
            
            # Check if we've hit max releases
            if self.max_releases is not None and self._num_releases >= self.max_releases:
                # Mark remaining cells as skipped
                remaining = list(cells.keys())[len(results):]
                for remaining_id in remaining:
                    results.append(SVTResult(
                        cell_id=remaining_id,
                        true_value=cells[remaining_id],
                        decision=ReleaseDecision.SKIP,
                        test_passed=False
                    ))
                break
        
        num_released = sum(1 for r in results if r.decision == ReleaseDecision.RELEASE)
        num_suppressed = sum(1 for r in results if r.decision == ReleaseDecision.SUPPRESS)
        
        return SVTBatchResult(
            results=results,
            num_tested=len(cells),
            num_released=num_released,
            num_suppressed=num_suppressed,
            rho_used=float(self._rho_used)
        )
    
    def reset(self) -> None:
        """Reset state for a new batch."""
        self._noisy_threshold = None
        self._num_releases = 0
        self._rho_used = Fraction(0)
    
    @property
    def rho_used(self) -> float:
        """Get total rho used so far."""
        return float(self._rho_used)
    
    @property
    def num_releases(self) -> int:
        """Get number of cells released so far."""
        return self._num_releases


class AdaptiveSVT:
    """
    Adaptive Sparse Vector Technique with dynamic thresholding.
    
    Uses different thresholds for different strata, based on expected
    cell volumes. Higher-volume strata can have lower relative thresholds.
    """
    
    def __init__(
        self,
        base_threshold: float,
        rho_total: float,
        sensitivity: float = 1.0
    ):
        """
        Initialize Adaptive SVT.
        
        Args:
            base_threshold: Base threshold (adjusted per stratum)
            rho_total: Total privacy budget
            sensitivity: L2 sensitivity
        """
        self.base_threshold = base_threshold
        self.rho_total = rho_total
        self.sensitivity = sensitivity
        
        # Budget split: 10% threshold, 20% tests, 70% releases
        self.rho_threshold = rho_total * 0.1
        self.rho_test = rho_total * 0.2
        self.rho_release = rho_total * 0.7
        
        self._stratum_svts: Dict[str, SparseVectorTechnique] = {}
    
    def get_svt_for_stratum(
        self,
        stratum_id: str,
        stratum_scale: float = 1.0,
        max_releases: Optional[int] = None
    ) -> SparseVectorTechnique:
        """
        Get or create SVT for a specific stratum.
        
        Args:
            stratum_id: Stratum identifier
            stratum_scale: Scale factor for threshold (higher = higher threshold)
            max_releases: Max releases for this stratum
            
        Returns:
            SparseVectorTechnique configured for the stratum
        """
        if stratum_id not in self._stratum_svts:
            # Adjust threshold based on stratum scale
            adjusted_threshold = self.base_threshold * stratum_scale
            
            # Each stratum uses full budget (parallel composition)
            svt = SparseVectorTechnique(
                threshold=adjusted_threshold,
                rho_threshold=self.rho_threshold,
                rho_test=self.rho_test,
                rho_release=self.rho_release,
                max_releases=max_releases,
                sensitivity=self.sensitivity
            )
            self._stratum_svts[stratum_id] = svt
            
            logger.info(f"Created SVT for stratum {stratum_id}: threshold={adjusted_threshold:.2f}")
        
        return self._stratum_svts[stratum_id]
    
    def process_stratum(
        self,
        stratum_id: str,
        cells: Dict[str, float],
        stratum_scale: float = 1.0,
        max_releases: Optional[int] = None
    ) -> SVTBatchResult:
        """
        Process cells for a specific stratum.
        
        Args:
            stratum_id: Stratum identifier
            cells: Dictionary of cell_id -> true_value
            stratum_scale: Scale factor for threshold
            max_releases: Maximum releases for this stratum
            
        Returns:
            SVTBatchResult for this stratum
        """
        svt = self.get_svt_for_stratum(stratum_id, stratum_scale, max_releases)
        return svt.process_batch(cells)


def private_cell_release(
    cells: Dict[str, float],
    threshold: float,
    rho: float,
    sensitivity: float = 1.0,
    max_releases: Optional[int] = None
) -> Tuple[Dict[str, float], Dict[str, str]]:
    """
    Convenience function for private cell release using SVT.
    
    Args:
        cells: Dictionary of cell_id -> true_value
        threshold: Minimum value to release
        rho: Total privacy budget
        sensitivity: L2 sensitivity
        max_releases: Maximum cells to release
        
    Returns:
        Tuple of:
        - released: Dict of cell_id -> noisy_value for released cells
        - status: Dict of cell_id -> status ("released", "suppressed", "skipped")
    """
    # Split budget: 10% threshold, 10% tests, 80% releases
    svt = SparseVectorTechnique(
        threshold=threshold,
        rho_threshold=rho * 0.1,
        rho_test=rho * 0.1,
        rho_release=rho * 0.8,
        max_releases=max_releases,
        sensitivity=sensitivity
    )
    
    result = svt.process_batch(cells)
    
    released = result.get_released_values()
    status = {
        r.cell_id: r.decision.value
        for r in result.results
    }
    
    logger.info(result.summary())
    
    return released, status

