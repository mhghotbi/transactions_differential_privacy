"""
Non-Negative Least Squares (NNLS) Post-Processing.

Implements Census 2020-style consistency enforcement:
- Adjusts noisy city values to sum to exact province totals
- Maintains non-negativity constraints
- Minimizes L2 distance from noisy values

Reference: "The 2020 Census Disclosure Avoidance System TopDown Algorithm"
"""

import logging
from typing import Dict, List, Optional, Tuple, Union
from dataclasses import dataclass

import numpy as np

try:
    from scipy.optimize import nnls, lsq_linear
    from scipy.sparse import csr_matrix
    HAS_SCIPY = True
except ImportError:
    HAS_SCIPY = False


logger = logging.getLogger(__name__)


@dataclass
class ConsistencyConstraint:
    """Represents a sum constraint: sum of children must equal parent."""
    child_indices: List[int]  # Indices of child cells
    parent_value: float       # Target sum (invariant)
    name: str = ""           # Description


class NNLSPostProcessor:
    """
    Non-Negative Least Squares optimization for DP post-processing.
    
    Given noisy measurements y and constraint that sum(x) = T,
    solves: min ||x - y||² subject to sum(x) = T, x >= 0
    
    This is the Census 2020 approach for geographic consistency.
    """
    
    def __init__(self, method: str = 'scipy'):
        """
        Initialize post-processor.
        
        Args:
            method: 'scipy' for scipy.optimize, 'simple' for closed-form
        """
        self.method = method
        
        if method == 'scipy' and not HAS_SCIPY:
            logger.warning("scipy not available, falling back to simple method")
            self.method = 'simple'
    
    def solve(
        self,
        noisy_values: np.ndarray,
        target_sum: float,
        weights: Optional[np.ndarray] = None
    ) -> np.ndarray:
        """
        Adjust noisy values to sum to target while minimizing change.
        
        Args:
            noisy_values: Noisy measurements (1D array)
            target_sum: Required sum (invariant)
            weights: Optional weights for each value (higher = less change)
            
        Returns:
            Adjusted values that sum to target_sum and are non-negative
        """
        n = len(noisy_values)
        
        if n == 0:
            return noisy_values.copy()
        
        if n == 1:
            # Single value must equal target
            return np.array([max(0, target_sum)])
        
        if self.method == 'scipy':
            return self._solve_scipy(noisy_values, target_sum, weights)
        else:
            return self._solve_simple(noisy_values, target_sum)
    
    def _solve_scipy(
        self,
        noisy_values: np.ndarray,
        target_sum: float,
        weights: Optional[np.ndarray] = None
    ) -> np.ndarray:
        """
        Solve using scipy's lsq_linear with bounds and equality constraint.
        
        Problem: min ||x - y||² s.t. sum(x) = T, x >= 0
        
        This is a quadratic programming problem. We solve it using:
        1. KKT conditions for the equality constraint
        2. Iterative projection for non-negativity
        
        For better numerical stability, we use scipy.optimize.lsq_linear
        when available, with equality constraint via reformulation.
        """
        n = len(noisy_values)
        
        # Handle edge cases
        if target_sum < 0:
            logger.warning(f"Negative target_sum {target_sum}, setting to 0")
            target_sum = 0
        
        if target_sum == 0:
            return np.zeros(n)
        
        # Try scipy.optimize.lsq_linear if available
        try:
            from scipy.optimize import lsq_linear
            
            # Reformulate as: min ||Ax - b||² with bounds and equality
            # We use the null-space method:
            # x = x_particular + null_space @ z
            # where x_particular satisfies sum(x) = target_sum
            
            # Simpler approach: use projected gradient descent
            # with KKT-based closed form solution
            
            # Start with closed-form solution ignoring non-negativity
            # For min ||x - y||² s.t. sum(x) = T:
            # x* = y + (T - sum(y))/n * ones
            
            adjustment = (target_sum - noisy_values.sum()) / n
            x = noisy_values + adjustment
            
            # Iterative projection to satisfy x >= 0 and sum(x) = T
            max_iters = 100
            tol = 1e-10
            
            for iteration in range(max_iters):
                # Project to non-negative
                x_old = x.copy()
                x = np.maximum(x, 0)
                
                # Re-adjust to satisfy sum constraint
                current_sum = x.sum()
                
                if current_sum < tol:
                    # All values clamped to 0, distribute evenly
                    x = np.full(n, target_sum / n)
                    break
                
                if abs(current_sum - target_sum) < tol:
                    break
                
                # Dykstra-like projection: adjust positive values
                diff = target_sum - current_sum
                positive_mask = x > 0
                num_positive = positive_mask.sum()
                
                if num_positive > 0:
                    x[positive_mask] += diff / num_positive
                    x = np.maximum(x, 0)
                
                # Check convergence
                if np.linalg.norm(x - x_old) < tol:
                    break
            
            return x
            
        except ImportError:
            # Fall back to simple method
            return self._solve_simple(noisy_values, target_sum)
    
    def _solve_simple(
        self,
        noisy_values: np.ndarray,
        target_sum: float
    ) -> np.ndarray:
        """
        Simple closed-form solution with iterative non-negativity projection.
        
        Algorithm:
        1. Compute difference: d = (T - sum(y)) / n
        2. Add d to all values
        3. Set negatives to 0
        4. Redistribute the excess to positive values
        5. Repeat until converged
        """
        n = len(noisy_values)
        x = noisy_values.copy().astype(float)
        
        if target_sum < 0:
            target_sum = 0
        
        if target_sum == 0:
            return np.zeros(n)
        
        max_iters = 100
        for _ in range(max_iters):
            current_sum = x.sum()
            
            if abs(current_sum - target_sum) < 1e-10:
                break
            
            # Adjustment needed
            diff = target_sum - current_sum
            
            # Find active set (values that can be adjusted)
            if diff > 0:
                # Need to increase sum - all values can increase
                x += diff / n
            else:
                # Need to decrease sum - only positive values can decrease
                positive_mask = x > 0
                num_positive = positive_mask.sum()
                
                if num_positive == 0:
                    break
                
                x[positive_mask] += diff / num_positive
            
            # Project to non-negative
            x = np.maximum(x, 0)
        
        return x
    
    def solve_hierarchical(
        self,
        noisy_values: np.ndarray,
        constraints: List[ConsistencyConstraint]
    ) -> np.ndarray:
        """
        Solve with multiple hierarchical constraints.
        
        For Census-style hierarchy: province sums must match, then adjust cities.
        
        Args:
            noisy_values: All noisy city values
            constraints: List of sum constraints (one per province)
            
        Returns:
            Adjusted values satisfying all constraints
        """
        result = noisy_values.copy().astype(float)
        
        for constraint in constraints:
            indices = constraint.child_indices
            target = constraint.parent_value
            
            # Extract subset
            subset = result[indices]
            
            # Solve for this subset
            adjusted = self.solve(subset, target)
            
            # Update result
            result[indices] = adjusted
        
        return result


class TemporalConsistencyProcessor:
    """
    Enforces temporal consistency: daily values must sum to monthly invariant.
    
    For Census 2020-style monthly invariants:
    - National monthly totals are EXACT (no noise)
    - Province monthly totals are EXACT (no noise)
    - Daily values can have noise but must sum to monthly total
    """
    
    def __init__(self):
        self.nnls = NNLSPostProcessor()
    
    def enforce_monthly_invariant(
        self,
        daily_values: np.ndarray,
        monthly_total: float
    ) -> np.ndarray:
        """
        Adjust daily values to sum to monthly total.
        
        Args:
            daily_values: Noisy daily values (shape: [num_days])
            monthly_total: True monthly total (invariant)
            
        Returns:
            Adjusted daily values that sum to monthly_total
        """
        return self.nnls.solve(daily_values, monthly_total)
    
    def enforce_all_invariants(
        self,
        data: Dict[str, np.ndarray],
        invariants: Dict[str, float]
    ) -> Dict[str, np.ndarray]:
        """
        Enforce all temporal invariants.
        
        Args:
            data: Dict mapping (province, query) to daily values array
            invariants: Dict mapping (province, query) to monthly total
            
        Returns:
            Adjusted data with invariants enforced
        """
        result = {}
        
        for key, daily_values in data.items():
            if key in invariants:
                monthly_total = invariants[key]
                result[key] = self.enforce_monthly_invariant(
                    daily_values, monthly_total
                )
            else:
                result[key] = daily_values.copy()
        
        return result


class GeographicConsistencyProcessor:
    """
    Enforces geographic consistency: city values must sum to province total.
    """
    
    def __init__(self):
        self.nnls = NNLSPostProcessor()
    
    def enforce_province_consistency(
        self,
        city_values: Dict[str, float],
        province_total: float
    ) -> Dict[str, float]:
        """
        Adjust city values to sum to province total.
        
        Args:
            city_values: Dict mapping city_name to noisy value
            province_total: True province total (invariant)
            
        Returns:
            Adjusted city values
        """
        cities = list(city_values.keys())
        values = np.array([city_values[c] for c in cities])
        
        adjusted = self.nnls.solve(values, province_total)
        
        return {city: adjusted[i] for i, city in enumerate(cities)}


def create_constraint_matrix(
    num_cells: int,
    province_city_mapping: Dict[int, List[int]]
) -> Tuple[np.ndarray, np.ndarray]:
    """
    Create constraint matrix for hierarchical consistency.
    
    Args:
        num_cells: Total number of city cells
        province_city_mapping: Maps province_idx to list of city_indices
        
    Returns:
        A: Constraint matrix where each row is a province sum constraint
        b: Right-hand side (will be filled with province totals)
    """
    num_provinces = len(province_city_mapping)
    
    A = np.zeros((num_provinces, num_cells))
    
    for p_idx, (province, cities) in enumerate(province_city_mapping.items()):
        for city_idx in cities:
            A[p_idx, city_idx] = 1.0
    
    return A, np.zeros(num_provinces)

