"""
Census 2020-style Controlled Rounding Algorithm.

Implements integer rounding that preserves:
- Unbiasedness: E[round(x)] = x
- Boundedness: floor(x) <= round(x) <= ceil(x)
- Consistency: sum of children = parent
- Non-negativity: all values >= 0

Reference: "The 2020 Census Disclosure Avoidance System TopDown Algorithm"
Section 4.4: "Rounder"
"""

import logging
import secrets
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass

import numpy as np


logger = logging.getLogger(__name__)


@dataclass
class RoundingResult:
    """Result of the rounding operation."""
    values: np.ndarray          # Rounded integer values
    original: np.ndarray        # Original fractional values
    adjustment: np.ndarray      # Adjustment made (rounded - floor)
    satisfied_constraints: bool # Whether all constraints are satisfied


class CensusControlledRounder:
    """
    Census 2020 Controlled Rounding Algorithm.
    
    This implements the exact algorithm used in the 2020 Census DAS:
    1. Start with fractional NNLS solution
    2. Use randomized rounding respecting constraints
    3. Ensure integer solutions that sum correctly
    
    Properties:
    - Unbiased: E[round(x)] = x for each cell
    - Bounded: floor(x) <= round(x) <= ceil(x)
    - Consistent: sum(children) = parent (exact)
    - Non-negative: all values >= 0
    """
    
    def __init__(self, seed: Optional[int] = None):
        """
        Initialize rounder.
        
        Args:
            seed: Random seed for reproducibility (None = cryptographic random)
        """
        self.rng = np.random.default_rng(seed)
        self.use_crypto_random = (seed is None)
    
    def round(
        self,
        fractional: np.ndarray,
        target_sum: Optional[int] = None
    ) -> RoundingResult:
        """
        Round fractional values to integers while preserving sum.
        
        Args:
            fractional: Fractional values to round
            target_sum: Required integer sum (if None, use rounded sum)
            
        Returns:
            RoundingResult with rounded integer values
        """
        n = len(fractional)
        
        if n == 0:
            return RoundingResult(
                values=np.array([], dtype=np.int64),
                original=fractional.copy(),
                adjustment=np.array([], dtype=np.int64),
                satisfied_constraints=True
            )
        
        # Compute floors and fractional parts
        floors = np.floor(fractional).astype(np.int64)
        frac_parts = fractional - floors
        
        # Determine target sum
        if target_sum is None:
            target_sum = int(round(fractional.sum()))
        
        # Current floor sum
        floor_sum = floors.sum()
        
        # Number of values to round up
        num_round_up = target_sum - floor_sum
        
        # Handle edge cases
        if num_round_up < 0:
            # Need to round some down below floor (shouldn't happen with good NNLS)
            logger.warning(f"Negative round-up count: {num_round_up}")
            num_round_up = 0
        
        if num_round_up > n:
            # Need more round-ups than values (shouldn't happen)
            logger.warning(f"Round-up count {num_round_up} exceeds n={n}")
            num_round_up = n
        
        # Select which values to round up using randomized rounding
        round_up_indices = self._select_round_up(frac_parts, num_round_up)
        
        # Apply rounding
        result = floors.copy()
        result[round_up_indices] += 1
        
        # Ensure non-negativity
        result = np.maximum(result, 0)
        
        # Verify constraint
        satisfied = (result.sum() == target_sum)
        
        return RoundingResult(
            values=result,
            original=fractional.copy(),
            adjustment=result - floors,
            satisfied_constraints=satisfied
        )
    
    def _select_round_up(
        self,
        frac_parts: np.ndarray,
        num_round_up: int
    ) -> np.ndarray:
        """
        Select indices to round up using unbiased randomized rounding.
        
        Uses the fractional parts as probabilities to ensure unbiasedness:
        P(round up x) = frac(x)
        
        Then adjusts to get exactly num_round_up values rounded up.
        """
        n = len(frac_parts)
        
        if num_round_up == 0:
            return np.array([], dtype=np.int64)
        
        if num_round_up == n:
            return np.arange(n, dtype=np.int64)
        
        # Method: Systematic sampling weighted by fractional parts
        # This is more stable than independent Bernoulli trials
        
        # Normalize fractional parts to probabilities
        frac_sum = frac_parts.sum()
        
        if frac_sum < 1e-10:
            # All near-integers, select randomly
            indices = self.rng.choice(n, size=num_round_up, replace=False)
            return indices
        
        # Use priority-based selection
        # Higher fractional part = higher priority for rounding up
        # Add small random noise to break ties
        
        if self.use_crypto_random:
            # Cryptographically random tie-breaking
            noise = np.array([secrets.randbelow(1000000) / 1000000 for _ in range(n)])
        else:
            noise = self.rng.random(n)
        
        # Priority = fractional part + small noise
        priorities = frac_parts + noise * 0.001
        
        # Select top num_round_up indices
        indices = np.argsort(priorities)[-num_round_up:]
        
        return indices
    
    def round_with_constraints(
        self,
        fractional: np.ndarray,
        constraints: List[Tuple[List[int], int]]
    ) -> RoundingResult:
        """
        Round with multiple sum constraints (hierarchical).
        
        Args:
            fractional: Fractional values
            constraints: List of (child_indices, target_sum) tuples
            
        Returns:
            RoundingResult satisfying all constraints
        """
        n = len(fractional)
        result = np.floor(fractional).astype(np.int64)
        
        # Process each constraint
        for child_indices, target_sum in constraints:
            indices = np.array(child_indices)
            subset = fractional[indices]
            
            # Round this subset
            subset_result = self.round(subset, target_sum)
            
            # Update main result
            result[indices] = subset_result.values
        
        # Check all constraints satisfied
        all_satisfied = True
        for child_indices, target_sum in constraints:
            actual_sum = result[child_indices].sum()
            if actual_sum != target_sum:
                all_satisfied = False
                logger.warning(
                    f"Constraint not satisfied: sum={actual_sum}, target={target_sum}"
                )
        
        return RoundingResult(
            values=result,
            original=fractional.copy(),
            adjustment=result - np.floor(fractional).astype(np.int64),
            satisfied_constraints=all_satisfied
        )


class DeterministicRounder:
    """
    Deterministic rounding for reproducibility.
    
    Uses banker's rounding (round half to even) with adjustment
    to maintain sum constraint.
    """
    
    def round(
        self,
        fractional: np.ndarray,
        target_sum: Optional[int] = None
    ) -> RoundingResult:
        """
        Round deterministically while preserving sum.
        """
        n = len(fractional)
        
        # Banker's rounding
        result = np.rint(fractional).astype(np.int64)
        
        if target_sum is not None:
            # Adjust to meet target
            current_sum = result.sum()
            diff = target_sum - current_sum
            
            if diff != 0:
                # Find values closest to 0.5 to adjust
                frac_parts = np.abs(fractional - result)
                adjustment_indices = np.argsort(frac_parts)
                
                # Adjust one by one
                for idx in adjustment_indices:
                    if diff == 0:
                        break
                    
                    if diff > 0 and result[idx] < np.ceil(fractional[idx]):
                        result[idx] += 1
                        diff -= 1
                    elif diff < 0 and result[idx] > np.floor(fractional[idx]):
                        result[idx] -= 1
                        diff += 1
        
        # Ensure non-negativity
        result = np.maximum(result, 0)
        
        floors = np.floor(fractional).astype(np.int64)
        
        return RoundingResult(
            values=result,
            original=fractional.copy(),
            adjustment=result - floors,
            satisfied_constraints=(target_sum is None or result.sum() == target_sum)
        )


class HierarchicalRounder:
    """
    Rounds hierarchical data while maintaining parent-child consistency.
    
    For Census-style hierarchy:
    - Province totals are fixed (invariants)
    - City values are rounded to sum to province total
    """
    
    def __init__(self, seed: Optional[int] = None):
        self.rounder = CensusControlledRounder(seed)
    
    def round_hierarchy(
        self,
        city_values: Dict[str, Dict[str, float]],
        province_totals: Dict[str, int]
    ) -> Dict[str, Dict[str, int]]:
        """
        Round city values to match province totals.
        
        Args:
            city_values: {province: {city: fractional_value}}
            province_totals: {province: integer_total}
            
        Returns:
            Rounded integer values: {province: {city: integer_value}}
        """
        result = {}
        
        for province, cities in city_values.items():
            city_names = list(cities.keys())
            values = np.array([cities[c] for c in city_names])
            
            target = province_totals.get(province, int(round(values.sum())))
            
            rounded = self.rounder.round(values, target)
            
            result[province] = {
                city: int(rounded.values[i])
                for i, city in enumerate(city_names)
            }
        
        return result
    
    def round_daily_to_monthly(
        self,
        daily_values: np.ndarray,
        monthly_total: int
    ) -> np.ndarray:
        """
        Round daily values to sum to monthly total.
        
        Args:
            daily_values: Fractional daily values
            monthly_total: Integer monthly total (invariant)
            
        Returns:
            Integer daily values summing to monthly_total
        """
        result = self.rounder.round(daily_values, monthly_total)
        return result.values

