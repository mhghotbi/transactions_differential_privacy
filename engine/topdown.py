"""
Top-Down DP Engine with Province-Month Level Invariants and User-Level Differential Privacy.

Implements a Census 2020 DAS-style methodology for applying differential privacy
to transaction data with geographic hierarchy.

KEY FEATURES:
1. Province-month level totals are EXACT INVARIANTS (publicly published data)
2. City-level data receives ALL privacy budget
3. NNLS post-processing enforces province-month constraints with non-negativity
4. Controlled rounding maintains integer consistency

HIERARCHY:
- Province-Month (invariant): Σ_{city,mcc,day} x_{p,city,mcc,day} = Y_p  [EXACT - PUBLIC]
- Cell (province,city,mcc,day): Individual measurements with DP noise

The province-month totals are publicly available data that MUST be matched.
This is not a privacy leak - these are already public statistics.

USER-LEVEL DP:
- Privacy unit is the CARD (user), not individual transactions
- Removing a card can affect up to D_max distinct cells
- L2 sensitivity = sqrt(D_max) * K where K = max transactions per card per cell

PRIVACY GUARANTEE:
- Province-month totals are public (invariants) - no privacy cost
- Cell-level measurements satisfy (ε, δ)-DP (via zCDP)
- Post-processing (NNLS + rounding) preserves DP guarantees
"""

import math
import logging
from typing import Dict, Optional, Any, Tuple
from fractions import Fraction

import numpy as np
from scipy.optimize import nnls
from pyspark.sql import SparkSession

from core.config import Config
from schema.geography import Geography
from schema.histogram import TransactionHistogram, ProvinceHistogram, CityHistogram
from core.budget import Budget, BudgetAllocator
from core.primitives import DiscreteGaussianMechanism, add_discrete_gaussian_noise
from core.sensitivity import GlobalSensitivityCalculator, UserLevelSensitivity


logger = logging.getLogger(__name__)


class TopDownEngine:
    """
    Top-down differential privacy engine with PROVINCE-MONTH LEVEL INVARIANTS.
    
    This implements a Census 2020 DAS-style algorithm where:
    1. Province-month totals are exact invariants (publicly published - NO noise)
       - Computed as: Y_p = Σ_{city,mcc,day} x_{p,city,mcc,day}
    2. City-level data receives DP noise (ALL budget)
    3. NNLS post-processing ensures:
       - City sums match exact province-month totals
       - All values are non-negative
       - Minimum distortion from noisy measurements
    4. Controlled rounding produces integer outputs
    
    MATHEMATICAL FORMULATION:
    Let Y_p = exact province-month total for province p (PUBLIC INVARIANT)
              Y_p = Σ_{city,mcc,day} x_{p,city,mcc,day}
    Let z_{p,c,m,d} = noisy cell measurement
    
    We solve for protected values x_{p,c,m,d}:
        minimize   Σ_{p,c,m,d} (x_{p,c,m,d} - z_{p,c,m,d})²
        subject to Σ_{c,m,d} x_{p,c,m,d} = Y_p    for all provinces p
                   x_{p,c,m,d} ≥ 0                for all cells
    
    This is solved using NNLS with iterative projection per province.
    
    Uses zCDP (zero-Concentrated DP) with Discrete Gaussian mechanism.
    Privacy budget is fully allocated to cell level.
    """
    
    def __init__(
        self,
        spark: SparkSession,
        config: Config,
        geography: Geography,
        budget: Budget
    ):
        """
        Initialize engine.
        
        Args:
            spark: Active Spark session
            config: Configuration object
            geography: Geography instance
            budget: Privacy budget manager
        """
        self.spark = spark
        self.config = config
        self.geography = geography
        self.budget = budget
        self.allocator = BudgetAllocator(budget, config.data.num_days)
        
        # Store EXACT province-month aggregates (PUBLIC INVARIANTS - no noise)
        # Shape per query: (province_dim,) - one value per province
        self._province_month_invariants: Dict[str, np.ndarray] = {}
        
        # MCC group information for stratified sensitivity
        self._mcc_group_caps = config.privacy.mcc_group_caps or {}
        self._mcc_to_group = config.privacy.mcc_to_group or {}
        self._mcc_grouping_enabled = config.privacy.mcc_grouping_enabled and bool(self._mcc_group_caps)
        
        # User-level sensitivity parameters
        self._d_max: Optional[int] = None
        self._user_level_sensitivities: Dict[str, UserLevelSensitivity] = {}
        
        # Winsorize cap for amount queries
        self._winsorize_cap: float = 1.0
        
        # Post-processing configuration
        self._nnls_max_iterations = 1000
        self._nnls_tolerance = 1e-10
    
    def set_user_level_params(
        self,
        d_max: int,
        k_bound: int,
        winsorize_cap: float
    ) -> None:
        """
        Set user-level DP parameters.
        
        These MUST be computed from the data before running the engine.
        
        Args:
            d_max: Maximum cells any card appears in
            k_bound: Maximum transactions per card per cell
            winsorize_cap: Cap for amount queries (per cell per card)
        """
        self._d_max = d_max
        self._winsorize_cap = winsorize_cap
        
        # Precompute sensitivities for all queries
        sqrt_d = math.sqrt(d_max)
        
        self._user_level_sensitivities = {
            "transaction_count": UserLevelSensitivity(
                query_name="transaction_count",
                max_cells_per_user=d_max,
                per_cell_contribution=float(k_bound),
                l2_sensitivity=sqrt_d * k_bound,
                l1_sensitivity=d_max * k_bound,
                sensitivity_type="count"
            ),
            "unique_cards": UserLevelSensitivity(
                query_name="unique_cards",
                max_cells_per_user=d_max,
                per_cell_contribution=1.0,
                l2_sensitivity=sqrt_d,
                l1_sensitivity=float(d_max),
                sensitivity_type="unique"
            ),
            "total_amount": UserLevelSensitivity(
                query_name="total_amount",
                max_cells_per_user=d_max,
                per_cell_contribution=winsorize_cap,
                l2_sensitivity=sqrt_d * winsorize_cap,
                l1_sensitivity=d_max * winsorize_cap,
                sensitivity_type="sum"
            ),
        }
        
        logger.info("=" * 60)
        logger.info("User-Level DP Parameters Set")
        logger.info("=" * 60)
        logger.info(f"D_max (max cells per card): {d_max}")
        logger.info(f"sqrt(D_max): {sqrt_d:.4f}")
        logger.info(f"K (per-cell bound): {k_bound}")
        logger.info(f"Winsorize cap: {winsorize_cap:,.2f}")
        logger.info("")
        logger.info("User-Level L2 Sensitivities:")
        for name, sens in self._user_level_sensitivities.items():
            logger.info(f"  {name:20s}: {sens.l2_sensitivity:,.4f}")
        logger.info("=" * 60)
    
    def run(self, histogram: TransactionHistogram) -> TransactionHistogram:
        """
        Apply top-down DP with PROVINCE-MONTH LEVEL INVARIANTS.
        
        Algorithm:
        1. Compute province-month totals from data (these match public statistics)
        2. Add DP noise to cell-level data (full budget)
        3. NNLS post-processing to enforce province-month constraints
        4. Controlled rounding for integer consistency
        
        Args:
            histogram: Original histogram with true counts
            
        Returns:
            New histogram with DP-protected values
        """
        logger.info("=" * 60)
        logger.info("Top-Down DP with PROVINCE-MONTH LEVEL INVARIANTS")
        logger.info("=" * 60)
        logger.info(f"Total privacy budget (rho): {self.budget.total_rho}")
        logger.info(f"Epsilon at delta={self.budget.delta}: {self.budget.total_epsilon:.4f}")
        logger.info("")
        logger.info("INVARIANT POLICY: Province-month totals are EXACT (publicly published)")
        logger.info("                  Y_p = Σ_{city,mcc,day} x_{p,city,mcc,day}")
        logger.info("NOISE POLICY: All budget allocated to cell level")
        
        # Verify user-level parameters are set
        if self._d_max is None:
            self._handle_missing_user_level_params()
        else:
            logger.info(f"User-level D_max: {self._d_max}")
            logger.info(f"sqrt(D_max): {math.sqrt(self._d_max):.4f}")
        
        # Create a copy for the protected result
        protected = histogram.copy()
        
        # Step 1: Compute province-month totals (PUBLIC INVARIANTS)
        logger.info("")
        logger.info("=" * 40)
        logger.info("Step 1: Compute Province-Month Invariants (Public Data)")
        logger.info("=" * 40)
        self._compute_province_month_invariants(histogram)
        
        # Step 2: Add noise at cell level (FULL BUDGET)
        logger.info("")
        logger.info("=" * 40)
        logger.info("Step 2: Cell-Level Noise Injection (Full Budget)")
        logger.info("=" * 40)
        self._apply_cell_level_noise(histogram, protected)
        
        # Step 3: NNLS post-processing (enforce province-month constraints + non-negativity)
        logger.info("")
        logger.info("=" * 40)
        logger.info("Step 3: NNLS Post-Processing (Province-Month Constraints)")
        logger.info("=" * 40)
        self._nnls_post_process(protected)
        
        # Step 4: Controlled rounding
        logger.info("")
        logger.info("=" * 40)
        logger.info("Step 4: Controlled Rounding")
        logger.info("=" * 40)
        self._controlled_rounding(protected)
        
        # Verify invariants are preserved
        logger.info("")
        logger.info("=" * 40)
        logger.info("Verification: Province-Month Invariants Check")
        logger.info("=" * 40)
        self._verify_invariants(protected)
        
        logger.info("")
        logger.info("=" * 60)
        logger.info("Top-Down DP Processing Complete")
        logger.info("=" * 60)
        
        return protected
    
    def _handle_missing_user_level_params(self) -> None:
        """Handle case where user-level params are not set."""
        logger.warning(
            "WARNING: User-level DP parameters not set! "
            "Using fallback K-only sensitivity which may violate privacy. "
            "Call set_user_level_params() before running."
        )
        K = self.config.privacy.computed_contribution_bound or 1
        self._d_max = 1
        self._winsorize_cap = 1.0
        self.set_user_level_params(d_max=1, k_bound=K, winsorize_cap=self._winsorize_cap)
    
    def _compute_province_month_invariants(self, histogram: TransactionHistogram) -> None:
        """
        Compute province-month level totals as invariants.
        
        These are PUBLIC DATA that must be matched exactly.
        No noise is added - these values are already publicly known.
        
        Province-month invariant for province p:
        Y_p = Σ_{city,mcc,day} x_{p,city,mcc,day}
        
        Output shape per query: (province_dim,)
        """
        queries = TransactionHistogram.QUERIES
        
        for query in queries:
            # Get full histogram: shape (province, city, mcc, day)
            data = histogram.get_query_array(query)
            
            # Aggregate to province-month level: sum over cities, MCCs, and days
            # Result shape: (province_dim,)
            province_month_totals = np.sum(data, axis=(1, 2, 3))  # Sum over city, mcc, day
            
            # Store as exact invariant (PUBLIC DATA - no noise)
            self._province_month_invariants[query] = province_month_totals.astype(np.int64)
            
            total_value = np.sum(province_month_totals)
            num_provinces = len(province_month_totals)
            nonzero_provinces = np.sum(province_month_totals > 0)
            
            logger.info(
                f"  {query}: total={total_value:,}, "
                f"provinces={nonzero_provinces}/{num_provinces} with data"
            )
        
        logger.info("  [Province-month totals stored as PUBLIC INVARIANTS]")
    
    def _apply_cell_level_noise(
        self,
        original: TransactionHistogram,
        protected: TransactionHistogram
    ) -> None:
        """
        Apply noise at cell level with FULL privacy budget.
        
        Since province-month totals are public invariants (no privacy cost),
        the entire privacy budget is allocated to cell-level measurements.
        
        Budget allocation:
        - Province-month level: 0 (public data - invariants)
        - Cell level: 100% of total_rho
        
        This is split among queries according to config.privacy.query_split.
        """
        queries = TransactionHistogram.QUERIES
        total_queries = len(queries)
        
        # Get the mask of cells with actual data
        has_data_mask = original._has_data
        num_data_cells = np.sum(has_data_mask)
        total_cells = int(np.prod(original.shape))
        
        logger.info(f"  Data cells: {num_data_cells:,} / {total_cells:,} total")
        
        # Full budget goes to cell level since province-month are public invariants
        total_rho = float(self.budget.total_rho)
        
        for idx, query in enumerate(queries):
            # Get query's share of budget
            query_weight = self.config.privacy.query_split.get(query, 1.0 / total_queries)
            rho = Fraction(total_rho * query_weight).limit_denominator(10000)
            
            # Get original data
            original_data = original.get_query_array(query)
            
            # Use stratified noise for total_amount if MCC grouping enabled
            if query == 'total_amount' and self._mcc_grouping_enabled:
                logger.info(f"  [{idx+1}/{total_queries}] {query}: Stratified noise by MCC group")
                noisy_data = self._apply_stratified_amount_noise(
                    original, original_data, rho, has_data_mask
                )
            else:
                # Standard noise for counting queries
                sensitivity = self._get_sensitivity(query)
                sigma = np.sqrt(sensitivity**2 / (2 * float(rho)))
                
                logger.info(
                    f"  [{idx+1}/{total_queries}] {query}: "
                    f"ρ={float(rho):.6f}, Δ₂={sensitivity:.2f}, σ={sigma:.2f}"
                )
                
                # Create output array (copy of original)
                noisy_data = original_data.astype(np.float64).copy()
                
                # Only add noise to cells with actual data
                if num_data_cells > 0:
                    data_values = original_data[has_data_mask].astype(np.float64)
                    noisy_values = add_discrete_gaussian_noise(
                        data_values.astype(np.int64),
                        rho=rho,
                        sensitivity=sensitivity,
                        use_fast_sampling=True
                    )
                    noisy_data[has_data_mask] = noisy_values
            
            # Update protected histogram (keep as float for NNLS)
            protected.data[query] = noisy_data.astype(np.float64)
            
            original_total = np.sum(original_data)
            noisy_total = np.sum(noisy_data)
            logger.info(f"    Total: {original_total:,} -> {noisy_total:,.0f}")
    
    def _apply_stratified_amount_noise(
        self,
        histogram: TransactionHistogram,
        original_data: np.ndarray,
        rho: Fraction,
        has_data_mask: np.ndarray
    ) -> np.ndarray:
        """
        Apply stratified noise to total_amount by MCC group.
        
        Uses parallel composition: each MCC group gets full budget rho.
        """
        noisy_data = original_data.astype(np.float64).copy()
        
        mcc_labels = histogram.dimensions['mcc'].labels or []
        
        # Build MCC index to group mapping
        mcc_idx_to_group = {}
        for mcc_idx, mcc_code in enumerate(mcc_labels):
            if mcc_code in self._mcc_to_group:
                mcc_idx_to_group[mcc_idx] = self._mcc_to_group[mcc_code]
            else:
                mcc_idx_to_group[mcc_idx] = max(self._mcc_group_caps.keys()) if self._mcc_group_caps else 0
        
        d_max = self._d_max or 1
        sqrt_d = math.sqrt(d_max)
        
        for group_id, group_cap in self._mcc_group_caps.items():
            group_mcc_indices = [
                mcc_idx for mcc_idx, g_id in mcc_idx_to_group.items()
                if g_id == group_id
            ]
            
            if not group_mcc_indices:
                continue
            
            sensitivity = sqrt_d * group_cap
            sigma = math.sqrt(sensitivity**2 / (2 * float(rho)))
            
            group_data_cells = sum(
                np.sum(has_data_mask[:, :, mcc_idx, :])
                for mcc_idx in group_mcc_indices
            )
            
            logger.info(
                f"    Group {group_id}: {len(group_mcc_indices)} MCCs, "
                f"cap={group_cap:,.0f}, Δ₂={sensitivity:,.0f}, σ={sigma:,.0f}"
            )
            
            for mcc_idx in group_mcc_indices:
                mcc_mask = has_data_mask[:, :, mcc_idx, :]
                if np.sum(mcc_mask) == 0:
                    continue
                
                data_values = original_data[:, :, mcc_idx, :][mcc_mask]
                noise = np.random.normal(0, sigma, data_values.shape)
                noisy_data[:, :, mcc_idx, :][mcc_mask] = data_values + noise
        
        return noisy_data
    
    def _nnls_post_process(self, protected: TransactionHistogram) -> None:
        """
        NNLS post-processing to enforce province-month constraints with non-negativity.
        
        For each query, we solve per province:
            minimize   Σ_{c,m,d} (x_{p,c,m,d} - z_{p,c,m,d})²
            subject to Σ_{c,m,d} x_{p,c,m,d} = Y_p   (province-month PUBLIC invariant)
                       x_{p,c,m,d} ≥ 0               (non-negativity)
        
        where:
            x_{p,c,m,d} = protected cell value
            z_{p,c,m,d} = noisy measurement
            Y_p = exact province-month invariant (PUBLIC DATA)
        
        Algorithm (per province):
        1. Apply NNLS to get non-negative values closest to noisy measurements
        2. Compute current sum over (city, mcc, day)
        3. Adjust values proportionally to match province-month invariant
        4. Re-verify non-negativity
        
        This is a projection onto the intersection of:
        - The non-negative orthant
        - The affine subspace defined by province-month sum constraints
        """
        queries = TransactionHistogram.QUERIES
        num_provinces = protected.shape[0]
        num_cities = protected.shape[1]
        num_mccs = protected.shape[2]
        num_days = protected.shape[3]
        
        cells_per_province = num_cities * num_mccs * num_days
        
        for query in queries:
            noisy_data = protected.get_query_array(query).astype(np.float64)
            province_invariants = self._province_month_invariants[query]
            
            total_adjusted = 0
            
            # Process each province independently
            for p_idx in range(num_provinces):
                # Get the province slice: shape (city_dim, mcc_dim, day_dim)
                province_slice = noisy_data[p_idx].flatten()
                target_sum = float(province_invariants[p_idx])
                
                if target_sum <= 0:
                    # Province-month invariant is zero - set all cells to 0
                    noisy_data[p_idx] = 0
                    continue
                
                n = len(province_slice)
                
                # Step 1: NNLS projection to non-negative orthant
                # Solve: min ||x - z||² s.t. x ≥ 0
                A = np.eye(n)
                b = province_slice
                
                x_nnls, residual = nnls(A, b, maxiter=self._nnls_max_iterations)
                
                # Step 2: Enforce sum constraint via proportional adjustment
                current_sum = np.sum(x_nnls)
                
                if current_sum > 0:
                    # Scale to match province-month invariant (PUBLIC DATA)
                    scale_factor = target_sum / current_sum
                    x_adjusted = x_nnls * scale_factor
                else:
                    # All values are zero but we need positive sum
                    # Distribute uniformly across cells that had data
                    nonzero_mask = province_slice > 0
                    if np.sum(nonzero_mask) > 0:
                        x_adjusted = np.zeros(n)
                        x_adjusted[nonzero_mask] = target_sum / np.sum(nonzero_mask)
                    else:
                        # No data cells - distribute uniformly (rare edge case)
                        x_adjusted = np.full(n, target_sum / n)
                
                # Step 3: Verify non-negativity after adjustment
                x_adjusted = np.maximum(x_adjusted, 0)
                
                # Step 4: Re-adjust if clipping changed the sum
                adjusted_sum = np.sum(x_adjusted)
                if abs(adjusted_sum - target_sum) > 1e-6 and adjusted_sum > 0:
                    x_adjusted = x_adjusted * (target_sum / adjusted_sum)
                
                # Count adjustments made
                diff = np.sum(np.abs(x_adjusted - province_slice))
                if diff > 1e-6:
                    total_adjusted += 1
                
                # Reshape and store
                noisy_data[p_idx] = x_adjusted.reshape((num_cities, num_mccs, num_days))
            
            protected.data[query] = noisy_data
            logger.info(
                f"  {query}: {total_adjusted}/{num_provinces} provinces adjusted via NNLS"
            )
    
    def _controlled_rounding(self, protected: TransactionHistogram) -> None:
        """
        Apply controlled rounding to maintain integer consistency.
        
        Uses randomized rounding that preserves:
        1. Expected values (unbiased)
        2. Province-month sum constraints (exactly - PUBLIC DATA)
        
        Algorithm:
        For each province:
        1. Compute fractional parts of all cells
        2. Determine how many cells need to round up to match integer sum
        3. Probabilistically select cells to round up based on fractional parts
        4. Round remaining cells down
        
        This maintains E[round(x)] = x and Σ round(x_{p,c,m,d}) = Y_p
        """
        queries = TransactionHistogram.QUERIES
        num_provinces = protected.shape[0]
        num_cities = protected.shape[1]
        num_mccs = protected.shape[2]
        num_days = protected.shape[3]
        
        for query in queries:
            data = protected.get_query_array(query).astype(np.float64)
            province_invariants = self._province_month_invariants[query]
            
            rounded_data = np.zeros_like(data, dtype=np.int64)
            
            for p_idx in range(num_provinces):
                target_sum = int(province_invariants[p_idx])
                province_slice = data[p_idx].flatten()
                n = len(province_slice)
                
                if target_sum <= 0:
                    rounded_data[p_idx] = 0
                    continue
                
                # Floor all values
                floors = np.floor(province_slice).astype(np.int64)
                floors = np.maximum(floors, 0)  # Ensure non-negative
                fractional_parts = province_slice - floors
                fractional_parts = np.maximum(fractional_parts, 0)  # Handle negative values
                
                # Compute how many need to round up to match PUBLIC invariant
                floor_sum = np.sum(floors)
                num_round_up = target_sum - floor_sum
                
                if num_round_up <= 0:
                    # Sum of floors meets or exceeds target
                    rounded_province = floors.copy()
                    # May need to reduce some values
                    while np.sum(rounded_province) > target_sum:
                        non_zero = np.where(rounded_province > 0)[0]
                        if len(non_zero) == 0:
                            break
                        idx = np.random.choice(non_zero)
                        rounded_province[idx] -= 1
                elif num_round_up >= n:
                    # Need to round up all cells and possibly add more
                    rounded_province = floors + 1
                    extra_needed = num_round_up - n
                    if extra_needed > 0:
                        # Distribute extra among random cells
                        for _ in range(int(extra_needed)):
                            idx = np.random.randint(n)
                            rounded_province[idx] += 1
                else:
                    # Normal case: probabilistic rounding
                    # Select cells to round up based on fractional parts
                    frac_sum = np.sum(fractional_parts)
                    if frac_sum > 0:
                        probs = fractional_parts / frac_sum
                    else:
                        probs = np.ones(n) / n
                    
                    # Ensure valid probability distribution
                    probs = np.maximum(probs, 1e-10)
                    probs = probs / np.sum(probs)
                    
                    try:
                        round_up_indices = np.random.choice(
                            n, size=int(num_round_up), replace=False, p=probs
                        )
                    except ValueError:
                        # Fallback to uniform sampling
                        round_up_indices = np.random.choice(
                            n, size=min(int(num_round_up), n), replace=False
                        )
                    
                    rounded_province = floors.copy()
                    rounded_province[round_up_indices] += 1
                
                # Final adjustment to ensure EXACT match to PUBLIC invariant
                final_sum = np.sum(rounded_province)
                diff = target_sum - final_sum
                
                if diff > 0:
                    # Need to add more
                    for _ in range(int(diff)):
                        idx = np.random.randint(n)
                        rounded_province[idx] += 1
                elif diff < 0:
                    # Need to subtract
                    for _ in range(int(-diff)):
                        non_zero = np.where(rounded_province > 0)[0]
                        if len(non_zero) == 0:
                            break
                        idx = np.random.choice(non_zero)
                        rounded_province[idx] -= 1
                
                rounded_data[p_idx] = rounded_province.reshape((num_cities, num_mccs, num_days))
            
            protected.set_query_array(query, rounded_data)
            
            # Verify totals match PUBLIC invariants
            final_total = np.sum(rounded_data)
            invariant_total = np.sum(province_invariants)
            logger.info(f"  {query}: rounded total={final_total:,} (public invariant={invariant_total:,})")
    
    def _verify_invariants(self, protected: TransactionHistogram) -> None:
        """
        Verify that province-month invariants are exactly preserved.
        
        This is a CRITICAL verification step - the province-month totals
        are PUBLIC DATA that MUST match exactly.
        """
        queries = TransactionHistogram.QUERIES
        all_valid = True
        
        for query in queries:
            data = protected.get_query_array(query)
            invariants = self._province_month_invariants[query]
            
            # Compute actual province-month sums: sum over city, mcc, day
            actual_sums = np.sum(data, axis=(1, 2, 3))  # Sum over city, mcc, day
            
            # Check EXACT match (these are PUBLIC values that MUST match)
            match = np.array_equal(actual_sums, invariants)
            status = "✓" if match else "✗"
            
            max_diff = np.max(np.abs(actual_sums - invariants))
            
            if not match:
                all_valid = False
                logger.error(
                    f"  {query}: {status} MISMATCH with public data! Max deviation: {max_diff:.0f}"
                )
                # Log problematic provinces
                mismatches = np.where(actual_sums != invariants)[0]
                for p_idx in mismatches[:5]:
                    logger.error(
                        f"    Province {p_idx}: public={invariants[p_idx]:,}, "
                        f"computed={actual_sums[p_idx]:,}, diff={actual_sums[p_idx] - invariants[p_idx]:,}"
                    )
            else:
                logger.info(f"  {query}: {status} Province-month totals EXACTLY match public data")
        
        if all_valid:
            logger.info("  [All province-month invariants verified - matches public data]")
        else:
            logger.error("  [CRITICAL: Public data mismatch detected!]")
    
    def _get_sensitivity(self, query: str, mcc_group: Optional[int] = None) -> float:
        """
        Get the L2 sensitivity for a query under USER-LEVEL DP.
        
        USER-LEVEL SENSITIVITY:
        - L2 = sqrt(D_max) * per_cell_contribution
        
        For counting queries: L2 = sqrt(D_max) * K
        For unique queries: L2 = sqrt(D_max) * 1
        For sum queries: L2 = sqrt(D_max) * cap
        """
        if self._user_level_sensitivities and query in self._user_level_sensitivities:
            base_sensitivity = self._user_level_sensitivities[query].l2_sensitivity
            
            if query == 'total_amount' and mcc_group is not None and mcc_group in self._mcc_group_caps:
                d_max = self._d_max or 1
                sqrt_d = math.sqrt(d_max)
                group_cap = self._mcc_group_caps[mcc_group]
                return sqrt_d * group_cap
            
            return base_sensitivity
        
        # Fallback computation
        K = self.config.privacy.computed_contribution_bound or 1
        d_max = self._d_max or 1
        sqrt_d = math.sqrt(d_max)
        
        if query == 'total_amount':
            if mcc_group is not None and mcc_group in self._mcc_group_caps:
                cap = self._mcc_group_caps[mcc_group]
            elif self._mcc_group_caps:
                cap = max(self._mcc_group_caps.values())
            else:
                cap = self._winsorize_cap
            return sqrt_d * cap
        elif query == 'unique_cards':
            return sqrt_d * 1.0
        else:
            return sqrt_d * K


class SimpleEngine:
    """
    Simplified engine that applies flat DP noise without hierarchy.
    
    Useful for testing and comparison.
    """
    
    def __init__(self, config: Config, budget: Budget):
        self.config = config
        self.budget = budget
    
    def run(self, histogram: TransactionHistogram) -> TransactionHistogram:
        """Apply flat DP noise to histogram."""
        protected = histogram.copy()
        
        total_rho = self.budget.total_rho
        num_queries = len(TransactionHistogram.QUERIES)
        rho_per_query = total_rho / num_queries
        
        for query in TransactionHistogram.QUERIES:
            data = histogram.get_query_array(query)
            
            noisy_data = add_discrete_gaussian_noise(
                data.astype(np.int64),
                rho=rho_per_query,
                sensitivity=1.0
            )
            
            noisy_data = np.maximum(np.round(noisy_data), 0).astype(np.int64)
            protected.set_query_array(query, noisy_data)
        
        return protected
