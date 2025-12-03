"""
Top-Down DP Engine with User-Level Differential Privacy.

Implements the top-down mechanism for applying differential privacy
to the geographic hierarchy: Province -> City.

KEY CHANGE: Uses USER-LEVEL DP (per card) not EVENT-LEVEL DP (per transaction).

User-Level DP means:
- Privacy unit is the CARD (user), not individual transactions
- Removing a card can affect up to D distinct cells (where D = max cells per card)
- L2 sensitivity = sqrt(D) * K where K = max transactions per card per cell
- This provides stronger privacy but requires more noise

The top-down approach:
1. Add noise at province level (using user-level sensitivity)
2. Add noise at city level (using user-level sensitivity)
3. Ensure consistency between levels through post-processing
"""

import math
import logging
from typing import Dict, Optional, Any
from fractions import Fraction

import numpy as np
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
    Top-down differential privacy engine with USER-LEVEL DP.
    
    Applies DP noise hierarchically:
    1. Province level: Aggregate cities, add noise
    2. City level: Add noise, post-process to be consistent with province totals
    
    Uses zCDP (zero-Concentrated DP) with Discrete Gaussian mechanism.
    
    USER-LEVEL DP:
    - Privacy unit is the CARD, not individual transactions
    - Sensitivity accounts for cards appearing in multiple cells
    - L2 sensitivity = sqrt(D_max) * K where:
      - D_max = max distinct cells any card appears in
      - K = max transactions per card per cell (bounded contribution)
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
        
        # Store noisy aggregates for consistency
        self._province_aggregates: Dict[str, np.ndarray] = {}
        
        # MCC group information for stratified sensitivity
        self._mcc_group_caps = config.privacy.mcc_group_caps or {}
        self._mcc_to_group = config.privacy.mcc_to_group or {}
        self._mcc_grouping_enabled = config.privacy.mcc_grouping_enabled and bool(self._mcc_group_caps)
        
        # User-level sensitivity parameters
        # D_max: Maximum number of distinct cells any card appears in
        # This MUST be computed from data before running the engine
        self._d_max: Optional[int] = None
        self._user_level_sensitivities: Dict[str, UserLevelSensitivity] = {}
        
        # Winsorize cap for amount queries
        self._winsorize_cap: float = 1.0
    
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
            "unique_acceptors": UserLevelSensitivity(
                query_name="unique_acceptors",
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
        Apply top-down DP to the histogram with USER-LEVEL DP.
        
        Args:
            histogram: Original histogram with true counts
            
        Returns:
            New histogram with DP-protected values
        """
        logger.info("=" * 60)
        logger.info("Starting Top-Down DP Processing (User-Level DP)")
        logger.info("=" * 60)
        logger.info(f"Total privacy budget (rho): {self.budget.total_rho}")
        logger.info(f"Epsilon at delta={self.budget.delta}: {self.budget.total_epsilon:.4f}")
        
        # Verify user-level parameters are set
        if self._d_max is None:
            logger.warning(
                "WARNING: User-level DP parameters not set! "
                "Using fallback K-only sensitivity which may violate privacy. "
                "Call set_user_level_params() before running."
            )
            # Try to use config values as fallback
            K = self.config.privacy.computed_contribution_bound or 1
            # Assume D_max = 1 (UNSAFE but allows backward compatibility)
            self._d_max = 1
            self._winsorize_cap = 1.0
            self.set_user_level_params(
                d_max=1,
                k_bound=K,
                winsorize_cap=self._winsorize_cap
            )
        else:
            logger.info(f"User-level D_max: {self._d_max}")
            logger.info(f"sqrt(D_max): {math.sqrt(self._d_max):.4f}")
        
        # Create a copy for the protected result
        protected = histogram.copy()
        
        # Step 1: Province-level noise
        logger.info("")
        logger.info("=" * 40)
        logger.info("Step 1: Province-level noise injection")
        logger.info("=" * 40)
        self._apply_province_level_noise(histogram, protected)
        
        # Step 2: City-level noise
        logger.info("")
        logger.info("=" * 40)
        logger.info("Step 2: City-level noise injection")
        logger.info("=" * 40)
        self._apply_city_level_noise(histogram, protected)
        
        # Step 3: Post-processing for non-negativity
        logger.info("")
        logger.info("=" * 40)
        logger.info("Step 3: Post-processing")
        logger.info("=" * 40)
        self._post_process(protected)
        
        logger.info("")
        logger.info("=" * 60)
        logger.info("Top-Down DP Processing Complete")
        logger.info("=" * 60)
        
        return protected
    
    def _apply_province_level_noise(
        self,
        original: TransactionHistogram,
        protected: TransactionHistogram
    ) -> None:
        """
        Apply noise at province level.
        
        Aggregates data to province-day level and adds Discrete Gaussian noise.
        """
        queries = TransactionHistogram.QUERIES
        total_queries = len(queries)
        
        for idx, query in enumerate(queries):
            # Get budget for this query at province level
            rho = self.budget.get_query_budget(query, 'province')
            
            # Aggregate to province level
            # Shape: (province_dim,) after summing over cities, mccs, and days
            province_data = original.aggregate_to_province(query)
            array_size = int(np.prod(province_data.shape))
            
            # Use global sensitivity (no per-group stratification at province level)
            sensitivity = self._get_sensitivity(query)
            sigma = np.sqrt(sensitivity**2 / (2 * float(rho)))
            
            logger.info(f"  [{idx+1}/{total_queries}] {query}: rho={float(rho):.6f}, sensitivity={sensitivity:.2f}, sigma={sigma:.2f}, cells={array_size:,}")
            
            # Add noise (province level is small, can use exact or fast)
            noisy_province = add_discrete_gaussian_noise(
                province_data.astype(np.int64),
                rho=rho,
                sensitivity=sensitivity,
                use_fast_sampling=True
            )
            
            # Store for consistency enforcement
            self._province_aggregates[query] = noisy_province
            
            logger.info(f"    Done. Province totals: {np.sum(province_data):,} -> {np.sum(noisy_province):,}")
    
    def _apply_city_level_noise(
        self,
        original: TransactionHistogram,
        protected: TransactionHistogram
    ) -> None:
        """
        Apply noise at city level and update protected histogram.
        
        Adds Discrete Gaussian noise at the finest granularity.
        Uses fast NumPy-based sampling for performance.
        
        For total_amount with MCC grouping enabled, uses stratified noise
        with per-group sensitivity (parallel composition).
        """
        queries = TransactionHistogram.QUERIES
        total_queries = len(queries)
        
        for idx, query in enumerate(queries):
            # Get budget for this query at city level
            rho = self.budget.get_query_budget(query, 'city')
            
            # Get original data
            original_data = original.get_query_array(query)
            array_size = int(np.prod(original_data.shape))
            
            # Use stratified noise for total_amount if MCC grouping enabled
            if query == 'total_amount' and self._mcc_grouping_enabled:
                logger.info(f"  [{idx+1}/{total_queries}] {query}: Using stratified noise by MCC group")
                noisy_data = self._apply_stratified_amount_noise(
                    original, original_data, rho
                )
            else:
                # Standard noise for counting queries
                sensitivity = self._get_sensitivity(query)
                sigma = np.sqrt(sensitivity**2 / (2 * float(rho)))
                
                logger.info(f"  [{idx+1}/{total_queries}] {query}: rho={float(rho):.6f}, sensitivity={sensitivity:.2f}, sigma={sigma:.2f}, cells={array_size:,}")
                
                noisy_data = add_discrete_gaussian_noise(
                    original_data.astype(np.int64),
                    rho=rho,
                    sensitivity=sensitivity,
                    use_fast_sampling=True
                )
            
            # Update protected histogram
            protected.set_query_array(query, noisy_data)
            
            original_total = np.sum(original_data)
            noisy_total = np.sum(noisy_data)
            logger.info(f"    Done. Total: {original_total:,} -> {noisy_total:,}")
    
    def _apply_stratified_amount_noise(
        self,
        histogram: TransactionHistogram,
        original_data: np.ndarray,
        rho: Fraction
    ) -> np.ndarray:
        """
        Apply stratified noise to total_amount by MCC group with USER-LEVEL DP.
        
        Each MCC group gets noise scaled to its own sensitivity.
        Uses parallel composition - each group uses the full budget rho
        since MCC groups are disjoint.
        
        USER-LEVEL SENSITIVITY for each group:
        L2 = sqrt(D_max) * group_cap
        
        Args:
            histogram: Original histogram for MCC label lookup
            original_data: Original amount data array (shape: province, city, mcc, day)
            rho: Privacy budget (same for all groups under parallel composition)
            
        Returns:
            Noisy data array
        """
        noisy_data = original_data.astype(np.float64).copy()
        
        # Get MCC labels from histogram
        mcc_labels = histogram.dimensions['mcc'].labels or []
        
        # Build MCC index to group mapping
        mcc_idx_to_group = {}
        for mcc_idx, mcc_code in enumerate(mcc_labels):
            if mcc_code in self._mcc_to_group:
                mcc_idx_to_group[mcc_idx] = self._mcc_to_group[mcc_code]
            else:
                # Unknown MCC - assign to highest group (most conservative)
                mcc_idx_to_group[mcc_idx] = max(self._mcc_group_caps.keys()) if self._mcc_group_caps else 0
        
        # Get D_max for user-level sensitivity
        d_max = self._d_max or 1
        sqrt_d = math.sqrt(d_max)
        
        # Process each MCC group separately (parallel composition)
        for group_id, group_cap in self._mcc_group_caps.items():
            # Find MCC indices in this group
            group_mcc_indices = [
                mcc_idx for mcc_idx, g_id in mcc_idx_to_group.items()
                if g_id == group_id
            ]
            
            if not group_mcc_indices:
                continue
            
            # USER-LEVEL SENSITIVITY: sqrt(D_max) * group_cap
            # This accounts for one card appearing in up to D_max cells
            sensitivity = sqrt_d * group_cap
            sigma = math.sqrt(sensitivity**2 / (2 * float(rho)))
            
            logger.info(
                f"    Group {group_id}: {len(group_mcc_indices)} MCCs, "
                f"cap={group_cap:,.0f}, L2=sqrt({d_max})*{group_cap:,.0f}={sensitivity:,.0f}, "
                f"sigma={sigma:,.0f}"
            )
            
            # Extract data for this group's MCCs
            # Data shape: (province, city, mcc, day)
            for mcc_idx in group_mcc_indices:
                # Get slice for this MCC
                group_data = original_data[:, :, mcc_idx, :].copy()
                
                # Add noise with group-specific sigma (user-level)
                noise = np.random.normal(0, sigma, group_data.shape)
                noisy_group = group_data + noise
                
                # Store back
                noisy_data[:, :, mcc_idx, :] = noisy_group
        
        return noisy_data.astype(np.int64)
    
    def _get_sensitivity(self, query: str, mcc_group: Optional[int] = None) -> float:
        """
        Get the L2 sensitivity for a query under USER-LEVEL DP.
        
        USER-LEVEL SENSITIVITY:
        - L2 = sqrt(D_max) * per_cell_contribution
        - This accounts for one card appearing in up to D_max cells
        
        For counting queries: L2 = sqrt(D_max) * K
        For unique queries: L2 = sqrt(D_max) * 1
        For sum queries: L2 = sqrt(D_max) * cap
        
        Args:
            query: Query name
            mcc_group: Optional MCC group ID for stratified sensitivity
            
        Returns:
            L2 sensitivity value for user-level DP
        """
        # Check if user-level sensitivities have been set
        if self._user_level_sensitivities and query in self._user_level_sensitivities:
            base_sensitivity = self._user_level_sensitivities[query].l2_sensitivity
            
            # For total_amount with MCC grouping, adjust the cap component
            if query == 'total_amount' and mcc_group is not None and mcc_group in self._mcc_group_caps:
                # Recalculate with per-group cap
                d_max = self._d_max or 1
                sqrt_d = math.sqrt(d_max)
                group_cap = self._mcc_group_caps[mcc_group]
                return sqrt_d * group_cap
            
            return base_sensitivity
        
        # Fallback: compute sensitivity from config (backward compatibility)
        # This path should NOT be used in production - always set user-level params
        K = self.config.privacy.computed_contribution_bound or 1
        d_max = self._d_max or 1  # Use D_max if set, else assume 1 (UNSAFE)
        sqrt_d = math.sqrt(d_max)
        
        if d_max == 1:
            logger.warning(
                f"D_max not set! Using D_max=1 which UNDERESTIMATES sensitivity. "
                f"Call set_user_level_params() before running the engine."
            )
        
        if query == 'total_amount':
            # For sum of amounts, sensitivity is sqrt(D_max) × cap
            # The cap should be in the same units as the amounts (not normalized to [0,1])
            # Amounts are winsorized but NOT normalized, so cap is in original units (e.g., dollars)
            if mcc_group is not None and mcc_group in self._mcc_group_caps:
                cap = self._mcc_group_caps[mcc_group]
            elif self._mcc_group_caps:
                cap = max(self._mcc_group_caps.values())
            else:
                # Use winsorize cap from set_user_level_params()
                cap = self._winsorize_cap
                if cap == 1.0:
                    # This should not happen in production - winsorize_cap should be set correctly
                    # Log error as this indicates a configuration issue
                    logger.error(
                        f"ERROR: Using default winsorize_cap=1.0 for total_amount sensitivity. "
                        f"This assumes amounts are normalized to [0,1], but amounts are in original units. "
                        f"This will UNDERESTIMATE sensitivity and VIOLATE PRIVACY. "
                        f"Ensure set_user_level_params() is called with correct winsorize_cap from preprocessor."
                    )
            
            sensitivity = sqrt_d * cap
            logger.debug(f"total_amount sensitivity: sqrt({d_max}) * {cap} = {sensitivity:.4f}")
            return sensitivity
            
        elif query in ('unique_cards', 'unique_acceptors'):
            # Unique queries: each cell changes by at most 1
            sensitivity = sqrt_d * 1.0
            logger.debug(f"{query} sensitivity: sqrt({d_max}) * 1 = {sensitivity:.4f}")
            return sensitivity
            
        else:
            # Counting queries: each cell changes by at most K
            sensitivity = sqrt_d * K
            logger.debug(f"{query} sensitivity: sqrt({d_max}) * {K} = {sensitivity:.4f}")
            return sensitivity
    
    def _post_process(self, protected: TransactionHistogram) -> None:
        """
        Post-process protected histogram.
        
        Operations:
        1. Round to integers
        2. Enforce non-negativity (set negative values to 0)
        3. Optionally enforce consistency between levels
        """
        queries = TransactionHistogram.QUERIES
        
        for query in queries:
            data = protected.get_query_array(query)
            
            # Count negative values before
            negative_count = np.sum(data < 0)
            
            # Round and enforce non-negativity
            data = np.round(data).astype(np.int64)
            data = np.maximum(data, 0)
            
            protected.set_query_array(query, data)
            
            if negative_count > 0:
                logger.info(f"  {query}: {negative_count:,} negative values set to 0")
    
    def _enforce_consistency(
        self,
        protected: TransactionHistogram,
        query: str
    ) -> None:
        """
        Enforce consistency between city-level and province-level aggregates.
        
        Uses simple proportional adjustment to make city sums match province totals.
        
        This is optional and may not preserve DP guarantees strictly.
        """
        if query not in self._province_aggregates:
            return
        
        province_targets = self._province_aggregates[query]
        data = protected.get_query_array(query)
        
        num_provinces = data.shape[0]
        
        for p_idx in range(num_provinces):
            # Current sum for this province (over all cities, mccs, days)
            current_sum = np.sum(data[p_idx])
            
            if current_sum == 0:
                continue
            
            # Target from province-level noise
            target_sum = np.sum(province_targets[p_idx])
            
            if target_sum <= 0:
                # Province total is zero or negative, set all to 0
                data[p_idx] = 0
            else:
                # Scale proportionally
                scale_factor = target_sum / current_sum
                data[p_idx] = np.round(data[p_idx] * scale_factor).astype(np.int64)
                data[p_idx] = np.maximum(data[p_idx], 0)
        
        protected.set_query_array(query, data)


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
        
        # Split budget equally among queries
        rho_per_query = total_rho / num_queries
        
        for query in TransactionHistogram.QUERIES:
            data = histogram.get_query_array(query)
            
            noisy_data = add_discrete_gaussian_noise(
                data.astype(np.int64),
                rho=rho_per_query,
                sensitivity=1.0
            )
            
            # Post-process: non-negativity
            noisy_data = np.maximum(np.round(noisy_data), 0).astype(np.int64)
            
            protected.set_query_array(query, noisy_data)
        
        return protected

