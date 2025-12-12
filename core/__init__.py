"""
Transaction SDC System
======================
Statistical Disclosure Control system for transaction data.

Uses context-aware plausibility-based noise with:
- Multiplicative jitter that preserves ratios naturally
- Data-driven bounds per (MCC, City, Weekday) context
- Province count invariants maintained exactly

Supports:
- Geographic hierarchy: Province -> City
- Time hierarchy: Month -> Day -> Weekday
- Queries: transaction count, unique cards, total amount

NOTE: DP modules (budget.py, primitives.py, sensitivity.py, stratification.py, 
mcc_groups.py) are kept for potential future use but are not currently active.
"""

__version__ = "3.0.0"
__author__ = "Transaction SDC Team"

# Core exports (active)
from .config import Config, PrivacyConfig, DataConfig, SparkConfig
from .pipeline import DPPipeline, PipelineResult

# Legacy DP exports (kept for future use, not currently active)
# Uncomment if formal DP is needed:
# from .budget import Budget, BudgetAllocation, BudgetAllocator, MonthlyBudgetManager, BudgetVerifier, CompositionType
# from .sensitivity import GlobalSensitivityCalculator, UserLevelSensitivity, StratumSensitivity, StratumSensitivityCalculator, SensitivityBudgetOptimizer
# from .stratification import CityTier, DayType, CityTierClassifier, DayTypeClassifier, StratumManager, Stratum

__all__ = [
    # Config
    "Config", "PrivacyConfig", "DataConfig", "SparkConfig",
    # Pipeline
    "DPPipeline", "PipelineResult",
]

