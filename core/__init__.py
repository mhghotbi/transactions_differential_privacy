"""
Transaction DP System
=====================
A differential privacy system for transaction data using zCDP.

Supports:
- Geographic hierarchy: Province -> City
- Time hierarchy: Month -> Day
- Queries: transaction count, unique cards, unique acceptors, total amount

Key Features:
- User-Level DP: Privacy unit is the card (user), not individual transactions
- Stratified Noise: Different noise calibration by (MCC, city_tier, day_type)
- Monthly Budget Management: Proper composition for periodic releases
"""

__version__ = "2.0.0"
__author__ = "Transaction DP Team"

# Core exports
from .config import Config, PrivacyConfig, DataConfig, SparkConfig
from .budget import (
    Budget, 
    BudgetAllocation, 
    BudgetAllocator, 
    MonthlyBudgetManager,
    BudgetVerifier,
    CompositionType
)
from .sensitivity import (
    GlobalSensitivityCalculator,
    UserLevelSensitivity,
    StratumSensitivity,
    StratumSensitivityCalculator,
    SensitivityBudgetOptimizer
)
from .stratification import (
    CityTier,
    DayType,
    CityTierClassifier,
    DayTypeClassifier,
    StratumManager,
    Stratum
)
from .pipeline import DPPipeline, PipelineResult

__all__ = [
    # Config
    "Config", "PrivacyConfig", "DataConfig", "SparkConfig",
    # Budget
    "Budget", "BudgetAllocation", "BudgetAllocator", 
    "MonthlyBudgetManager", "BudgetVerifier", "CompositionType",
    # Sensitivity
    "GlobalSensitivityCalculator", "UserLevelSensitivity",
    "StratumSensitivity", "StratumSensitivityCalculator", "SensitivityBudgetOptimizer",
    # Stratification
    "CityTier", "DayType", "CityTierClassifier", "DayTypeClassifier",
    "StratumManager", "Stratum",
    # Pipeline
    "DPPipeline", "PipelineResult",
]

