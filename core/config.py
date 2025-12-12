"""
Configuration management for Transaction DP System.
Handles loading, validation, and access to configuration parameters.
"""

import configparser
import os
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
from pathlib import Path
from fractions import Fraction


logger = logging.getLogger(__name__)


@dataclass
class PrivacyConfig:
    """Statistical Disclosure Control configuration."""
    
    # Bounded contribution settings
    contribution_bound_method: str = "transaction_weighted_percentile"  # 'transaction_weighted_percentile', 'iqr', 'percentile', or 'fixed'
    contribution_bound_iqr_multiplier: float = 1.5
    contribution_bound_fixed: int = 5
    contribution_bound_percentile: float = 99.0
    contribution_bound_per_group: bool = True  # Compute K per MCC for memory efficiency (large datasets)
    
    # Computed K value (set after analysis)
    computed_contribution_bound: Optional[int] = None
    
    # Computed D_max (max cells per card)
    computed_d_max: Optional[int] = None
    
    # Suppression settings
    suppression_threshold: int = 5  # Suppress cells with noisy count < threshold
    suppression_method: str = "flag"  # 'flag', 'null', or 'value'
    suppression_sentinel: int = -1  # Sentinel value for suppressed cells (if method='value')
    
    # Per-MCC winsorization settings
    mcc_cap_percentile: float = 99.0  # Percentile for per-MCC winsorization caps
    
    # Computed per-MCC caps (set during preprocessing)
    mcc_caps: Optional[Dict[str, float]] = None  # mcc_code -> winsorization_cap
    
    # Utility-focused noise parameters (SDC)
    noise_level: float = 0.15  # Relative noise level (15% std for count)
    noise_seed: int = 42  # Random seed for reproducible noise generation
    min_noise_factor_deviation: float = 0.01  # Minimum noise factor deviation from 1.0 (0.01 = 1%, 0.0 = disabled)
    
    # Differential Privacy parameters (for formal DP implementations)
    total_rho: Fraction = field(default_factory=lambda: Fraction(1, 4))  # Total privacy budget in zCDP (rho parameter)
    delta: float = 1e-10  # Delta parameter for (epsilon, delta)-DP conversion
    geographic_split: Dict[str, float] = field(default_factory=lambda: {"province": 0.2, "city": 0.8})  # Budget split across geographic levels
    query_split: Dict[str, float] = field(default_factory=lambda: {"transaction_count": 0.34, "unique_cards": 0.33, "total_amount": 0.33})  # Budget split across query types
    
    # Additional noise parameters (for derived statistics)
    cards_jitter: float = 0.05  # Jitter level for unique_cards (5%)
    amount_jitter: float = 0.05  # Jitter level for total_amount (5%)
    
    # Sensitivity calculation settings
    sensitivity_method: str = "global"  # 'global' or 'local' - global accounts for multi-cell contributions
    fixed_max_cells_per_card: int = 100  # Fixed D_max value (if not computed from data)
    
    # Confidence interval settings
    confidence_levels: List[float] = field(default_factory=lambda: [0.90])  # Confidence levels for intervals (e.g., [0.90, 0.95])
    include_relative_moe: bool = True  # Include relative margin of error in output
    
    def validate(self) -> None:
        """Validate SDC configuration."""
        if self.contribution_bound_method not in ('transaction_weighted_percentile', 'iqr', 'percentile', 'fixed'):
            raise ValueError(f"contribution_bound_method must be 'transaction_weighted_percentile', 'iqr', 'percentile', or 'fixed', got {self.contribution_bound_method}")
        
        if self.contribution_bound_fixed < 1:
            raise ValueError(f"contribution_bound_fixed must be >= 1, got {self.contribution_bound_fixed}")
        
        if self.suppression_threshold < 0:
            raise ValueError(f"suppression_threshold must be >= 0, got {self.suppression_threshold}")
        
        if self.suppression_method not in ('flag', 'null', 'value'):
            raise ValueError(f"suppression_method must be 'flag', 'null', or 'value', got {self.suppression_method}")
        
        if not 0 < self.mcc_cap_percentile <= 100:
            raise ValueError(f"mcc_cap_percentile must be in (0, 100], got {self.mcc_cap_percentile}")
        
        if not 0 <= self.min_noise_factor_deviation < 1.0:
            raise ValueError(f"min_noise_factor_deviation must be in [0, 1), got {self.min_noise_factor_deviation}")
        
        if self.total_rho <= 0:
            raise ValueError(f"total_rho must be > 0, got {self.total_rho}")
        
        if not 0 < self.delta < 1:
            raise ValueError(f"delta must be in (0, 1), got {self.delta}")
        
        # Validate geographic_split and query_split sum to 1.0
        geo_sum = sum(self.geographic_split.values())
        if abs(geo_sum - 1.0) > 1e-6:
            raise ValueError(f"geographic_split must sum to 1.0, got {geo_sum}")
        
        query_sum = sum(self.query_split.values())
        if abs(query_sum - 1.0) > 1e-6:
            raise ValueError(f"query_split must sum to 1.0, got {query_sum}")
        
        # Validate sensitivity_method
        if self.sensitivity_method not in ('global', 'local'):
            raise ValueError(f"sensitivity_method must be 'global' or 'local', got {self.sensitivity_method}")
        
        # Validate confidence_levels
        for level in self.confidence_levels:
            if not 0 < level < 1:
                raise ValueError(f"confidence_levels must be in (0, 1), got {level}")
        
        # Validate jitter levels
        if not 0 <= self.cards_jitter < 1:
            raise ValueError(f"cards_jitter must be in [0, 1), got {self.cards_jitter}")
        if not 0 <= self.amount_jitter < 1:
            raise ValueError(f"amount_jitter must be in [0, 1), got {self.amount_jitter}")
        
        if self.fixed_max_cells_per_card < 1:
            raise ValueError(f"fixed_max_cells_per_card must be >= 1, got {self.fixed_max_cells_per_card}")


@dataclass
class DataConfig:
    """Data-related configuration."""
    input_path: str = ""
    output_path: str = ""
    city_province_path: str = "data/city_province.csv"
    input_format: str = "parquet"  # parquet or csv
    
    # Winsorization settings
    winsorize_percentile: float = 99.0
    winsorize_cap: Optional[float] = None  # If set, use this instead of percentile
    
    # Time settings
    num_days: int = 30
    
    def validate(self) -> None:
        """Validate data configuration."""
        if not self.input_path:
            raise ValueError("input_path must be specified")
        if not self.output_path:
            raise ValueError("output_path must be specified")
        if not 0 < self.winsorize_percentile <= 100:
            raise ValueError(f"winsorize_percentile must be in (0, 100], got {self.winsorize_percentile}")


@dataclass
class SparkConfig:
    """Spark-related configuration."""
    app_name: str = "TransactionDP"
    master: str = "local[*]"
    executor_memory: str = "180g"
    driver_memory: str = "180g"
    shuffle_partitions: int = 200
    
    # Performance optimization: Skip expensive count() operations used only for logging
    # Set to True for production runs on very large datasets (10B+ rows) 
    # This skips record counts that don't affect DP correctness but take 10+ min each
    skip_expensive_counts: bool = False
    
    def to_spark_conf(self) -> Dict[str, str]:
        """Convert to Spark configuration dictionary."""
        return {
            "spark.app.name": self.app_name,
            "spark.executor.memory": self.executor_memory,
            "spark.driver.memory": self.driver_memory,
            "spark.sql.shuffle.partitions": str(self.shuffle_partitions)
        }


@dataclass
class Config:
    """Main configuration container."""
    privacy: PrivacyConfig = field(default_factory=PrivacyConfig)
    data: DataConfig = field(default_factory=DataConfig)
    spark: SparkConfig = field(default_factory=SparkConfig)
    
    # Column mappings (maps internal names to source column names)
    columns: Dict[str, str] = field(default_factory=lambda: {
        "amount": "transaction_amount",
        "transaction_date": "transaction_date",
        "card_number": "card_number",
        "acceptor_id": "acceptorid",
        "acceptor_city": "city",
        "mcc": "mcc"
    })
    
    def validate(self) -> None:
        """Validate entire configuration."""
        self.privacy.validate()
        self.data.validate()
        logger.info("Configuration validated successfully")
    
    @classmethod
    def from_ini(cls, config_path: str) -> "Config":
        """Load configuration from INI file."""
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Config file not found: {config_path}")
        
        parser = configparser.ConfigParser()
        parser.read(config_path, encoding='utf-8')
        
        config = cls()
        
        # Load privacy section (SDC settings)
        if 'privacy' in parser:
            sec = parser['privacy']
            
            # Parse bounded contribution settings
            if 'contribution_bound_method' in sec:
                config.privacy.contribution_bound_method = sec['contribution_bound_method']
            if 'contribution_bound_iqr_multiplier' in sec:
                config.privacy.contribution_bound_iqr_multiplier = float(sec['contribution_bound_iqr_multiplier'])
            if 'contribution_bound_fixed' in sec:
                config.privacy.contribution_bound_fixed = int(sec['contribution_bound_fixed'])
            if 'contribution_bound_percentile' in sec:
                config.privacy.contribution_bound_percentile = float(sec['contribution_bound_percentile'])
            if 'contribution_bound_per_group' in sec:
                config.privacy.contribution_bound_per_group = sec.getboolean('contribution_bound_per_group')
            
            # Parse suppression settings
            if 'suppression_threshold' in sec:
                config.privacy.suppression_threshold = int(sec['suppression_threshold'])
            if 'suppression_method' in sec:
                config.privacy.suppression_method = sec['suppression_method']
            if 'suppression_sentinel' in sec:
                config.privacy.suppression_sentinel = int(sec['suppression_sentinel'])
            
            # Parse per-MCC winsorization settings
            if 'mcc_cap_percentile' in sec:
                config.privacy.mcc_cap_percentile = float(sec['mcc_cap_percentile'])
            
            # Parse utility-focused noise parameters
            if 'noise_level' in sec:
                config.privacy.noise_level = float(sec['noise_level'])
            if 'noise_seed' in sec:
                config.privacy.noise_seed = int(sec['noise_seed'])
            if 'min_noise_factor_deviation' in sec:
                config.privacy.min_noise_factor_deviation = float(sec['min_noise_factor_deviation'])
            
            # Parse Differential Privacy parameters
            if 'total_rho' in sec:
                rho_str = sec['total_rho'].strip()
                # Support both fraction format (1/4) and decimal format (0.25)
                if '/' in rho_str:
                    config.privacy.total_rho = Fraction(rho_str)
                else:
                    config.privacy.total_rho = Fraction(float(rho_str)).limit_denominator(10000)
            if 'delta' in sec:
                config.privacy.delta = float(sec['delta'])
            
            # Parse budget splits (format: "key1:value1,key2:value2")
            if 'geographic_split' in sec:
                split_str = sec['geographic_split'].strip()
                config.privacy.geographic_split = {}
                for item in split_str.split(','):
                    key, value = item.split(':')
                    config.privacy.geographic_split[key.strip()] = float(value.strip())
            
            if 'query_split' in sec:
                split_str = sec['query_split'].strip()
                config.privacy.query_split = {}
                for item in split_str.split(','):
                    key, value = item.split(':')
                    config.privacy.query_split[key.strip()] = float(value.strip())
            
            # Parse additional noise parameters
            if 'cards_jitter' in sec:
                config.privacy.cards_jitter = float(sec['cards_jitter'])
            if 'amount_jitter' in sec:
                config.privacy.amount_jitter = float(sec['amount_jitter'])
            
            # Parse sensitivity settings
            if 'sensitivity_method' in sec:
                config.privacy.sensitivity_method = sec['sensitivity_method']
            if 'fixed_max_cells_per_card' in sec:
                config.privacy.fixed_max_cells_per_card = int(sec['fixed_max_cells_per_card'])
            
            # Parse confidence interval settings
            if 'confidence_levels' in sec:
                levels_str = sec['confidence_levels'].strip()
                # Support both single value (0.90) and comma-separated list (0.90,0.95)
                if ',' in levels_str:
                    config.privacy.confidence_levels = [float(x.strip()) for x in levels_str.split(',')]
                else:
                    config.privacy.confidence_levels = [float(levels_str)]
            if 'include_relative_moe' in sec:
                config.privacy.include_relative_moe = sec.getboolean('include_relative_moe')
        
        # Load data section
        if 'data' in parser:
            sec = parser['data']
            config.data.input_path = sec.get('input_path', '')
            config.data.output_path = sec.get('output_path', '')
            config.data.city_province_path = sec.get('city_province_path', 'data/city_province.csv')
            config.data.input_format = sec.get('input_format', 'parquet')
            config.data.winsorize_percentile = float(sec.get('winsorize_percentile', '99.0'))
            if 'winsorize_cap' in sec:
                config.data.winsorize_cap = float(sec['winsorize_cap'])
            config.data.num_days = int(sec.get('num_days', '30'))
        
        # Load spark section
        if 'spark' in parser:
            sec = parser['spark']
            config.spark.app_name = sec.get('app_name', 'TransactionDP')
            config.spark.master = sec.get('master', 'local[*]')
            config.spark.executor_memory = sec.get('executor_memory', '4g')
            config.spark.driver_memory = sec.get('driver_memory', '2g')
            config.spark.shuffle_partitions = int(sec.get('shuffle_partitions', '200'))
        
        # Load columns section
        if 'columns' in parser:
            for key, value in parser['columns'].items():
                config.columns[key] = value
        
        logger.info(f"Configuration loaded from {config_path}")
        return config
    
    def to_ini(self, config_path: str) -> None:
        """Save configuration to INI file."""
        parser = configparser.ConfigParser()
        
        # Privacy section (SDC settings)
        parser['privacy'] = {
            'contribution_bound_method': self.privacy.contribution_bound_method,
            'contribution_bound_iqr_multiplier': str(self.privacy.contribution_bound_iqr_multiplier),
            'contribution_bound_fixed': str(self.privacy.contribution_bound_fixed),
            'contribution_bound_percentile': str(self.privacy.contribution_bound_percentile),
            'contribution_bound_per_group': str(self.privacy.contribution_bound_per_group).lower(),
            'suppression_threshold': str(self.privacy.suppression_threshold),
            'suppression_method': self.privacy.suppression_method,
            'suppression_sentinel': str(self.privacy.suppression_sentinel),
            'mcc_cap_percentile': str(self.privacy.mcc_cap_percentile),
            'noise_level': str(self.privacy.noise_level),
            'noise_seed': str(self.privacy.noise_seed),
            'min_noise_factor_deviation': str(self.privacy.min_noise_factor_deviation),
            'total_rho': str(self.privacy.total_rho),  # Will output as fraction (e.g., "1/4")
            'delta': str(self.privacy.delta),
            'geographic_split': ','.join(f'{k}:{v}' for k, v in self.privacy.geographic_split.items()),
            'query_split': ','.join(f'{k}:{v}' for k, v in self.privacy.query_split.items()),
            'cards_jitter': str(self.privacy.cards_jitter),
            'amount_jitter': str(self.privacy.amount_jitter),
            'sensitivity_method': self.privacy.sensitivity_method,
            'fixed_max_cells_per_card': str(self.privacy.fixed_max_cells_per_card),
            'confidence_levels': ','.join(str(level) for level in self.privacy.confidence_levels),
            'include_relative_moe': str(self.privacy.include_relative_moe).lower(),
        }
        
        # Data section
        parser['data'] = {
            'input_path': self.data.input_path,
            'output_path': self.data.output_path,
            'city_province_path': self.data.city_province_path,
            'input_format': self.data.input_format,
            'winsorize_percentile': str(self.data.winsorize_percentile),
            'num_days': str(self.data.num_days),
        }
        if self.data.winsorize_cap is not None:
            parser['data']['winsorize_cap'] = str(self.data.winsorize_cap)
        
        # Spark section
        parser['spark'] = {
            'app_name': self.spark.app_name,
            'master': self.spark.master,
            'executor_memory': self.spark.executor_memory,
            'driver_memory': self.spark.driver_memory,
            'shuffle_partitions': str(self.spark.shuffle_partitions),
        }
        
        # Columns section
        parser['columns'] = self.columns
        
        with open(config_path, 'w', encoding='utf-8') as f:
            parser.write(f)
        
        logger.info(f"Configuration saved to {config_path}")
