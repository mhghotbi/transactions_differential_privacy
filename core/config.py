"""
Configuration management for Transaction DP System.
Handles loading, validation, and access to configuration parameters.
"""

import configparser
import os
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
from fractions import Fraction
from pathlib import Path


logger = logging.getLogger(__name__)


@dataclass
class PrivacyConfig:
    """Privacy-related configuration."""
    total_rho: Fraction = field(default_factory=lambda: Fraction(1, 4))  # 0.25 monthly
    delta: float = 1e-10
    geographic_split: Dict[str, float] = field(default_factory=lambda: {
        "province": 0.2,
        "city": 0.8
    })
    query_split: Dict[str, float] = field(default_factory=lambda: {
        "transaction_count": 0.34,
        "unique_cards": 0.33,
        "total_amount": 0.33
    })
    
    # Bounded contribution settings
    contribution_bound_method: str = "iqr"  # 'iqr', 'percentile', or 'fixed'
    contribution_bound_iqr_multiplier: float = 1.5
    contribution_bound_fixed: int = 5
    contribution_bound_percentile: float = 99.0
    
    # Computed K value (set after analysis)
    computed_contribution_bound: Optional[int] = None
    
    # Computed D_max (max cells per card) for user-level DP
    computed_d_max: Optional[int] = None
    
    # Suppression settings
    suppression_threshold: int = 10  # Suppress cells with noisy count < threshold
    suppression_method: str = "flag"  # 'flag', 'null', or 'value'
    suppression_sentinel: int = -1  # Sentinel value for suppressed cells (if method='value')
    
    # Confidence interval settings
    confidence_levels: List[float] = field(default_factory=lambda: [0.90])  # Default 90% CI
    include_relative_moe: bool = True  # Include relative margin of error
    
    # Global sensitivity settings
    sensitivity_method: str = "global"  # 'local', 'global', or 'fixed'
    fixed_max_cells_per_card: int = 100  # For 'fixed' sensitivity method
    
    # MCC grouping settings for stratified sensitivity
    mcc_grouping_enabled: bool = True
    mcc_num_groups: int = 5  # Target number of groups (auto-determined from data)
    mcc_group_cap_percentile: float = 99.0  # Percentile for per-group caps
    
    # Computed MCC grouping (set after analysis)
    mcc_group_caps: Optional[Dict[int, float]] = None  # group_id -> cap
    mcc_to_group: Optional[Dict[str, int]] = None  # mcc_code -> group_id
    
    def validate(self) -> None:
        """Validate privacy configuration."""
        geo_sum = sum(self.geographic_split.values())
        if abs(geo_sum - 1.0) > 1e-6:
            raise ValueError(f"Geographic split must sum to 1.0, got {geo_sum}")
        
        query_sum = sum(self.query_split.values())
        if abs(query_sum - 1.0) > 1e-6:
            raise ValueError(f"Query split must sum to 1.0, got {query_sum}")
        
        if self.total_rho <= 0:
            raise ValueError(f"total_rho must be positive, got {self.total_rho}")
        
        if self.contribution_bound_method not in ('iqr', 'percentile', 'fixed'):
            raise ValueError(f"contribution_bound_method must be 'iqr', 'percentile', or 'fixed', got {self.contribution_bound_method}")
        
        if self.contribution_bound_fixed < 1:
            raise ValueError(f"contribution_bound_fixed must be >= 1, got {self.contribution_bound_fixed}")
        
        if self.suppression_threshold < 0:
            raise ValueError(f"suppression_threshold must be >= 0, got {self.suppression_threshold}")
        
        if self.suppression_method not in ('flag', 'null', 'value'):
            raise ValueError(f"suppression_method must be 'flag', 'null', or 'value', got {self.suppression_method}")
        
        if self.sensitivity_method not in ('local', 'global', 'fixed'):
            raise ValueError(f"sensitivity_method must be 'local', 'global', or 'fixed', got {self.sensitivity_method}")
        
        for level in self.confidence_levels:
            if not 0 < level < 1:
                raise ValueError(f"confidence_level must be in (0, 1), got {level}")


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
    date_column: str = "transaction_date"
    date_format: str = "%Y-%m-%d"
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
        
        # Load privacy section
        if 'privacy' in parser:
            sec = parser['privacy']
            config.privacy.total_rho = Fraction(sec.get('total_rho', '1'))
            config.privacy.delta = float(sec.get('delta', '1e-10'))
            
            # Parse geographic split
            if 'geographic_split_province' in sec:
                config.privacy.geographic_split['province'] = float(sec['geographic_split_province'])
            if 'geographic_split_city' in sec:
                config.privacy.geographic_split['city'] = float(sec['geographic_split_city'])
            
            # Parse query split
            for query in ['transaction_count', 'unique_cards', 'total_amount']:
                key = f'query_split_{query}'
                if key in sec:
                    config.privacy.query_split[query] = float(sec[key])
            
            # Parse bounded contribution settings
            if 'contribution_bound_method' in sec:
                config.privacy.contribution_bound_method = sec['contribution_bound_method']
            if 'contribution_bound_iqr_multiplier' in sec:
                config.privacy.contribution_bound_iqr_multiplier = float(sec['contribution_bound_iqr_multiplier'])
            if 'contribution_bound_fixed' in sec:
                config.privacy.contribution_bound_fixed = int(sec['contribution_bound_fixed'])
            if 'contribution_bound_percentile' in sec:
                config.privacy.contribution_bound_percentile = float(sec['contribution_bound_percentile'])
            
            # Parse suppression settings
            if 'suppression_threshold' in sec:
                config.privacy.suppression_threshold = int(sec['suppression_threshold'])
            if 'suppression_method' in sec:
                config.privacy.suppression_method = sec['suppression_method']
            if 'suppression_sentinel' in sec:
                config.privacy.suppression_sentinel = int(sec['suppression_sentinel'])
            
            # Parse confidence interval settings
            if 'confidence_levels' in sec:
                config.privacy.confidence_levels = [float(x.strip()) for x in sec['confidence_levels'].split(',')]
            if 'include_relative_moe' in sec:
                config.privacy.include_relative_moe = sec.getboolean('include_relative_moe')
            
            # Parse global sensitivity settings
            if 'sensitivity_method' in sec:
                config.privacy.sensitivity_method = sec['sensitivity_method']
            if 'fixed_max_cells_per_card' in sec:
                config.privacy.fixed_max_cells_per_card = int(sec['fixed_max_cells_per_card'])
        
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
            config.data.date_column = sec.get('date_column', 'transaction_date')
            config.data.date_format = sec.get('date_format', '%Y-%m-%d')
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
        
        # Privacy section
        parser['privacy'] = {
            'total_rho': str(self.privacy.total_rho),
            'delta': str(self.privacy.delta),
            'geographic_split_province': str(self.privacy.geographic_split['province']),
            'geographic_split_city': str(self.privacy.geographic_split['city']),
            'contribution_bound_method': self.privacy.contribution_bound_method,
            'contribution_bound_iqr_multiplier': str(self.privacy.contribution_bound_iqr_multiplier),
            'contribution_bound_fixed': str(self.privacy.contribution_bound_fixed),
            'contribution_bound_percentile': str(self.privacy.contribution_bound_percentile),
            'suppression_threshold': str(self.privacy.suppression_threshold),
            'suppression_method': self.privacy.suppression_method,
            'suppression_sentinel': str(self.privacy.suppression_sentinel),
            'confidence_levels': ','.join(str(x) for x in self.privacy.confidence_levels),
            'include_relative_moe': str(self.privacy.include_relative_moe).lower(),
            'sensitivity_method': self.privacy.sensitivity_method,
            'fixed_max_cells_per_card': str(self.privacy.fixed_max_cells_per_card),
        }
        for query, weight in self.privacy.query_split.items():
            parser['privacy'][f'query_split_{query}'] = str(weight)
        
        # Data section
        parser['data'] = {
            'input_path': self.data.input_path,
            'output_path': self.data.output_path,
            'city_province_path': self.data.city_province_path,
            'input_format': self.data.input_format,
            'winsorize_percentile': str(self.data.winsorize_percentile),
            'date_column': self.data.date_column,
            'date_format': self.data.date_format,
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
