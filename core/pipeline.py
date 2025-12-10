"""
DP Pipeline Orchestration.

This module coordinates the entire differential privacy workflow:
1. Read transaction data
2. Preprocess and aggregate into histograms
3. Apply DP noise using top-down mechanism
4. Write protected results
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Any, Optional

from core.config import Config


logger = logging.getLogger(__name__)


@dataclass
class PipelineResult:
    """Result of a pipeline execution."""
    success: bool
    total_records: int = 0
    budget_used: str = ""
    output_path: str = ""
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    errors: list = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "success": self.success,
            "total_records": self.total_records,
            "budget_used": self.budget_used,
            "output_path": self.output_path,
            "duration_seconds": (
                (self.end_time - self.start_time).total_seconds()
                if self.start_time and self.end_time else None
            ),
            "errors": self.errors
        }


class DPPipeline:
    """
    Main pipeline for applying differential privacy to transaction data.
    
    The pipeline follows these steps:
    1. Initialize Spark session
    2. Load geography mapping (city -> province)
    3. Read transaction data
    4. Preprocess: winsorize amounts, compute day indices
    5. Aggregate to histograms per (province, city, mcc, day)
    6. Apply top-down DP: Province level -> City level
    7. Write protected results
    """
    
    def __init__(self, config: Config):
        """
        Initialize pipeline.
        
        Args:
            config: Configuration object
        """
        self.config = config
        self._spark = None
        self._geography = None
        self._budget = None
    
    def _init_spark(self):
        """Initialize Spark session."""
        from pyspark.sql import SparkSession
        
        logger.info("Initializing Spark session...")
        
        # Log config values for debugging
        logger.info(f"Config spark.master: {self.config.spark.master}")
        logger.info(f"Config spark.app_name: {self.config.spark.app_name}")
        logger.info(f"Config spark.executor_memory: {self.config.spark.executor_memory}")
        logger.info(f"Config spark.driver_memory: {self.config.spark.driver_memory}")
        
        # Always check for existing session first - use it if available
        # Don't stop existing sessions - just use them (even if master is different)
        existing_session = SparkSession.getActiveSession()
        desired_master = self.config.spark.master or "local[*]"
        
        if existing_session:
            current_master = existing_session.sparkContext.master
            logger.info(f"✅ Found existing Spark session with master={current_master}")
            logger.info(f"   Desired master={desired_master}")
            
            # Always reuse existing session - don't stop it
            # Master mismatch is not critical - we'll just use what's available
            if current_master != desired_master:
                logger.warning(f"   Note: Master mismatch (desired={desired_master}, actual={current_master})")
                logger.warning(f"   Using existing session anyway - master cannot be changed after creation")
            
            self._spark = existing_session
            self._spark_created_by_pipeline = False  # Reusing existing - don't stop it
            
            # Verify and log session info
            actual_master = self._spark.sparkContext.master
            actual_parallelism = self._spark.sparkContext.defaultParallelism
            logger.info(f"Using existing Spark session:")
            logger.info(f"  Master: {actual_master}")
            logger.info(f"  Default Parallelism: {actual_parallelism}")
            logger.info(f"  Spark Version: {self._spark.version}")
            return self._spark  # Exit early - reuse existing session
        
        builder = SparkSession.builder.appName(self.config.spark.app_name)
        
        # Set master FIRST (before getOrCreate) - this is critical!
        desired_master = self.config.spark.master or "local[*]"
        builder = builder.master(desired_master)
        logger.info(f"Setting Spark master to: {desired_master}")
        
        for key, value in self.config.spark.to_spark_conf().items():
            if key != "spark.app.name":  # Already set
                builder = builder.config(key, value)
        
        # Optimizations for large datasets
        builder = builder.config("spark.sql.adaptive.enabled", "true")
        builder = builder.config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        builder = builder.config("spark.sql.adaptive.skewJoin.enabled", "true")
        builder = builder.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        
        # For large datasets (10M+ records), increase partitions
        # Default shuffle partitions based on executor memory
        executor_memory = self.config.spark.executor_memory
        if executor_memory:
            # Parse memory string (e.g., "4g" -> 4)
            mem_gb = int(executor_memory.rstrip('gGmM')) if executor_memory[-1].lower() in 'gm' else 4
            # Set shuffle partitions to 2-3x cores, minimum 200 for large datasets
            shuffle_partitions = max(200, mem_gb * 50)
            builder = builder.config("spark.sql.shuffle.partitions", str(shuffle_partitions))
            builder = builder.config("spark.default.parallelism", str(shuffle_partitions))
            logger.info(f"Set shuffle partitions to {shuffle_partitions} for large dataset processing")
        
        # Memory management for large datasets
        builder = builder.config("spark.memory.fraction", "0.8")
        builder = builder.config("spark.memory.storageFraction", "0.3")
        
        # Network timeout for large operations
        builder = builder.config("spark.network.timeout", "600s")
        builder = builder.config("spark.sql.broadcastTimeout", "600s")
        
        # Create new session (only if we didn't reuse existing one above)
        # Double-check no session exists (in case one was created between checks)
        try:
            temp_session = SparkSession.getActiveSession()
            if temp_session:
                logger.info("Found Spark session created between checks, reusing it")
                self._spark = temp_session
                self._spark_created_by_pipeline = False  # Don't stop it
                return self._spark
        except:
            pass
        
        # Try to create new session
        try:
            self._spark = builder.getOrCreate()
            self._spark_created_by_pipeline = True  # We created this session
            logger.info("✅ Created new Spark session")
        except Exception as e:
            # If creation fails, check if session exists now (created by another thread)
            logger.warning(f"Failed to create Spark session: {e}")
            logger.info("Checking if session exists now...")
            temp_session = SparkSession.getActiveSession()
            if temp_session:
                logger.info("✅ Found existing Spark session, using it")
                self._spark = temp_session
                self._spark_created_by_pipeline = False  # Don't stop it
            else:
                # Re-raise if we really can't get a session
                logger.error("❌ No Spark session available and cannot create one")
                raise RuntimeError(f"Failed to initialize Spark session: {e}") from e
        
        # Verify master was set correctly
        actual_master = self._spark.sparkContext.master
        actual_parallelism = self._spark.sparkContext.defaultParallelism
        
        logger.info(f"Spark session initialized: {self._spark.sparkContext.appName}")
        logger.info(f"Spark master: {actual_master}")
        logger.info(f"Spark default parallelism: {actual_parallelism}")
        
        # Warn if master doesn't match
        if actual_master != desired_master:
            logger.warning(f"WARNING: Spark master mismatch! Desired={desired_master}, Actual={actual_master}")
            logger.warning("This may happen if Spark session was created elsewhere. Try stopping all Spark sessions.")
        
        # For local[N], verify parallelism matches N
        if desired_master.startswith("local["):
            try:
                # Extract number from local[N] or local[*]
                if desired_master == "local[*]":
                    # Should use all cores
                    import os
                    expected_cores = os.cpu_count() or 2
                else:
                    # Extract N from local[N]
                    n_str = desired_master[6:-1]  # Remove "local[" and "]"
                    expected_cores = int(n_str) if n_str.isdigit() else 2
                
                if actual_parallelism != expected_cores:
                    logger.warning(f"WARNING: Parallelism mismatch! Expected={expected_cores}, Actual={actual_parallelism}")
                    logger.warning(f"Spark may not be using all requested cores. Check Spark configuration.")
            except Exception as e:
                logger.debug(f"Could not verify parallelism: {e}")
        
        return self._spark
    
    def _init_geography(self):
        """Load geography mapping."""
        from schema.geography import Geography
        
        logger.info(f"Loading geography from: {self.config.data.city_province_path}")
        
        self._geography = Geography.from_csv(self.config.data.city_province_path)
        
        logger.info(f"Loaded {self._geography.num_provinces} provinces, "
                   f"{self._geography.num_cities} cities")
        
        return self._geography
    
    def _init_budget(self):
        """Initialize privacy budget."""
        from core.budget import Budget
        
        logger.info("Initializing privacy budget...")
        
        self._budget = Budget(
            total_rho=self.config.privacy.total_rho,
            delta=self.config.privacy.delta,
            geographic_split=self.config.privacy.geographic_split,
            query_split=self.config.privacy.query_split
        )
        
        logger.info(self._budget.summary())
        
        return self._budget
    
    def run(self) -> Dict[str, Any]:
        """
        Execute the full DP pipeline.
        
        Returns:
            Dictionary with execution results
        """
        result = PipelineResult(success=False)
        result.start_time = datetime.now()
        
        # Track if we created the Spark session (vs reusing existing one)
        # Don't stop Spark session in finally if we reused an existing one (e.g., from notebook)
        self._spark_created_by_pipeline = False
        
        try:
            # Initialize components
            # FIRST: Check if there's already an existing Spark session (e.g., from notebook)
            from pyspark.sql import SparkSession
            existing_spark = SparkSession.getActiveSession()
            if existing_spark:
                logger.info(f"✅ Found existing Spark session - reusing it (master={existing_spark.sparkContext.master})")
                self._spark = existing_spark
                self._spark_created_by_pipeline = False
                spark = existing_spark
            else:
                # No existing session, create new one
                spark = self._init_spark()
                if spark is None:
                    # Try one more time to get existing session
                    spark = SparkSession.getActiveSession()
                    if spark:
                        self._spark = spark
                        self._spark_created_by_pipeline = False
                        logger.info("Recovered existing Spark session on retry")
                    else:
                        raise RuntimeError(
                            "Failed to initialize Spark session. "
                            "If running in a notebook, ensure SparkSession is created before running the pipeline."
                        )
            
            geography = self._init_geography()
            if geography is None:
                raise RuntimeError("Failed to initialize Geography")
            
            budget = self._init_budget()
            if budget is None:
                raise RuntimeError("Failed to initialize Budget")
            
            # Import components (after Spark init)
            from reader.spark_reader import SparkTransactionReader
            from reader.preprocessor import TransactionPreprocessor
            from engine.topdown import TopDownEngine
            from writer.parquet_writer import ParquetWriter
            
            # Step 1: Read data
            logger.info("=" * 60)
            logger.info("Step 1: Reading transaction data")
            logger.info("=" * 60)
            
            reader = SparkTransactionReader(
                spark=spark,
                config=self.config,
                geography=geography
            )
            if reader is None:
                raise RuntimeError("Failed to create SparkTransactionReader")
            
            raw_df = reader.read()
            result.total_records = raw_df.count()
            logger.info(f"Read {result.total_records:,} transactions")
            
            # Step 2: Preprocess
            logger.info("=" * 60)
            logger.info("Step 2: Preprocessing data")
            logger.info("=" * 60)
            
            preprocessor = TransactionPreprocessor(
                spark=spark,
                config=self.config,
                geography=geography
            )
            histograms = preprocessor.process(raw_df)
            
            # Step 3: Apply DP
            logger.info("=" * 60)
            logger.info("Step 3: Applying differential privacy")
            logger.info("=" * 60)
            
            engine = TopDownEngine(
                spark=spark,
                config=self.config,
                geography=geography,
                budget=budget
            )
            
            # Set user-level DP parameters from preprocessing
            d_max = preprocessor.d_max or self.config.privacy.computed_d_max or 1
            k_bound = self.config.privacy.computed_contribution_bound or 1
            
            # Get winsorize cap from preprocessor - this should always be set during preprocessing
            winsorize_cap = preprocessor.winsorize_cap
            if winsorize_cap is None or winsorize_cap == 1.0:
                # Try to get from config as fallback
                if self.config.data.winsorize_cap is not None:
                    winsorize_cap = self.config.data.winsorize_cap
                    logger.warning(
                        f"Winsorize cap not found in preprocessor, using config value: {winsorize_cap:,.2f}"
                    )
                else:
                    # Last resort: compute from data (should not happen in production)
                    logger.error(
                        "CRITICAL: Winsorize cap not set! Using default 1.0 which assumes normalized amounts. "
                        "This will violate privacy if amounts are in original units. "
                        "Ensure winsorization is applied during preprocessing."
                    )
                    winsorize_cap = 1.0
            
            logger.info(f"Using winsorize cap for amount sensitivity: {winsorize_cap:,.2f}")
            
            engine.set_user_level_params(
                d_max=d_max,
                k_bound=k_bound,
                winsorize_cap=winsorize_cap
            )
            
            # Keep a copy of original histogram before DP (for comparison output)
            logger.info("Creating copy of original histogram for comparison output...")
            original_histograms = histograms.copy() if hasattr(histograms, 'copy') else histograms
            
            protected_histograms = engine.run(histograms)
            
            # Step 4: Write output
            logger.info("=" * 60)
            logger.info("Step 4: Writing protected data")
            logger.info("=" * 60)
            
            writer = ParquetWriter(
                spark=spark,
                config=self.config
            )
            # Write both original and protected data with suffixes
            writer.write_comparison(
                original_histogram=original_histograms,
                protected_histogram=protected_histograms
            )
            
            # Success
            result.success = True
            result.budget_used = str(self.config.privacy.total_rho)
            result.output_path = self.config.data.output_path
            
            logger.info("Pipeline completed successfully")
            
        except Exception as e:
            logger.exception(f"Pipeline failed: {e}")
            result.errors.append(str(e))
        
        finally:
            result.end_time = datetime.now()
            
            # Only stop Spark session if we created it (not if we reused existing one)
            # This prevents closing notebook's Spark session
            if self._spark is not None and getattr(self, '_spark_created_by_pipeline', False):
                logger.info("Stopping Spark session created by pipeline...")
                try:
                    self._spark.stop()
                except Exception as e:
                    logger.warning(f"Error stopping Spark session: {e}")
            elif self._spark is not None:
                logger.info("Keeping existing Spark session (not created by pipeline)")
        
        return result.to_dict()
    
    def validate(self) -> bool:
        """
        Validate configuration without running the pipeline.
        
        Returns:
            True if configuration is valid
        """
        try:
            self.config.validate()
            
            # Check files exist
            import os
            if not os.path.exists(self.config.data.city_province_path):
                raise FileNotFoundError(
                    f"City-province file not found: {self.config.data.city_province_path}"
                )
            
            logger.info("Configuration validation passed")
            return True
            
        except Exception as e:
            logger.error(f"Configuration validation failed: {e}")
            return False

