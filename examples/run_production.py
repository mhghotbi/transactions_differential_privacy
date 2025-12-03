#!/usr/bin/env python3
"""
Production Pipeline Runner for 10B+ Transaction Records.

This script runs the full DP pipeline optimized for production scale.
It uses distributed processing throughout - no data is collected to driver.

Usage:
    # Local testing (small data)
    python examples/run_production.py \
        --input data/transactions.parquet \
        --output output/protected \
        --rho 1

    # Production (on Spark cluster)
    spark-submit \
        --master yarn \
        --deploy-mode cluster \
        --driver-memory 16g \
        --executor-memory 32g \
        --executor-cores 4 \
        --num-executors 100 \
        --conf spark.sql.shuffle.partitions=2000 \
        run_production.py \
        --input hdfs:///data/transactions \
        --output hdfs:///output/protected \
        --rho 1 \
        --checkpoint hdfs:///tmp/dp_checkpoint

Example Cluster Sizing for 10B rows:
    - 100 executors × 32GB = 3.2TB total memory
    - 100 executors × 4 cores = 400 cores
    - Expected runtime: 2-4 hours depending on data locality
"""

import os
import sys
import argparse
import logging
from datetime import datetime
from fractions import Fraction

# Add parent to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from pyspark.sql import SparkSession

from core.config import Config
from schema.geography import Geography
from core.budget import Budget
from reader.preprocessor_distributed import (
    ProductionPipeline,
    get_spark_config_for_10b
)


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(
        description='Production DP Pipeline for Transaction Data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Local test
  python run_production.py --input data/test.parquet --output output/ --rho 1
  
  # Production on YARN
  spark-submit --master yarn run_production.py --input hdfs://... --output hdfs://...
        """
    )
    
    parser.add_argument(
        '--input', '-i',
        required=True,
        help='Input path (local, HDFS, S3)'
    )
    
    parser.add_argument(
        '--output', '-o',
        required=True,
        help='Output path for protected data'
    )
    
    parser.add_argument(
        '--format', '-f',
        default='parquet',
        choices=['parquet', 'csv', 'delta'],
        help='Input format (default: parquet)'
    )
    
    parser.add_argument(
        '--rho',
        type=str,
        default='1',
        help='Privacy budget rho (e.g., 1, 1/2, 0.5)'
    )
    
    parser.add_argument(
        '--geography', '-g',
        default='data/city_province.csv',
        help='Path to city_province.csv'
    )
    
    parser.add_argument(
        '--checkpoint',
        default=None,
        help='Checkpoint directory (HDFS/S3 for production)'
    )
    
    parser.add_argument(
        '--num-days',
        type=int,
        default=30,
        help='Number of days in data (default: 30)'
    )
    
    parser.add_argument(
        '--winsorize-percentile',
        type=float,
        default=99.0,
        help='Percentile for winsorization (default: 99)'
    )
    
    parser.add_argument(
        '--local',
        action='store_true',
        help='Run in local mode (for testing)'
    )
    
    parser.add_argument(
        '--exact',
        action='store_true',
        default=True,
        help='Use exact Discrete Gaussian (Census 2020 style, default)'
    )
    
    parser.add_argument(
        '--approximate',
        action='store_true',
        help='Use approximate continuous Gaussian (faster, less exact)'
    )
    
    parser.add_argument(
        '--census-das',
        action='store_true',
        help='Use full Census DAS-style hierarchical noise with consistency enforcement'
    )
    
    parser.add_argument(
        '--no-nnls',
        action='store_true',
        help='Disable NNLS post-processing (use simple scaling instead)'
    )
    
    parser.add_argument(
        '--no-rounding',
        action='store_true',
        help='Disable controlled rounding (use simple rounding instead)'
    )
    
    return parser.parse_args()


def parse_rho(rho_str: str) -> Fraction:
    """Parse rho from string (supports fractions like 1/2)."""
    if '/' in rho_str:
        num, denom = rho_str.split('/')
        return Fraction(int(num), int(denom))
    else:
        return Fraction(rho_str).limit_denominator(1000)


def create_spark_session(local: bool = False) -> SparkSession:
    """Create Spark session with production config."""
    builder = SparkSession.builder \
        .appName(f"TransactionDP-{datetime.now().strftime('%Y%m%d_%H%M%S')}")
    
    if local:
        builder = builder.master("local[*]")
        # Reduced settings for local testing
        builder = builder \
            .config("spark.driver.memory", "4g") \
            .config("spark.sql.shuffle.partitions", "8")
    else:
        # Production settings
        config = get_spark_config_for_10b()
        for key, value in config.items():
            builder = builder.config(key, value)
    
    return builder.getOrCreate()


def main():
    args = parse_args()
    
    logger.info("=" * 70)
    logger.info("PRODUCTION DP PIPELINE FOR TRANSACTIONS")
    logger.info("=" * 70)
    logger.info(f"Input:      {args.input}")
    logger.info(f"Output:     {args.output}")
    logger.info(f"Format:     {args.format}")
    logger.info(f"Rho:        {args.rho}")
    logger.info(f"Local mode: {args.local}")
    logger.info("=" * 70)
    
    # Parse privacy budget
    rho = parse_rho(args.rho)
    logger.info(f"Privacy budget: ρ = {rho} = {float(rho):.4f}")
    
    # Create Spark session
    logger.info("\nInitializing Spark session...")
    spark = create_spark_session(local=args.local)
    
    try:
        # Log Spark config
        logger.info(f"Spark version: {spark.version}")
        logger.info(f"Spark master: {spark.sparkContext.master}")
        
        # Load geography
        logger.info("\nLoading geography...")
        
        # Find geography file
        geo_paths = [
            args.geography,
            os.path.join(os.path.dirname(__file__), '..', 'data', 'city_province.csv'),
            'data/city_province.csv',
        ]
        
        geo_path = None
        for path in geo_paths:
            if os.path.exists(path):
                geo_path = path
                break
        
        if geo_path is None:
            raise FileNotFoundError(f"Cannot find geography file. Tried: {geo_paths}")
        
        geography = Geography.from_csv(geo_path)
        logger.info(f"Loaded {len(geography.provinces)} provinces")
        
        # Create config (minimal for distributed)
        class MinimalConfig:
            class DataConfig:
                num_days = args.num_days
                winsorize_percentile = args.winsorize_percentile
                winsorize_cap = None
            data = DataConfig()
        
        config = MinimalConfig()
        
        # Create budget
        budget = Budget(total_rho=rho)
        logger.info(f"Total epsilon (δ=10⁻¹⁰): {budget.total_epsilon:.2f}")
        
        # Determine mechanism type
        use_exact = not args.approximate
        use_census_das = args.census_das
        
        if use_census_das:
            logger.info("\n🏛️  Using Census DAS methodology (exact + hierarchical + consistency)")
        elif use_exact:
            logger.info("\n🎯 Using exact Discrete Gaussian mechanism")
        else:
            logger.info("\n⚡ Using approximate continuous Gaussian (faster)")
        
        # NNLS and rounding options
        use_nnls = not args.no_nnls
        use_controlled_rounding = not args.no_rounding
        
        if use_census_das:
            logger.info(f"  NNLS post-processing: {use_nnls}")
            logger.info(f"  Controlled rounding: {use_controlled_rounding}")
        
        # Create and run pipeline
        pipeline = ProductionPipeline(
            spark=spark,
            config=config,
            geography=geography,
            budget=budget,
            checkpoint_dir=args.checkpoint,
            use_exact_mechanism=use_exact,
            use_census_das=use_census_das,
            use_nnls=use_nnls,
            use_controlled_rounding=use_controlled_rounding
        )
        
        pipeline.run(
            input_path=args.input,
            output_path=args.output,
            input_format=args.format
        )
        
        logger.info("\n✅ Pipeline completed successfully!")
        
    except Exception as e:
        logger.error(f"\n❌ Pipeline failed: {e}")
        raise
    
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

