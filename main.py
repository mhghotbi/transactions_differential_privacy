#!/usr/bin/env python3
"""
Transaction DP System - Main Entry Point
=========================================
Command-line interface for running differential privacy on transaction data.

Usage:
    python main.py --config configs/default.ini
    python main.py --config configs/default.ini --rho 0.5 --output results/
"""

import argparse
import logging
import logging.handlers
import sys
import os
from datetime import datetime
from pathlib import Path
from fractions import Fraction
from typing import Optional

from core.config import Config
from core.pipeline import DPPipeline


def setup_logging(
    log_level: str = "INFO",
    log_file: Optional[str] = None,
    log_dir: str = "logs"
) -> logging.Logger:
    """
    Set up logging with console and optional file output.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
        log_file: Optional log file name. If None, auto-generated.
        log_dir: Directory for log files
        
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger("transaction_dp")
    logger.setLevel(getattr(logging, log_level.upper()))
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_format = logging.Formatter(
        '%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%H:%M:%S'
    )
    console_handler.setFormatter(console_format)
    logger.addHandler(console_handler)
    
    # File handler (rotating)
    if log_file or log_dir:
        os.makedirs(log_dir, exist_ok=True)
        if log_file is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            log_file = f"transaction_dp_{timestamp}.log"
        
        log_path = os.path.join(log_dir, log_file)
        file_handler = logging.handlers.RotatingFileHandler(
            log_path,
            maxBytes=10*1024*1024,  # 10MB
            backupCount=5,
            encoding='utf-8'
        )
        file_handler.setLevel(logging.DEBUG)
        file_format = logging.Formatter(
            '%(asctime)s [%(levelname)s] %(name)s:%(lineno)d - %(message)s'
        )
        file_handler.setFormatter(file_format)
        logger.addHandler(file_handler)
        logger.info(f"Logging to file: {log_path}")
    
    return logger


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Transaction DP System - Apply differential privacy to transaction data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Run with config file
    python main.py --config configs/default.ini
    
    # Override privacy budget
    python main.py --config configs/default.ini --rho 0.5
    
    # Specify input/output
    python main.py --config configs/default.ini \\
        --input data/transactions.parquet \\
        --output results/dp_output/
        """
    )
    
    # Required arguments
    parser.add_argument(
        "--config", "-c",
        required=True,
        help="Path to configuration INI file"
    )
    
    # Optional overrides
    parser.add_argument(
        "--rho",
        type=str,
        default=None,
        help="Override total privacy budget (rho for zCDP). Can be fraction like '1/2'"
    )
    
    parser.add_argument(
        "--input", "-i",
        type=str,
        default=None,
        help="Override input data path"
    )
    
    parser.add_argument(
        "--output", "-o",
        type=str,
        default=None,
        help="Override output path"
    )
    
    parser.add_argument(
        "--city-province",
        type=str,
        default=None,
        help="Override path to city_province.csv"
    )
    
    # Logging options
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level (default: INFO)"
    )
    
    parser.add_argument(
        "--log-dir",
        type=str,
        default="logs",
        help="Directory for log files (default: logs/)"
    )
    
    # Execution options
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate configuration and exit without running"
    )
    
    parser.add_argument(
        "--spark-master",
        type=str,
        default=None,
        help="Override Spark master URL (e.g., 'local[*]', 'spark://host:7077')"
    )
    
    return parser.parse_args()


def apply_overrides(config: Config, args: argparse.Namespace) -> Config:
    """Apply command-line overrides to configuration."""
    if args.rho is not None:
        config.privacy.total_rho = Fraction(args.rho)
    
    if args.input is not None:
        config.data.input_path = args.input
    
    if args.output is not None:
        config.data.output_path = args.output
    
    if args.city_province is not None:
        config.data.city_province_path = args.city_province
    
    if args.spark_master is not None:
        config.spark.master = args.spark_master
    
    return config


def print_banner():
    """Print application banner."""
    banner = """
╔════════════════════════════════════════════════════════════╗
║             Transaction DP System v1.0.0                   ║
║         Differential Privacy for Transaction Data          ║
╚════════════════════════════════════════════════════════════╝
    """
    print(banner)


def print_config_summary(config: Config, logger: logging.Logger):
    """Print configuration summary."""
    logger.info("=" * 60)
    logger.info("Configuration Summary")
    logger.info("=" * 60)
    logger.info(f"Privacy Budget (rho):     {config.privacy.total_rho} ({float(config.privacy.total_rho):.4f})")
    logger.info(f"Delta:                    {config.privacy.delta}")
    logger.info(f"Geographic Split:         Province={config.privacy.geographic_split['province']:.0%}, "
                f"City={config.privacy.geographic_split['city']:.0%}")
    logger.info(f"Input Path:               {config.data.input_path}")
    logger.info(f"Output Path:              {config.data.output_path}")
    logger.info(f"City-Province File:       {config.data.city_province_path}")
    logger.info(f"Winsorize Percentile:     {config.data.winsorize_percentile}%")
    logger.info(f"Number of Days:           {config.data.num_days}")
    logger.info(f"Spark Master:             {config.spark.master}")
    logger.info("=" * 60)


def main() -> int:
    """
    Main entry point.
    
    Returns:
        Exit code (0 for success, non-zero for error)
    """
    print_banner()
    
    # Parse arguments
    args = parse_args()
    
    # Set up logging
    logger = setup_logging(
        log_level=args.log_level,
        log_dir=args.log_dir
    )
    
    try:
        # Load configuration
        logger.info(f"Loading configuration from: {args.config}")
        config = Config.from_ini(args.config)
        
        # Apply command-line overrides
        config = apply_overrides(config, args)
        
        # Validate configuration
        logger.info("Validating configuration...")
        config.validate()
        
        # Print summary
        print_config_summary(config, logger)
        
        # Dry run check
        if args.dry_run:
            logger.info("Dry run mode - exiting without processing")
            return 0
        
        # Create and run pipeline
        logger.info("Initializing DP pipeline...")
        pipeline = DPPipeline(config)
        
        logger.info("Starting differential privacy processing...")
        start_time = datetime.now()
        
        result = pipeline.run()
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        logger.info("=" * 60)
        logger.info("Processing Complete")
        logger.info("=" * 60)
        logger.info(f"Duration:                 {duration:.2f} seconds")
        logger.info(f"Output Path:              {config.data.output_path}")
        logger.info(f"Records Processed:        {result.get('total_records', 'N/A')}")
        logger.info(f"Privacy Budget Used:      {result.get('budget_used', config.privacy.total_rho)}")
        logger.info("=" * 60)
        
        return 0
        
    except FileNotFoundError as e:
        logger.error(f"File not found: {e}")
        return 1
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        return 1
    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
        return 2


if __name__ == "__main__":
    sys.exit(main())

