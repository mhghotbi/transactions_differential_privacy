#!/usr/bin/env python3
"""
Run Transaction DP Pipeline - Example
=====================================
Complete example that generates sample data and runs the DP pipeline.

Usage:
    python examples/run_pipeline.py
    
    # Or step by step:
    python examples/run_pipeline.py --generate-only
    python examples/run_pipeline.py --run-only --input data/sample_transactions.csv
"""

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path


def generate_data(output_path: str, city_province_path: str, num_records: int = 1000000):
    """Generate sample data."""
    from examples.generate_sample_data import generate_sample_data
    
    print("=" * 60)
    print("Step 1: Generating sample transaction data")
    print("=" * 60)
    
    generate_sample_data(
        num_records=num_records,
        output_path=output_path,
        city_province_path=city_province_path,
        num_days=30,
        num_cards=50000,
        num_acceptors=10000,
        seed=42
    )
    
    return output_path


def run_pipeline(input_path: str, output_path: str, city_province_path: str, rho: str = "1"):
    """Run the DP pipeline."""
    from fractions import Fraction
    from core.config import Config
    from core.pipeline import DPPipeline
    
    print("\n" + "=" * 60)
    print("Step 2: Running DP Pipeline")
    print("=" * 60)
    
    # Create config programmatically
    config = Config()
    config.data.input_path = input_path
    config.data.output_path = output_path
    config.data.city_province_path = city_province_path
    config.data.input_format = 'csv'
    config.data.num_days = 30
    config.data.winsorize_percentile = 99.0
    
    config.privacy.total_rho = Fraction(rho)
    config.privacy.delta = 1e-10
    
    config.spark.app_name = "TransactionDP-Example"
    config.spark.master = "local[*]"
    config.spark.executor_memory = "4g"
    config.spark.driver_memory = "2g"
    
    # Validate
    print("\nValidating configuration...")
    config.validate()
    
    # Run pipeline
    print("\nStarting pipeline...")
    pipeline = DPPipeline(config)
    result = pipeline.run()
    
    return result


def print_results(result: dict):
    """Print pipeline results."""
    print("\n" + "=" * 60)
    print("Results")
    print("=" * 60)
    
    print(f"Success:          {result['success']}")
    print(f"Total Records:    {result['total_records']:,}")
    print(f"Budget Used:      ρ = {result['budget_used']}")
    print(f"Output Path:      {result['output_path']}")
    
    if result.get('duration_seconds'):
        print(f"Duration:         {result['duration_seconds']:.2f} seconds")
    
    if result.get('errors'):
        print(f"Errors:           {result['errors']}")


def inspect_output(output_path: str):
    """Inspect the output data."""
    import json
    
    print("\n" + "=" * 60)
    print("Inspecting Output")
    print("=" * 60)
    
    # Read metadata
    metadata_path = os.path.join(output_path, "metadata.json")
    if os.path.exists(metadata_path):
        with open(metadata_path, 'r', encoding='utf-8') as f:
            metadata = json.load(f)
        
        print("\nMetadata:")
        print(f"  Created:        {metadata['created_at']}")
        print(f"  Total Cells:    {metadata['statistics']['total_cells']:,}")
        print(f"  Non-Zero Cells: {metadata['statistics']['non_zero_cells']:,}")
        print(f"  Output Records: {metadata['statistics']['output_records']:,}")
        print(f"  Dimensions:")
        print(f"    Provinces:    {metadata['dimensions']['num_provinces']}")
        print(f"    Cities:       {metadata['dimensions']['num_cities']}")
        print(f"    MCCs:         {metadata['dimensions']['num_mccs']}")
        print(f"    Days:         {metadata['dimensions']['num_days']}")
    
    # Check for Parquet output
    parquet_path = os.path.join(output_path, "protected_data")
    if os.path.exists(parquet_path):
        print(f"\nParquet data written to: {parquet_path}")
        
        # List partitions
        partitions = [d for d in os.listdir(parquet_path) if d.startswith('province_name=')]
        if partitions:
            print(f"  Partitions: {len(partitions)}")
            for p in sorted(partitions)[:5]:
                print(f"    - {p}")
            if len(partitions) > 5:
                print(f"    ... and {len(partitions) - 5} more")


def main():
    parser = argparse.ArgumentParser(
        description="Run Transaction DP Pipeline Example",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Generate data and run pipeline
    python examples/run_pipeline.py
    
    # Only generate data
    python examples/run_pipeline.py --generate-only
    
    # Only run pipeline with existing data
    python examples/run_pipeline.py --run-only --input data/sample_transactions.csv
    
    # Use smaller dataset for quick test
    python examples/run_pipeline.py --num-records 100000
    
    # Custom privacy budget
    python examples/run_pipeline.py --rho 1/2
        """
    )
    
    parser.add_argument(
        '--generate-only',
        action='store_true',
        help='Only generate sample data, do not run pipeline'
    )
    
    parser.add_argument(
        '--run-only',
        action='store_true',
        help='Only run pipeline, assume data exists'
    )
    
    parser.add_argument(
        '--input', '-i',
        type=str,
        default='data/sample_transactions.csv',
        help='Input data path (default: data/sample_transactions.csv)'
    )
    
    parser.add_argument(
        '--output', '-o',
        type=str,
        default='output/dp_protected',
        help='Output path (default: output/dp_protected)'
    )
    
    parser.add_argument(
        '--city-province', '-c',
        type=str,
        default='data/city_province.csv',
        help='Path to city_province.csv'
    )
    
    parser.add_argument(
        '--num-records', '-n',
        type=int,
        default=1000000,
        help='Number of records to generate (default: 1,000,000)'
    )
    
    parser.add_argument(
        '--rho', '-r',
        type=str,
        default='1',
        help='Privacy budget rho (default: 1, can be fraction like 1/2)'
    )
    
    args = parser.parse_args()
    
    print("╔════════════════════════════════════════════════════════════╗")
    print("║          Transaction DP System - Example Runner            ║")
    print("╚════════════════════════════════════════════════════════════╝")
    print(f"\nStarted at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    input_path = args.input
    output_path = args.output
    
    try:
        # Step 1: Generate data (if not run-only)
        if not args.run_only:
            generate_data(
                output_path=input_path,
                city_province_path=args.city_province,
                num_records=args.num_records
            )
        
        # Step 2: Run pipeline (if not generate-only)
        if not args.generate_only:
            result = run_pipeline(
                input_path=input_path,
                output_path=output_path,
                city_province_path=args.city_province,
                rho=args.rho
            )
            
            print_results(result)
            
            if result['success']:
                inspect_output(output_path)
        
        print("\n✅ Complete!")
        return 0
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main())

