#!/usr/bin/env python3
"""
Quick Test - Small Scale
========================
Quick test with small data (10K records) to verify everything works.

Usage:
    python examples/quick_test.py
"""

import os
import sys
import tempfile
from datetime import datetime


def main():
    print("╔════════════════════════════════════════════════════════════╗")
    print("║           Transaction DP System - Quick Test               ║")
    print("╚════════════════════════════════════════════════════════════╝")
    print(f"\nStarted at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Check if city_province.csv exists
    city_province_path = 'data/city_province.csv'
    if not os.path.exists(city_province_path):
        print(f"\n❌ Error: {city_province_path} not found!")
        print("   Please ensure city_province.csv is in the data/ directory.")
        return 1
    
    print("\n📋 Configuration:")
    print("   Records:       10,000 (small for quick test)")
    print("   Privacy (ρ):   1")
    print("   Days:          30")
    
    try:
        # Import after checking dependencies
        from examples.generate_sample_data import generate_sample_data
        from core.config import Config
        from core.pipeline import DPPipeline
        from fractions import Fraction
        
        # Use temp directory for quick test
        with tempfile.TemporaryDirectory() as tmpdir:
            input_path = os.path.join(tmpdir, 'test_transactions.csv')
            output_path = os.path.join(tmpdir, 'output')
            
            # Step 1: Generate small dataset
            print("\n" + "=" * 50)
            print("Step 1: Generating 10K test transactions...")
            print("=" * 50)
            
            generate_sample_data(
                num_records=10000,
                output_path=input_path,
                city_province_path=city_province_path,
                num_days=30,
                num_cards=1000,
                num_acceptors=500,
                seed=42
            )
            
            # Step 2: Run pipeline
            print("\n" + "=" * 50)
            print("Step 2: Running DP pipeline...")
            print("=" * 50)
            
            config = Config()
            config.data.input_path = input_path
            config.data.output_path = output_path
            config.data.city_province_path = city_province_path
            config.data.input_format = 'csv'
            config.data.num_days = 30
            config.data.winsorize_percentile = 99.0
            
            config.privacy.total_rho = Fraction(1)
            config.privacy.delta = 1e-10
            
            config.spark.app_name = "TransactionDP-QuickTest"
            config.spark.master = "local[2]"  # Use only 2 cores for quick test
            config.spark.executor_memory = "1g"
            config.spark.driver_memory = "1g"
            
            config.validate()
            
            pipeline = DPPipeline(config)
            result = pipeline.run()
            
            # Step 3: Results
            print("\n" + "=" * 50)
            print("Results")
            print("=" * 50)
            
            if result['success']:
                print(f"✅ SUCCESS!")
                print(f"   Records processed: {result['total_records']:,}")
                print(f"   Privacy budget:    ρ = {result['budget_used']}")
                if result.get('duration_seconds'):
                    print(f"   Duration:          {result['duration_seconds']:.2f} seconds")
                
                # Quick peek at output
                import json
                metadata_path = os.path.join(output_path, "metadata.json")
                if os.path.exists(metadata_path):
                    with open(metadata_path, 'r') as f:
                        meta = json.load(f)
                    print(f"\n   Output statistics:")
                    print(f"     Non-zero cells: {meta['statistics']['non_zero_cells']:,}")
                    print(f"     Output records: {meta['statistics']['output_records']:,}")
                
                print("\n✅ Quick test passed! The system is working correctly.")
                return 0
            else:
                print(f"❌ FAILED!")
                print(f"   Errors: {result.get('errors', 'Unknown')}")
                return 1
                
    except ImportError as e:
        print(f"\n❌ Import Error: {e}")
        print("   Please install dependencies: pip install -r requirements.txt")
        return 1
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main())

