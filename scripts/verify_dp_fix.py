"""
Verification script to check if DP fix resolved the extreme error rates.

This script:
1. Compares original vs DP-protected aggregate totals
2. Calculates error rates for all metrics
3. Verifies errors are within reasonable bounds (< 20% for aggregates)
4. Generates a verification report
"""

import os
import sys
import logging
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def verify_dp_output(
    original_data_path: str,
    dp_output_path: str,
    spark: SparkSession = None
) -> dict:
    """
    Verify DP output quality by comparing with original data.
    
    Args:
        original_data_path: Path to original transaction data (Parquet)
        dp_output_path: Path to DP-protected output directory
        spark: Optional Spark session (creates one if not provided)
        
    Returns:
        Dictionary with verification results
    """
    if spark is None:
        spark = SparkSession.builder \
            .appName("DPVerification") \
            .master("local[*]") \
            .getOrCreate()
    
    logger.info("=" * 70)
    logger.info("DP OUTPUT VERIFICATION")
    logger.info("=" * 70)
    
    # Load original data
    logger.info(f"\n📊 Loading original data from: {original_data_path}")
    original_df = spark.read.parquet(original_data_path)
    original_count = original_df.count()
    logger.info(f"  Original transactions: {original_count:,}")
    
    # Aggregate original data to cell level
    logger.info("\n📊 Aggregating original data to cell level...")
    original_agg = original_df.groupBy(
        'acceptor_city', 'mcc', 'transaction_date'
    ).agg(
        F.count('*').alias('transaction_count'),
        F.countDistinct('card_number').alias('unique_cards'),
        F.countDistinct('acceptor_id').alias('unique_acceptors'),
        F.sum('amount').alias('total_amount')
    )
    
    orig_cell_count = original_agg.count()
    logger.info(f"  Original cells: {orig_cell_count:,}")
    
    # Load DP-protected data
    protected_data_path = os.path.join(dp_output_path, "protected_data")
    if not os.path.exists(protected_data_path):
        raise FileNotFoundError(
            f"DP output not found at: {protected_data_path}\n"
            "Please run the DP pipeline first."
        )
    
    logger.info(f"\n📊 Loading DP-protected data from: {protected_data_path}")
    dp_df = spark.read.parquet(protected_data_path)
    dp_cell_count = dp_df.count()
    logger.info(f"  DP-protected cells: {dp_cell_count:,}")
    
    # Calculate aggregate totals
    logger.info("\n" + "=" * 70)
    logger.info("AGGREGATE LEVEL COMPARISON")
    logger.info("=" * 70)
    
    # Original totals
    orig_totals = original_agg.agg(
        F.sum('transaction_count').alias('transaction_count'),
        F.sum('unique_cards').alias('unique_cards'),
        F.sum('unique_acceptors').alias('unique_acceptors'),
        F.sum('total_amount').alias('total_amount')
    ).collect()[0]
    
    # DP totals
    dp_totals = dp_df.agg(
        F.sum('transaction_count').alias('transaction_count'),
        F.sum('unique_cards').alias('unique_cards'),
        F.sum('unique_acceptors').alias('unique_acceptors'),
        F.sum('total_amount').alias('total_amount')
    ).collect()[0]
    
    # Calculate errors
    metrics = ['transaction_count', 'unique_cards', 'unique_acceptors', 'total_amount']
    results = {}
    
    logger.info(f"\n{'Metric':<25} {'Original Total':>20} {'DP Total':>20} {'Error %':>12} {'Status':>8}")
    logger.info("-" * 90)
    
    all_passed = True
    
    for metric in metrics:
        orig_val = float(orig_totals[metric] or 0)
        dp_val = float(dp_totals[metric] or 0)
        
        if orig_val == 0:
            error_pct = 0.0 if dp_val == 0 else float('inf')
        else:
            error_pct = abs(dp_val - orig_val) / orig_val * 100
        
        # Determine status
        if error_pct < 5:
            status = "✅ EXCELLENT"
        elif error_pct < 10:
            status = "✅ GOOD"
        elif error_pct < 20:
            status = "⚠️ ACCEPTABLE"
        elif error_pct < 50:
            status = "⚠️ HIGH"
        else:
            status = "❌ TOO HIGH"
            all_passed = False
        
        results[metric] = {
            'original': orig_val,
            'dp': dp_val,
            'error_pct': error_pct,
            'status': status
        }
        
        logger.info(
            f"{metric:<25} {orig_val:>20,.0f} {dp_val:>20,.0f} "
            f"{error_pct:>11.2f}% {status:>8}"
        )
    
    # Summary
    logger.info("\n" + "=" * 70)
    logger.info("VERIFICATION SUMMARY")
    logger.info("=" * 70)
    
    avg_error = sum(r['error_pct'] for r in results.values()) / len(results)
    max_error = max(r['error_pct'] for r in results.values())
    
    logger.info(f"\n  Average Error: {avg_error:.2f}%")
    logger.info(f"  Maximum Error: {max_error:.2f}%")
    logger.info(f"  Cell Count Match: {'✅' if orig_cell_count == dp_cell_count else '❌'}")
    logger.info(f"    Original: {orig_cell_count:,}")
    logger.info(f"    DP: {dp_cell_count:,}")
    
    # Overall verdict
    logger.info("\n" + "=" * 70)
    if all_passed and max_error < 20:
        logger.info("✅ VERIFICATION PASSED")
        logger.info("   All error rates are within acceptable bounds (< 20%)")
        logger.info("   The DP fix appears to be working correctly.")
    elif max_error < 50:
        logger.info("⚠️ VERIFICATION PARTIAL")
        logger.info("   Some error rates are higher than ideal but acceptable.")
        logger.info("   Consider adjusting privacy budget or sensitivity parameters.")
    else:
        logger.info("❌ VERIFICATION FAILED")
        logger.info("   Error rates are still too high (> 50%).")
        logger.info("   The fix may not have resolved the issue completely.")
        logger.info("   Please check:")
        logger.info("   1. Is the fix applied correctly?")
        logger.info("   2. Are sensitivity parameters reasonable?")
        logger.info("   3. Is privacy budget sufficient?")
    logger.info("=" * 70)
    
    return {
        'passed': all_passed and max_error < 20,
        'average_error': avg_error,
        'max_error': max_error,
        'cell_count_match': orig_cell_count == dp_cell_count,
        'results': results
    }


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Verify DP output quality after fix"
    )
    parser.add_argument(
        '--original',
        type=str,
        required=True,
        help='Path to original transaction data (Parquet)'
    )
    parser.add_argument(
        '--dp-output',
        type=str,
        required=True,
        help='Path to DP-protected output directory'
    )
    
    args = parser.parse_args()
    
    try:
        result = verify_dp_output(
            original_data_path=args.original,
            dp_output_path=args.dp_output
        )
        
        # Exit with appropriate code
        sys.exit(0 if result['passed'] else 1)
        
    except Exception as e:
        logger.error(f"Verification failed: {e}", exc_info=True)
        sys.exit(1)

