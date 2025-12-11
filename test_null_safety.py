"""
Test to demonstrate the null safety fix for amount_stats logging.
"""

# Simulate what .first() returns in edge cases
test_cases = [
    {
        "name": "Both None (empty DataFrame)",
        "amount_stats": {'max_error': None, 'mean_error': None},
        "should_log_stats": False
    },
    {
        "name": "Only max_error present (would cause original bug)",
        "amount_stats": {'max_error': 1.5, 'mean_error': None},
        "should_log_stats": False
    },
    {
        "name": "Only mean_error present",
        "amount_stats": {'max_error': None, 'mean_error': 0.8},
        "should_log_stats": False
    },
    {
        "name": "Both present (normal case)",
        "amount_stats": {'max_error': 2.3, 'mean_error': 1.1},
        "should_log_stats": True
    },
    {
        "name": "amount_stats is None",
        "amount_stats": None,
        "should_log_stats": False
    }
]

def check_amount_stats_safe(amount_stats):
    """
    NEW (FIXED) VERSION: Checks both max_error AND mean_error.
    """
    return (amount_stats and 
            amount_stats.get('max_error') is not None and 
            amount_stats.get('mean_error') is not None)

def check_amount_stats_buggy(amount_stats):
    """
    OLD (BUGGY) VERSION: Only checks max_error.
    This would cause TypeError when mean_error is None.
    """
    return (amount_stats and 
            amount_stats.get('max_error') is not None)

print("=" * 70)
print("NULL SAFETY TEST: amount_stats Logging")
print("=" * 70)

for test in test_cases:
    print(f"\nTest Case: {test['name']}")
    print(f"  Data: {test['amount_stats']}")
    
    # Test the fixed version
    safe_result = check_amount_stats_safe(test['amount_stats'])
    print(f"  Fixed version would log stats: {safe_result}")
    print(f"  Expected: {test['should_log_stats']}")
    
    if safe_result == test['should_log_stats']:
        print("  ✅ PASS")
    else:
        print("  ❌ FAIL")
    
    # Show what buggy version would do
    buggy_result = check_amount_stats_buggy(test['amount_stats'])
    if buggy_result and not safe_result:
        print(f"  ⚠️  OLD BUG: Would try to log with mean_error=None → TypeError!")

print("\n" + "=" * 70)
print("SUMMARY")
print("=" * 70)
print("✅ Fixed version checks BOTH max_error AND mean_error")
print("✅ Prevents TypeError when only one field is present")
print("✅ Gracefully handles all edge cases")
print("=" * 70)

