"""Test script to verify configuration loading."""
from core.config import Config

print("="*60)
print("Configuration Loading Test")
print("="*60)

# Test 1: Check defaults
print("\n[TEST 1] Default values (no INI file):")
config1 = Config()
print(f"  contribution_bound_method: {config1.privacy.contribution_bound_method}")
print(f"  contribution_bound_per_group: {config1.privacy.contribution_bound_per_group}")
print(f"  contribution_bound_percentile: {config1.privacy.contribution_bound_percentile}")
print(f"  mcc_num_groups: {config1.privacy.mcc_num_groups}")
print(f"  suppression_threshold: {config1.privacy.suppression_threshold}")

# Test 2: Load from INI
print("\n[TEST 2] Loading from configs/default.ini:")
try:
    config2 = Config.from_ini('configs/default.ini')
    print(f"  contribution_bound_method: {config2.privacy.contribution_bound_method}")
    print(f"  contribution_bound_per_group: {config2.privacy.contribution_bound_per_group}")
    print(f"  contribution_bound_percentile: {config2.privacy.contribution_bound_percentile}")
    print(f"  mcc_num_groups: {config2.privacy.mcc_num_groups}")
    print(f"  suppression_threshold: {config2.privacy.suppression_threshold}")
except FileNotFoundError as e:
    print(f"  ERROR: {e}")
except Exception as e:
    print(f"  ERROR: {type(e).__name__}: {e}")

print("\n" + "="*60)
print("Test Complete")
print("="*60)

