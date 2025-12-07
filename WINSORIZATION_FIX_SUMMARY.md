# Winsorization-Invariant Mismatch Fix - Implementation Summary

## Critical Scientific Issue Resolved

### The Problem

**Before this fix**, the system had a fundamental scientific error:

1. Raw transactions were winsorized (e.g., 20M IRR → 10M IRR cap)
2. Aggregation used winsorized amounts
3. Province invariants computed from winsorized totals (18M)
4. DP noise added to cells
5. NNLS ensured cities sum to winsorized province totals (18M)

**❌ CRITICAL ISSUE**: Province invariants (18M) didn't match publicly published totals (28M original)!

### Example Scenario

```
Province Tehran - Original Data:
  Card A: 20,000,000 IRR (above 99th percentile)
  Card B:  5,000,000 IRR
  Card C:  3,000,000 IRR
  Total:  28,000,000 IRR  ← Publicly published

After Winsorization (cap = 10M):
  Card A: 10,000,000 IRR (capped)
  Card B:  5,000,000 IRR
  Card C:  3,000,000 IRR
  Total:  18,000,000 IRR  ← OLD system invariant

Mismatch: 28M (public) ≠ 18M (our invariants) ❌
```

## The Solution

**Winsorization serves TWO distinct purposes**:

1. **Sensitivity Reduction** (for DP noise calibration) → Use winsorized amounts
2. **Invariant Computation** (must match public data) → Use ORIGINAL amounts

**New correct flow**:
1. Track BOTH original and winsorized amounts during aggregation
2. Compute province invariants from **original amounts** (match public data)
3. Compute sensitivity from **winsorized amounts** (bound noise magnitude)
4. Add noise to cells (using winsorized sensitivity)
5. NNLS post-processing to match **original invariants**

## Implementation Details

### Files Modified

#### 1. Schema Enhancement (`schema/histogram.py`)

**Changes**:
- Added `total_amount_original` to `QUERIES` list
- Created `OUTPUT_QUERIES` list (without `total_amount_original`)
- Added `drop_original_amounts()` method
- Updated `summary()` to differentiate both amounts

**Key Addition**:
```python
# All queries including temporary ones (during preprocessing/DP)
QUERIES = ['transaction_count', 'unique_cards', 'total_amount', 'total_amount_original']

# Output queries (after DP processing, without temporary fields)
OUTPUT_QUERIES = ['transaction_count', 'unique_cards', 'total_amount']

def drop_original_amounts(self) -> None:
    """Drop total_amount_original from histogram after DP processing."""
    if 'total_amount_original' in self.data:
        del self.data['total_amount_original']
```

#### 2. Preprocessing - Dual Aggregation (`reader/preprocessor.py`)

**Changes**:
- Modified `_aggregate_to_histogram()` to compute both sums in single pass

**Key Change**:
```python
# BEFORE (only winsorized)
agg_df = df.groupBy(...).agg(
    F.sum('amount_winsorized').alias('total_amount')  # ❌
)

# AFTER (dual tracking - no extra cost!)
agg_df = df.groupBy(...).agg(
    F.sum('amount_winsorized').alias('total_amount'),         # For sensitivity
    F.sum('amount').alias('total_amount_original')             # For invariants ✓
)
```

**Performance**: No extra cost - both sums computed in single aggregation pass.

#### 3. Distributed Preprocessing (`reader/preprocessor_distributed.py`)

**Changes**:
- Modified `_aggregate_distributed()` for dual sums
- Updated `apply_noise_hierarchical()` to use original amounts for invariants
- Added cleanup to drop temporary fields before output

**Key Changes**:
```python
# Aggregation with dual amounts
agg_df = df.groupBy(...).agg(
    F.sum('amount_capped').alias('total_amount'),           # For DP
    F.sum('amount').alias('total_amount_original')          # For invariants
)

# Invariant computation uses ORIGINAL
national_monthly = df.agg(
    F.sum('total_amount_original').alias('total_amount_national_monthly')
).first()

# Cleanup before output
df = df.drop('total_amount_original', 'amount_capped')
```

#### 4. DP Engine - Correct Invariants (`engine/topdown.py`)

**Changes**:
- Modified `_compute_province_month_invariants()` to use original amounts
- Added `drop_original_amounts()` call before returning results

**Critical Fix**:
```python
def _compute_province_month_invariants(self, histogram):
    """
    CRITICAL FIX: Uses ORIGINAL (unwinsorized) amounts for total_amount invariants
    to match publicly published data.
    """
    for query in ['transaction_count', 'unique_cards', 'total_amount']:
        if query == 'total_amount' and 'total_amount_original' in histogram.data:
            # ✓ Use ORIGINAL amounts for invariants
            data = histogram.get_query_array('total_amount_original')
            logger.info("Using ORIGINAL (unwinsorized) amounts for total_amount invariants")
        else:
            data = histogram.get_query_array(query)
        
        # Compute province totals from correct source
        province_month_totals = np.sum(data, axis=(1, 2, 3))
        self._province_month_invariants[query] = province_month_totals
```

### 5. Validation Tests (`tests/test_winsorization_invariants.py`, `validate_fix.py`)

**Created comprehensive tests**:
- Dual amount support verification
- Drop functionality test
- **CRITICAL**: Invariant computation from original amounts
- Summary display test

## Scientific Correctness Verification

### Test Case

```python
# Input
Transaction 1: 20M IRR (above 99th percentile)
Transaction 2: 5M IRR
Transaction 3: 3M IRR

# After winsorization (cap = 10M)
amount_winsorized = [10M, 5M, 3M]  # For DP noise calibration
amount_original   = [20M, 5M, 3M]  # For invariants

# Province invariant (for NNLS)
invariant = sum(amount_original) = 28M ✓ Matches public data

# Sensitivity (for noise calibration)
sensitivity = f(winsorization_cap = 10M) ✓ Bounded noise

# City-level after DP + NNLS
City 1: 14M (noisy)
City 2: 14M (noisy)
NNLS ensures: 14M + 14M = 28M ✓ Matches original invariant
```

### Validation Checklist

✅ Province invariants match ORIGINAL (unwinsorized) totals  
✅ DP sensitivity uses WINSORIZED amounts for bounded noise  
✅ Single aggregation pass (memory efficient)  
✅ No extra shuffles (computationally efficient)  
✅ Temporary fields dropped from final output  
✅ Summary clearly differentiates both amounts during processing  

## Memory & Performance Impact

### Memory Efficiency
- **Single extra column** during aggregation (~13M cells, not 4.5B rows)
- Only one additional `int64` array in histogram structure
- Dropped before final output (zero impact on output size)

### Computational Efficiency
- **No extra aggregation passes** (both sums in same `groupBy`)
- **No extra shuffles** (computed together)
- **Zero overhead** in NNLS or noise injection
- Cleanup is O(1) operation

## Usage Notes

### For Users

When running the DP pipeline, you'll now see:

```
Step 1: Computing monthly invariants (EXACT)...
  CRITICAL: Using ORIGINAL (unwinsorized) amounts for total_amount invariants
  National monthly invariants (from ORIGINAL amounts):
    total_amount: 28,000,000
```

This confirms invariants match publicly published data.

### For Developers

During preprocessing, the histogram contains both:
- `total_amount`: Winsorized amounts (for DP sensitivity)
- `total_amount_original`: Original amounts (for invariants)

After DP processing, only `total_amount` remains in output.

## Mathematical Correctness

### Why This Fix is Essential

The province-level data published publicly contains **unwinsorized totals**. If our DP-protected city-level data sums to **winsorized province totals**, there's a fundamental inconsistency:

```
Public Data:           28M IRR (province total)
DP Protected Cities:   Sum to 18M IRR
                       ❌ INCONSISTENT!
```

With this fix:

```
Public Data:           28M IRR (province total)
DP Protected Cities:   Sum to 28M IRR via NNLS
                       ✓ CONSISTENT!
```

### Privacy Guarantees Maintained

- Winsorization **still** bounds sensitivity for DP noise
- Noise magnitude calibrated to **winsorized caps**
- Privacy budget calculations **unchanged**
- User-level DP guarantees **preserved**

The fix only ensures invariants match public data—it doesn't weaken privacy protections.

## Files Changed Summary

| File | Lines Changed | Purpose |
|------|--------------|---------|
| `schema/histogram.py` | ~30 | Add dual amount support + drop method |
| `reader/preprocessor.py` | ~10 | Dual aggregation (original + winsorized) |
| `reader/preprocessor_distributed.py` | ~40 | Dual aggregation + invariant computation + cleanup |
| `engine/topdown.py` | ~25 | Use original for invariants + cleanup |
| `tests/test_winsorization_invariants.py` | +140 (new) | Comprehensive validation tests |
| `validate_fix.py` | +80 (new) | Quick validation script |

**Total**: ~325 lines changed/added across 6 files

## Conclusion

This fix resolves a **critical scientific correctness issue** where province invariants didn't match publicly published data due to winsorization. The implementation:

- ✅ **Scientifically correct**: Invariants now match public totals
- ✅ **Memory efficient**: Single extra column during processing
- ✅ **Computationally efficient**: No extra aggregation passes
- ✅ **Privacy preserving**: DP guarantees unchanged
- ✅ **Clean output**: Temporary fields removed before final output

**Status**: All changes implemented and validated. Ready for testing with real data.

