# Refactoring Guidelines

## What Can Be Refactored

### ✅ Safe to Refactor

1. **Code Organization**
   - Extract helper functions from long methods
   - Split large classes into smaller, focused classes
   - Consolidate duplicate code
   - Improve naming for clarity

2. **Performance Optimizations**
   - Optimize Spark operations (joins, aggregations)
   - Reduce unnecessary shuffles
   - Improve caching strategy
   - Optimize DataFrame operations

3. **Code Quality**
   - Add type hints
   - Improve error messages
   - Add docstrings
   - Refactor complex conditionals
   - Extract magic numbers to constants

4. **Testing Infrastructure**
   - Add unit tests for isolated functions
   - Improve test coverage
   - Add integration tests
   - Mock Spark for faster tests

5. **Configuration**
   - Simplify configuration structure
   - Add validation rules
   - Improve configuration loading

### ⚠️ Refactor with Caution

1. **Noise Application Logic** (`engine/topdown_spark.py`)
   - Can refactor for clarity, but preserve exact algorithm
   - Must maintain province invariant preservation
   - Must preserve ratio validation logic
   - Test thoroughly after changes

2. **Scaling Operations** (Phase 6)
   - Can optimize, but must preserve exact province invariants
   - Must maintain proportional scaling of count/cards/amount
   - Verify 0% error on province totals after refactoring

3. **Controlled Rounding** (Phase 9)
   - Can refactor for clarity, but preserve exact algorithm
   - Must maintain province count invariants exactly
   - Must preserve ratio preservation during rounding
   - Test with edge cases (small counts, rounding up/down)

4. **Plausibility Bounds** (`core/plausibility_bounds.py`)
   - Can optimize computation, but preserve percentile logic
   - Must maintain context-specific bounds (MCC, City, Weekday)

## What MUST NOT Be Refactored

### ❌ Critical - Do Not Change

1. **Province Invariant Logic**
   - **NEVER** modify Phase 6 scaling without ensuring exact province totals
   - **NEVER** modify Phase 9 rounding without maintaining invariants
   - **NEVER** remove validation in Phase 10
   - These are the core correctness guarantees

2. **Noise Mechanism**
   - Multiplicative jitter formula: `noise_factor = 1 + noise_level * (uniform - 0.5) * 2`
   - Minimum deviation enforcement (`min_noise_factor_deviation`)
   - Independent noise for count/cards/amount (different seeds)

3. **Ratio Preservation**
   - avg_amount and tx_per_card validation logic
   - Proportional scaling when count changes
   - Bounds checking per context

4. **Data Flow Order**
   - Phase 1: Compute invariants (MUST be first)
   - Phase 6: Scale to invariants (MUST be after noise)
   - Phase 9: Controlled rounding (MUST be after scaling)
   - Phase 10: Validation (MUST be last)

## Refactoring Checklist

Before refactoring any component:

- [ ] Understand the component's role in the pipeline
- [ ] Identify all dependencies (what calls it, what it calls)
- [ ] Review related tests
- [ ] Check if component maintains invariants
- [ ] Plan how to verify correctness after refactoring

After refactoring:

- [ ] Run all existing tests
- [ ] Verify province invariants are exact (0% error)
- [ ] Check ratio bounds are respected
- [ ] Verify logical consistency (count=0 → cards=0, amount=0)
- [ ] Test with edge cases (small counts, large counts, sparse data)
- [ ] Check performance hasn't degraded significantly

## Common Refactoring Patterns

### Pattern 1: Extract Helper Function

**Before:**
```python
# Long method with complex logic
def apply_noise(self, df):
    # 50 lines of noise application logic
    ...
```

**After:**
```python
def apply_noise(self, df):
    df = self._compute_noise_factors(df)
    df = self._apply_multiplicative_jitter(df)
    df = self._enforce_minimum_deviation(df)
    return df

def _compute_noise_factors(self, df):
    # Extracted logic
    ...

def _apply_multiplicative_jitter(self, df):
    # Extracted logic
    ...
```

### Pattern 2: Extract Configuration Constants

**Before:**
```python
bounds_df = bounds_df.withColumn(
    'count_min',
    F.when(F.col('sample_count') < 3, ...)  # Magic number
)
```

**After:**
```python
MIN_SAMPLES_FOR_BOUNDS = 3

bounds_df = bounds_df.withColumn(
    'count_min',
    F.when(F.col('sample_count') < MIN_SAMPLES_FOR_BOUNDS, ...)
)
```

### Pattern 3: Simplify Complex Conditionals

**Before:**
```python
if (count > 0 and cards > 0 and amount > 0 and 
    avg_amount >= min_avg and avg_amount <= max_avg and
    tx_per_card >= min_tx and tx_per_card <= max_tx):
    # ...
```

**After:**
```python
def _is_valid_cell(count, cards, amount, avg_amount, tx_per_card, bounds):
    if count <= 0:
        return False
    if not (bounds.avg_amount_min <= avg_amount <= bounds.avg_amount_max):
        return False
    if not (bounds.tx_per_card_min <= tx_per_card <= bounds.tx_per_card_max):
        return False
    return True

if _is_valid_cell(count, cards, amount, avg_amount, tx_per_card, bounds):
    # ...
```

## Testing After Refactoring

### Critical Tests

1. **Province Invariant Test**
   ```python
   # After pipeline run
   province_totals = df.groupBy('province_idx').agg(
       F.sum('transaction_count').alias('sum_count')
   )
   # Verify sum_count == invariant_count exactly (0% error)
   ```

2. **Ratio Bounds Test**
   ```python
   # Verify all ratios within bounds
   df_with_ratios = df.withColumn('avg_amount', F.col('total_amount') / F.col('transaction_count'))
   violations = df_with_ratios.filter(
       (F.col('avg_amount') < F.col('avg_amount_min')) |
       (F.col('avg_amount') > F.col('avg_amount_max'))
   )
   assert violations.count() == 0
   ```

3. **Logical Consistency Test**
   ```python
   # Verify consistency rules
   inconsistent = df.filter(
       ((F.col('transaction_count') == 0) & (F.col('unique_cards') > 0)) |
       ((F.col('transaction_count') == 0) & (F.col('total_amount') > 0)) |
       (F.col('unique_cards') > F.col('transaction_count'))
   )
   assert inconsistent.count() == 0
   ```

## Performance Considerations

When refactoring for performance:

1. **Profile First**: Use Spark UI to identify bottlenecks
2. **Minimize Shuffles**: Prefer operations that don't require data movement
3. **Use Broadcasts**: For small lookup tables (< 100MB)
4. **Cache Strategically**: Only cache DataFrames reused 3+ times
5. **Partition Appropriately**: Repartition by join keys when needed

## Documentation Updates

After refactoring:

- [ ] Update docstrings if function signatures changed
- [ ] Update architecture.md if structure changed
- [ ] Update rules.md if patterns changed
- [ ] Add comments explaining non-obvious logic
- [ ] Document any new design decisions

## Code Review Focus Areas

When reviewing refactored code, pay special attention to:

1. **Invariant Preservation**: Does it still maintain province totals exactly?
2. **Ratio Preservation**: Are ratios still validated and adjusted correctly?
3. **Logical Consistency**: Are consistency rules still enforced?
4. **Error Handling**: Are errors handled appropriately?
5. **Performance**: Is it faster or at least not slower?
6. **Testability**: Is the code easier to test?

