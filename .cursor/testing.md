# Testing Guidelines

Testing strategy and guidelines for the Transaction SDC System.

---

## Testing Philosophy

- **Unit Tests**: Test individual components without Spark (fast, isolated)
- **Integration Tests**: Test full pipeline with Spark (slower, requires Spark)
- **SDC Validation**: Verify plausibility bounds, invariants, and ratios
- **Performance Tests**: Verify scalability with large datasets

---

## Test Structure

### Unit Tests (`tests/test_no_spark.py`)
Tests that don't require Spark or Java:
- Configuration loading and validation
- Bounded contribution calculations
- Noise generation and clamping
- Ratio preservation logic
- Invariant enforcement

**Run**: `python tests/test_no_spark.py`

### SDC Correctness Tests (`tests/test_sdc_correctness.py`)
Tests that verify SDC mechanisms:
- Province invariants preserved exactly
- Context-aware bounds applied correctly
- Ratios remain plausible
- No negative counts
- Suppression thresholds applied

**Run**: `python tests/test_sdc_correctness.py`

---

## Test Categories

### 1. Configuration Tests
**Purpose**: Verify configuration loading and validation

**Examples**:
- Load config from INI file
- Validate noise_level in valid range
- Validate contribution_bound_method is valid option
- Validate suppression_threshold >= 0
- Validate mcc_cap_percentile in (0, 100]

**Pattern**:
```python
def test_config_validation():
    config = Config()
    config.privacy.noise_level = -0.1  # Invalid
    with pytest.raises(ValueError):
        config.validate()
```

---

### 2. Bounded Contribution Tests
**Purpose**: Verify K computation and application

**Examples**:
- Transaction-weighted percentile method
- IQR method
- Fixed value method
- K applied correctly to limit transactions

**Pattern**:
```python
def test_bounded_contribution():
    calculator = BoundedContributionCalculator(method="transaction_weighted_percentile")
    K = calculator.compute_K(df)
    assert K > 0
    bounded_df = calculator.apply(df, K)
    assert bounded_df.groupBy('card', 'cell').count().max() <= K
```

---

### 3. Noise Generation Tests
**Purpose**: Verify multiplicative jitter and clamping

**Examples**:
- Multiplicative jitter preserves ratios
- Noise clamped to plausibility bounds
- Seed produces reproducible results
- Noise level affects magnitude correctly

**Pattern**:
```python
def test_multiplicative_jitter():
    original_count = 1000
    noise_level = 0.15
    noisy_count = apply_jitter(original_count, noise_level, seed=42)
    # Check ratio preserved
    assert abs(noisy_count / original_count - 1.0) < 0.5  # Within 50%
    # Check clamped to bounds
    assert min_bound <= noisy_count <= max_bound
```

---

### 4. Invariant Tests
**Purpose**: Verify province totals preserved exactly

**Examples**:
- Province totals unchanged after noise
- City values sum to exact province totals
- Post-processing maintains invariants

**Pattern**:
```python
def test_province_invariants():
    original_province_total = compute_province_total(df)
    noisy_df = apply_sdc(df)
    noisy_province_total = compute_province_total(noisy_df)
    assert original_province_total == noisy_province_total  # Exact match
```

---

### 5. Ratio Preservation Tests
**Purpose**: Verify ratios remain plausible

**Examples**:
- avg_amount stays within bounds
- tx_per_card stays within bounds
- Ratios preserved after noise application

**Pattern**:
```python
def test_ratio_preservation():
    noisy_df = apply_sdc(df)
    for row in noisy_df:
        avg_amount = row.total_amount / row.transaction_count
        assert is_plausible_avg_amount(avg_amount, row.mcc, row.city, row.weekday)
        
        tx_per_card = row.transaction_count / row.unique_cards
        assert is_plausible_tx_per_card(tx_per_card, row.mcc, row.city, row.weekday)
```

---

### 6. Context-Aware Bounds Tests
**Purpose**: Verify bounds computed per stratum

**Examples**:
- Bounds differ by (MCC, City, Weekday)
- Bounds computed from data correctly
- Noise clamped to stratum-specific bounds

**Pattern**:
```python
def test_context_aware_bounds():
    bounds_small_city_restaurant = compute_bounds("Restaurant", "SmallCity", "Weekday")
    bounds_large_city_gas = compute_bounds("Gas", "LargeCity", "Weekend")
    # Different contexts should have different bounds
    assert bounds_small_city_restaurant != bounds_large_city_gas
```

---

### 7. Suppression Tests
**Purpose**: Verify suppression thresholds applied

**Examples**:
- Cells below threshold are suppressed
- Suppression flag set correctly
- Suppressed values handled appropriately

**Pattern**:
```python
def test_suppression():
    threshold = 10
    noisy_df = apply_sdc(df, suppression_threshold=threshold)
    suppressed = noisy_df.filter(F.col("is_suppressed") == True)
    assert suppressed.select("transaction_count").rdd.map(lambda r: r[0]).min() < threshold
```

---

### 8. Integration Tests
**Purpose**: Test full pipeline end-to-end

**Examples**:
- Full pipeline runs successfully
- Output schema matches expected
- All SDC mechanisms applied correctly

**Pattern**:
```python
def test_full_pipeline():
    config = Config.from_ini("configs/default.ini")
    config.data.input_path = "test_data.parquet"
    config.data.output_path = "test_output/"
    pipeline = DPPipeline(config)
    result = pipeline.run()
    assert result.success
    assert os.path.exists(result.output_path)
```

---

## Test Data

### Small Test Dataset
- **Size**: ~10K transactions
- **Purpose**: Fast unit tests
- **Location**: `tests/data/small_test.parquet`

### Medium Test Dataset
- **Size**: ~1M transactions
- **Purpose**: Integration tests
- **Location**: `tests/data/medium_test.parquet`

### Large Test Dataset
- **Size**: ~100M transactions
- **Purpose**: Performance tests
- **Location**: `tests/data/large_test.parquet` (optional)

---

## Running Tests

### Unit Tests (No Spark)
```bash
python tests/test_no_spark.py
```

### SDC Correctness Tests
```bash
python tests/test_sdc_correctness.py
```

### All Tests
```bash
pytest tests/ -v
```

### With Coverage
```bash
pytest tests/ --cov=core --cov=engine --cov-report=html
```

---

## Test Best Practices

### 1. Use Fixtures for Common Setup
```python
@pytest.fixture
def sample_config():
    config = Config()
    config.privacy.noise_level = 0.15
    config.validate()
    return config
```

### 2. Test Edge Cases
- Empty datasets
- Single cell
- Extreme values
- Boundary conditions

### 3. Use Descriptive Test Names
```python
# ✅ Good
def test_province_invariants_preserved_exactly_after_noise():

# ❌ Bad
def test_invariants():
```

### 4. Test One Thing Per Test
```python
# ✅ Good - One assertion per test
def test_noise_level_validation():
    with pytest.raises(ValueError):
        config.privacy.noise_level = -0.1

# ❌ Bad - Multiple unrelated assertions
def test_config():
    assert config.noise_level == 0.15
    assert config.suppression_threshold == 10
    assert config.mcc_cap_percentile == 99.0
```

### 5. Use Seeds for Reproducibility
```python
def test_reproducible_noise():
    result1 = apply_jitter(value, noise_level, seed=42)
    result2 = apply_jitter(value, noise_level, seed=42)
    assert result1 == result2
```

---

## SDC Validation Checklist

Before considering tests passing, verify:
- [ ] Province invariants preserved exactly
- [ ] Context-aware bounds applied correctly
- [ ] Ratios remain plausible (avg_amount, tx_per_card)
- [ ] No negative counts
- [ ] Suppression thresholds applied
- [ ] Bounded contribution (K) applied correctly
- [ ] Noise reproducible with same seed

---

## Performance Testing

### Benchmark Tests
- Measure processing time for different dataset sizes
- Verify linear scaling with data size
- Check memory usage

### Example
```python
def test_performance_scaling():
    sizes = [1000, 10000, 100000]
    times = []
    for size in sizes:
        df = generate_test_data(size)
        start = time.time()
        apply_sdc(df)
        times.append(time.time() - start)
    # Verify sub-quadratic scaling
    assert times[2] / times[1] < 10  # Less than 10x for 10x data
```

---

## Continuous Integration

Tests should run automatically on:
- Pull requests
- Commits to main branch
- Nightly builds

**CI Configuration**: `.github/workflows/tests.yml` (if using GitHub Actions)

---

## Test Coverage Goals

- **Unit Tests**: >80% coverage
- **Integration Tests**: Cover main workflows
- **SDC Validation**: 100% of SDC mechanisms

---

## Debugging Failed Tests

1. **Check Logs**: Look for error messages in test output
2. **Reproduce Locally**: Run failing test in isolation
3. **Check Data**: Verify test data is correct
4. **Check Configuration**: Ensure test config is valid
5. **Check Spark**: For integration tests, verify Spark session initialized correctly

---

## Writing New Tests

When adding new functionality:
1. Write unit tests first (no Spark)
2. Add integration tests if needed
3. Add SDC validation tests for new SDC mechanisms
4. Update this document if adding new test categories

