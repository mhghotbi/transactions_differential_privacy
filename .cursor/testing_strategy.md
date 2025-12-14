# Testing Strategy for Refactoring

## Critical Tests (Must Pass After Any Refactoring)

### 1. Province Invariant Test

**Purpose**: Verify province count totals are exact (0% error)

```python
def test_province_invariants_exact(pipeline_result, original_data):
    """CRITICAL: Province totals must match exactly."""
    protected_df = spark.read.parquet(pipeline_result['output_path'])
    
    # Compute province totals from protected data
    protected_totals = protected_df.groupBy('province_idx').agg(
        F.sum('transaction_count').alias('protected_count')
    )
    
    # Compute original province totals
    original_totals = original_data.groupBy('province_idx').agg(
        F.sum('transaction_count').alias('original_count')
    )
    
    # Join and compare
    comparison = protected_totals.join(original_totals, 'province_idx')
    comparison = comparison.withColumn(
        'error_pct',
        F.abs(F.col('protected_count') - F.col('original_count')) / 
        F.greatest(F.col('original_count'), F.lit(1)) * 100
    )
    
    # CRITICAL: Error must be 0%
    max_error = comparison.agg(F.max('error_pct')).first()[0]
    assert max_error == 0.0, f"Province invariant violated: max error = {max_error}%"
```

### 2. Ratio Bounds Test

**Purpose**: Verify ratios stay within context-specific bounds

```python
def test_ratio_bounds(pipeline_result, bounds_df):
    """Verify avg_amount and tx_per_card within bounds."""
    protected_df = spark.read.parquet(pipeline_result['output_path'])
    
    # Join with bounds
    df_with_bounds = protected_df.join(
        F.broadcast(bounds_df),
        ['mcc_idx', 'city_idx', 'weekday'],
        'left'
    )
    
    # Compute actual ratios
    df_with_ratios = df_with_bounds.filter(
        F.col('transaction_count') > 0
    ).withColumn(
        'actual_avg_amount',
        F.col('total_amount') / F.col('transaction_count')
    ).withColumn(
        'actual_tx_per_card',
        F.col('transaction_count') / F.greatest(F.col('unique_cards'), F.lit(1))
    )
    
    # Check violations
    avg_amt_violations = df_with_ratios.filter(
        (F.col('actual_avg_amount') < F.col('avg_amount_min')) |
        (F.col('actual_avg_amount') > F.col('avg_amount_max'))
    ).count()
    
    tx_card_violations = df_with_ratios.filter(
        (F.col('actual_tx_per_card') < F.col('tx_per_card_min')) |
        (F.col('actual_tx_per_card') > F.col('tx_per_card_max'))
    ).count()
    
    assert avg_amt_violations == 0, f"avg_amount violations: {avg_amt_violations}"
    assert tx_card_violations == 0, f"tx_per_card violations: {tx_card_violations}"
```

### 3. Logical Consistency Test

**Purpose**: Verify consistency rules

```python
def test_logical_consistency(pipeline_result):
    """Verify consistency: count=0 → cards=0, amount=0."""
    protected_df = spark.read.parquet(pipeline_result['output_path'])
    
    inconsistent = protected_df.filter(
        # Rule 1: count=0 → cards=0
        ((F.col('transaction_count') == 0) & (F.col('unique_cards') > 0)) |
        # Rule 2: count=0 → amount=0
        ((F.col('transaction_count') == 0) & (F.col('total_amount') > 0)) |
        # Rule 3: count>0 → cards>=1
        ((F.col('transaction_count') > 0) & (F.col('unique_cards') == 0)) |
        # Rule 4: cards <= count
        (F.col('unique_cards') > F.col('transaction_count'))
    )
    
    violation_count = inconsistent.count()
    assert violation_count == 0, f"Logical inconsistencies: {violation_count}"
```

### 4. Noise Application Test

**Purpose**: Verify noise is actually applied

```python
def test_noise_applied(original_df, protected_df):
    """Verify noise was applied (values differ from original)."""
    # Compare at cell level (province, city, mcc, day)
    comparison = original_df.join(
        protected_df,
        ['province_idx', 'city_idx', 'mcc_idx', 'day_idx'],
        'inner'
    ).withColumn(
        'count_diff_pct',
        F.abs(F.col('protected_count') - F.col('original_count')) /
        F.greatest(F.col('original_count'), F.lit(1)) * 100
    )
    
    # At least some cells should have noise
    noisy_cells = comparison.filter(F.col('count_diff_pct') > 0.1).count()
    total_cells = comparison.count()
    noise_rate = noisy_cells / total_cells
    
    assert noise_rate > 0.5, f"Too few cells have noise: {noise_rate:.1%}"
```

## Unit Tests (No Spark Required)

### Test Configuration Loading

```python
def test_config_loading():
    """Test configuration loads correctly."""
    config = Config.from_ini('configs/default.ini')
    assert config.privacy.noise_level == 0.15
    assert config.validate() == True
```

### Test Geography Loading

```python
def test_geography_loading():
    """Test geography hierarchy loads correctly."""
    geo = Geography.from_csv('data/city_province.csv')
    assert geo.num_provinces > 0
    assert geo.num_cities > 0
    assert 'تهران' in geo.city_to_province
```

### Test Bounded Contribution

```python
def test_bounded_contribution():
    """Test K computation."""
    # Use small test data
    test_data = create_test_transactions(1000)
    k = compute_k_from_data(test_data, percentile=0.95)
    assert k > 0
    assert k <= 100  # Reasonable bound
```

## Integration Tests (Requires Spark)

### Test Full Pipeline

```python
def test_full_pipeline():
    """Test complete pipeline execution."""
    config = Config.from_ini('configs/default.ini')
    config.data.input_path = 'data/test_transactions.parquet'
    config.data.output_path = 'data/test_output'
    
    pipeline = DPPipeline(config)
    result = pipeline.run()
    
    assert result['success'] == True
    assert result['total_records'] > 0
    assert os.path.exists(result['output_path'])
```

### Test Preprocessing

```python
def test_preprocessing(spark, test_data):
    """Test preprocessing produces correct histogram."""
    preprocessor = TransactionPreprocessor(spark, config, geography)
    histograms = preprocessor.process(test_data)
    
    # Verify structure
    assert histograms.df.count() > 0
    assert 'province_idx' in histograms.df.columns
    assert 'transaction_count' in histograms.df.columns
```

### Test Engine

```python
def test_engine_noise(spark, test_histogram):
    """Test engine applies noise correctly."""
    engine = TopDownSparkEngine(spark, config, geography)
    protected = engine.run(test_histogram)
    
    # Verify structure preserved
    assert protected.df.count() == test_histogram.df.count()
    
    # Verify province invariants
    test_province_invariants_exact(protected, test_histogram)
```

## Edge Case Tests

### Test Small Counts

```python
def test_small_counts():
    """Test handling of cells with very small counts."""
    # Create data with many cells having count=1
    small_data = create_test_data_with_small_counts()
    result = run_pipeline(small_data)
    
    # Verify no negative counts
    protected_df = spark.read.parquet(result['output_path'])
    negative = protected_df.filter(F.col('transaction_count') < 0).count()
    assert negative == 0
```

### Test Sparse Data

```python
def test_sparse_data():
    """Test handling of sparse contexts (few samples)."""
    # Create data with many sparse contexts
    sparse_data = create_test_data_sparse()
    result = run_pipeline(sparse_data)
    
    # Verify bounds are reasonable for sparse contexts
    # (should use defaults or mean-based bounds)
    ...
```

### Test Large Counts

```python
def test_large_counts():
    """Test handling of cells with very large counts."""
    # Create data with some cells having count > 1M
    large_data = create_test_data_with_large_counts()
    result = run_pipeline(large_data)
    
    # Verify no overflow
    protected_df = spark.read.parquet(result['output_path'])
    max_count = protected_df.agg(F.max('transaction_count')).first()[0]
    assert max_count < 2**31  # int32 max
```

## Performance Tests

### Test Pipeline Performance

```python
def test_pipeline_performance():
    """Test pipeline completes in reasonable time."""
    import time
    
    config = Config.from_ini('configs/default.ini')
    config.data.input_path = 'data/large_test.parquet'  # 1M rows
    
    start = time.time()
    pipeline = DPPipeline(config)
    result = pipeline.run()
    duration = time.time() - start
    
    # Should complete in < 5 minutes for 1M rows
    assert duration < 300, f"Pipeline too slow: {duration:.1f}s"
```

## Regression Tests

### Test Output Format

```python
def test_output_format(pipeline_result):
    """Test output has correct schema."""
    protected_df = spark.read.parquet(pipeline_result['output_path'])
    
    expected_columns = [
        'province_idx', 'city_idx', 'mcc_idx', 'day_idx',
        'transaction_count', 'unique_cards', 'total_amount'
    ]
    
    for col in expected_columns:
        assert col in protected_df.columns, f"Missing column: {col}"
    
    # Verify types
    assert protected_df.schema['transaction_count'].dataType == LongType()
    assert protected_df.schema['unique_cards'].dataType == LongType()
    assert protected_df.schema['total_amount'].dataType == LongType()
```

## Test Data Generation

### Helper Functions

```python
def create_test_transactions(n: int) -> DataFrame:
    """Create synthetic transaction data for testing."""
    # Generate test data with known structure
    ...

def create_test_histogram(spark) -> SparkHistogram:
    """Create test histogram with known values."""
    # Create DataFrame with known province totals
    ...
```

## Running Tests

### Unit Tests (No Spark)
```bash
python -m pytest tests/test_no_spark.py -v
```

### Integration Tests (Requires Spark)
```bash
python -m pytest tests/test_dp_correctness.py -v
```

### All Tests
```bash
python -m pytest tests/ -v
```

## Continuous Integration

Tests should run:
- On every commit
- Before merging PRs
- After refactoring any critical component

**Critical tests must pass**: Province invariants, ratio bounds, logical consistency.

