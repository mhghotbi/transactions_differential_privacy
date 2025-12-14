# Anti-Patterns

Common mistakes and anti-patterns to avoid in the Transaction SDC System.

---

## ❌ Privacy & Security Anti-Patterns

### 1. Modifying Province Invariants
**Anti-Pattern**: Applying noise to province-level aggregates
```python
# ❌ WRONG - Province totals must be exact
province_total = province_total * (1 + noise_factor)
```

**Correct Pattern**:
```python
# ✅ CORRECT - Province totals are exact (invariant)
province_total = exact_province_total  # No noise
city_values = adjust_to_match_province(city_values, province_total)
```

**Why**: Province totals are public data (known from other sources). They must remain exact.

---

### 2. Using Global Noise Bounds
**Anti-Pattern**: Applying same plausibility bounds to all cells
```python
# ❌ WRONG - Global bounds don't account for context
min_bound = 0
max_bound = 1000000
noisy_value = np.clip(noisy_value, min_bound, max_bound)
```

**Correct Pattern**:
```python
# ✅ CORRECT - Context-aware bounds per (MCC, City, Weekday)
bounds = compute_bounds_per_stratum(mcc, city, weekday)
noisy_value = np.clip(noisy_value, bounds.min, bounds.max)
```

**Why**: Different contexts have different plausible ranges. Small city + Restaurant MCC ≠ Large city + Gas MCC.

---

### 3. Breaking Ratio Preservation
**Anti-Pattern**: Applying noise independently to counts and amounts
```python
# ❌ WRONG - Breaks ratio preservation
noisy_count = count * (1 + noise1)
noisy_amount = amount * (1 + noise2)
# avg_amount = noisy_amount / noisy_count may be implausible
```

**Correct Pattern**:
```python
# ✅ CORRECT - Preserve ratios
noisy_count = count * (1 + noise_factor)
noisy_amount = amount * (1 + noise_factor)  # Same factor
# avg_amount preserved automatically
# Then adjust if needed to stay within bounds
```

**Why**: Ratios (avg_amount, tx_per_card) must remain plausible for utility.

---

### 4. Logging Raw Transaction Data
**Anti-Pattern**: Logging individual transactions
```python
# ❌ WRONG - Privacy risk
logger.info(f"Transaction: {card_number} spent {amount} at {merchant}")
```

**Correct Pattern**:
```python
# ✅ CORRECT - Only log aggregated statistics
logger.info(f"Cell ({city}, {mcc}, {day}): count={count}, total={total}")
```

**Why**: Raw transaction data must never leave secure enclave or be logged.

---

## ❌ Performance Anti-Patterns

### 5. Using Pandas for Large Datasets
**Anti-Pattern**: Converting Spark DataFrame to Pandas for large data
```python
# ❌ WRONG - OOM risk for large datasets
df_pandas = df_spark.toPandas()  # Collects all data to driver
result = df_pandas.groupby('city').sum()
```

**Correct Pattern**:
```python
# ✅ CORRECT - Use Spark operations
result = df_spark.groupBy('city').sum()
# Or convert only if < 100K rows
if df_spark.count() < 100000:
    df_pandas = df_spark.toPandas()
```

**Why**: Pandas loads all data into driver memory. Spark distributes computation.

---

### 6. Collecting Large DataFrames
**Anti-Pattern**: Collecting entire DataFrame to driver
```python
# ❌ WRONG - OOM risk
all_rows = df.collect()  # Loads all data to driver
for row in all_rows:
    process(row)
```

**Correct Pattern**:
```python
# ✅ CORRECT - Use Spark operations or sample
df.foreach(lambda row: process(row))  # Distributed processing
# Or sample if needed
sample = df.sample(fraction=0.01)
```

**Why**: Collecting large DataFrames causes out-of-memory errors.

---

### 7. Creating Multiple Spark Sessions
**Anti-Pattern**: Creating new Spark session when one exists
```python
# ❌ WRONG - Unnecessary overhead
spark = SparkSession.builder.appName("New").getOrCreate()
# Later...
spark2 = SparkSession.builder.appName("Another").getOrCreate()
```

**Correct Pattern**:
```python
# ✅ CORRECT - Reuse existing session
existing = SparkSession.getActiveSession()
if existing:
    spark = existing
else:
    spark = SparkSession.builder.appName("App").getOrCreate()
```

**Why**: Multiple sessions waste resources and can cause conflicts.

---

## ❌ Code Quality Anti-Patterns

### 8. Skipping Configuration Validation
**Anti-Pattern**: Not validating configuration before use
```python
# ❌ WRONG - May fail later with cryptic error
config = Config.from_ini("config.ini")
pipeline = DPPipeline(config)  # May fail if config invalid
pipeline.run()
```

**Correct Pattern**:
```python
# ✅ CORRECT - Validate early
config = Config.from_ini("config.ini")
config.validate()  # Fails fast with clear error
pipeline = DPPipeline(config)
pipeline.run()
```

**Why**: Early validation provides clear error messages and prevents runtime failures.

---

### 9. Hardcoding Paths
**Anti-Pattern**: Hardcoding file paths in code
```python
# ❌ WRONG - Not configurable
df = spark.read.parquet("/fixed/path/to/data.parquet")
```

**Correct Pattern**:
```python
# ✅ CORRECT - Use configuration
df = spark.read.parquet(config.data.input_path)
```

**Why**: Hardcoded paths make code inflexible and hard to test.

---

### 10. Ignoring File Existence Checks
**Anti-Pattern**: Not checking if files exist before reading
```python
# ❌ WRONG - May fail with cryptic error
df = spark.read.parquet(input_path)  # FileNotFoundError if missing
```

**Correct Pattern**:
```python
# ✅ CORRECT - Check early
if not os.path.exists(input_path):
    raise FileNotFoundError(f"Input file not found: {input_path}")
df = spark.read.parquet(input_path)
```

**Why**: Early checks provide clear error messages and prevent wasted computation.

---

## ❌ SDC-Specific Anti-Patterns

### 11. Using Additive Noise Instead of Multiplicative
**Anti-Pattern**: Adding fixed noise amount
```python
# ❌ WRONG - Doesn't preserve ratios
noisy_value = original_value + np.random.normal(0, fixed_sigma)
```

**Correct Pattern**:
```python
# ✅ CORRECT - Multiplicative preserves ratios
noise_factor = np.random.normal(1.0, noise_level)
noisy_value = original_value * noise_factor
```

**Why**: Multiplicative noise preserves ratios naturally (avg_amount, tx_per_card).

---

### 12. Not Applying Bounded Contribution
**Anti-Pattern**: Allowing unlimited transactions per card per cell
```python
# ❌ WRONG - Outliers dominate statistics
df_agg = df.groupBy('city', 'mcc', 'day', 'card_number').agg(
    F.count('*').alias('count')
)
```

**Correct Pattern**:
```python
# ✅ CORRECT - Apply bounded contribution (K)
bounded = apply_bounded_contribution(df, K)
df_agg = bounded.groupBy('city', 'mcc', 'day', 'card_number').agg(
    F.count('*').alias('count')
)
```

**Why**: Bounded contribution prevents outliers from dominating statistics and improves utility.

---

### 13. Using Fixed K Instead of Data-Driven
**Anti-Pattern**: Using fixed K value without analyzing data
```python
# ❌ WRONG - May lose too much data or not bound enough
K = 5  # Fixed value
```

**Correct Pattern**:
```python
# ✅ CORRECT - Compute K from data
K = compute_transaction_weighted_percentile(df, percentile=99)
```

**Why**: Data-driven K adapts to actual distribution and minimizes data loss.

---

### 14. Not Preserving Ratios After Noise
**Anti-Pattern**: Applying noise without checking ratio plausibility
```python
# ❌ WRONG - May produce implausible ratios
noisy_count = count * (1 + noise1)
noisy_amount = amount * (1 + noise2)
# avg_amount = noisy_amount / noisy_count may be implausible
```

**Correct Pattern**:
```python
# ✅ CORRECT - Preserve ratios and validate
noisy_count = count * (1 + noise_factor)
noisy_amount = amount * (1 + noise_factor)
avg_amount = noisy_amount / noisy_count
if not is_plausible_ratio(avg_amount, mcc, city, weekday):
    adjust_to_plausible_range(noisy_count, noisy_amount)
```

**Why**: Ratios must remain plausible for utility preservation.

---

## Summary Checklist

Before submitting code, verify:
- [ ] Province invariants are preserved exactly
- [ ] Context-aware bounds are used (not global)
- [ ] Ratios are preserved (avg_amount, tx_per_card)
- [ ] No raw transaction data is logged
- [ ] Spark is used for large datasets (not Pandas)
- [ ] Configuration is validated before use
- [ ] File existence is checked before reading
- [ ] Multiplicative jitter is used (not additive)
- [ ] Bounded contribution (K) is applied
- [ ] K is computed from data (not fixed)

