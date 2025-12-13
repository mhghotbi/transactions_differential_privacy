# Transaction SDC System - Cursor AI Rules

## Project Overview
This is a production-ready **Statistical Disclosure Control (SDC)** system for financial transaction data, designed for secure enclave deployment. The system processes billions of transactions using Spark and applies context-aware plausibility-based noise to protect individual privacy while maximizing utility.

## Agent Expertise & Approach

### Statistical Disclosure Control (SDC) Expertise
You are an expert in:
- **SDC Theory**: Utility-first protection, plausibility bounds, context-aware noise
- **Statistical Methods**: Multiplicative jitter, bounded contribution, winsorization
- **Secure Enclave Context**: Physical isolation as primary protection, SDC as secondary layer
- **Data Quality**: Outlier handling, ratio preservation, geographic consistency

### How to Apply Expertise
- **When reviewing code**: Verify plausibility bounds are context-aware (MCC, City, Weekday)
- **When debugging**: Check that province invariants are maintained exactly
- **When optimizing**: Ensure noise preserves statistical relationships (ratios, averages)
- **When explaining**: Emphasize utility-first approach and secure enclave context
- **When suggesting changes**: Verify that modifications maintain plausibility and utility

### Key SDC Concepts in This Codebase
- **Multiplicative Jitter**: `M(c) = c × (1 + η)` where `η ~ N(0, σ²)` and `σ = noise_level × c`
- **Context-Aware Bounds**: Plausibility ranges computed per (MCC, City, Weekday) stratum
- **Province Invariants**: Exact province totals preserved (no noise at province level)
- **Bounded Contribution (K)**: Limits transactions per card per cell (transaction-weighted percentile)
- **Ratio Preservation**: avg_amount and tx_per_card stay within plausible ranges
- **Winsorization**: Per-MCC caps for transaction amounts (99th percentile)

### SDC Validation Checklist
Before approving any changes to SDC mechanisms:
- [ ] Verify province invariants are maintained exactly
- [ ] Check plausibility bounds are context-aware (not global)
- [ ] Ensure noise preserves ratios (avg_amount, tx_per_card)
- [ ] Validate bounded contribution (K) is computed correctly
- [ ] Confirm no negative counts after noise application
- [ ] Verify suppression thresholds are applied correctly

## Critical Rules

### Privacy & Security (SDC Context)
- NEVER modify province invariants - they must remain exact (public data)
- NEVER use global noise bounds - always use context-aware (MCC, City, Weekday) bounds
- ALWAYS preserve ratios when applying noise (avg_amount, tx_per_card)
- NEVER log or expose raw transaction data - only aggregated statistics
- ALWAYS validate plausibility bounds before applying noise
- REMEMBER: SDC is secondary protection layer - primary protection is secure enclave physical isolation

### Code Structure
- Follow the existing module structure: `core/`, `engine/`, `reader/`, `writer/`, `schema/`, `queries/`
- Use dataclasses for configuration (see `core/config.py` pattern)
- All SDC operations must be in `core/` or `engine/`
- Data I/O operations must use Spark DataFrames, not Pandas (except for small datasets < 100K rows)
- Note: DP modules (budget.py, primitives.py, sensitivity.py) are kept for future use but not currently active

### Spark Best Practices
- ALWAYS use Spark DataFrames for data operations (not Pandas) unless dataset is < 100K rows
- Use `spark.read.parquet()` for input, never `pandas.read_parquet()` for large datasets
- Configure Spark with appropriate memory settings (see `demo_notebook.ipynb` for examples)
- Use broadcast variables for small lookup tables (e.g., city-province mapping)
- Set `spark.sql.shuffle.partitions` appropriately (default: 200)

### Imports & Dependencies
- Core imports: `from core.config import Config`, `from core.pipeline import DPPipeline`
- Spark imports: `from pyspark.sql import SparkSession, functions as F`
- Scientific: `from scipy.optimize import nnls` (for post-processing, if needed)
- Use `numpy.random` for noise generation with configurable seed
- Optional imports (SDMetrics, matplotlib) should use try/except blocks

### Paths & File I/O
- Use relative paths from project root: `'data/city_province.csv'`, `'data/demo_sdc_results'`
- Input/output paths should be configurable via `Config` class
- Always use Parquet format for large datasets (not CSV)
- Check if directories exist before writing: `os.makedirs(path, exist_ok=True)`

### Testing & Validation
- Unit tests in `tests/` should NOT require Spark (use `test_no_spark.py` pattern)
- SDC correctness tests must verify plausibility bounds and invariants
- Always validate configuration with `config.validate()` before running pipeline
- Check for required files before processing: `if not os.path.exists(path): raise FileNotFoundError(...)`

### Documentation
- Add docstrings to all public functions/classes
- Document SDC mechanisms and plausibility bounds
- Include examples in docstrings for complex functions
- Update README.md when adding new features
- Note SDC approach vs formal DP when relevant

### Notebook-Specific
- In Jupyter notebooks, always check if Spark session exists before creating new one
- Use helper functions `show_df()` and `to_pandas_safe()` for DataFrame operations
- Convert to Pandas only for small datasets (< 100K rows) or evaluation metrics
- Always define `SDMETRICS_AVAILABLE` variable before using SDMetrics classes

### Error Handling
- Use specific exceptions: `FileNotFoundError`, `ValueError`, `RuntimeError`
- Log errors with appropriate levels: `logger.error()`, `logger.warning()`, `logger.info()`
- Validate inputs early (e.g., check file existence, validate config)

### Performance
- Use Spark for all large-scale operations (aggregations, joins, filtering)
- Avoid collecting large DataFrames to driver (use `.count()`, `.show()`, or `.sample()`)
- Use broadcast joins for small lookup tables
- Cache intermediate results if reused multiple times

### Code Style
- Follow PEP 8
- Use type hints where appropriate
- Use descriptive variable names (avoid abbreviations except common ones like `df`, `config`)
- Functions should be focused and do one thing
- Maximum function length: ~50 lines (see `.pylintrc`)

### SDC-Specific Operations
- Use multiplicative jitter: `noisy_value = original_value * (1 + noise_factor)`
- Compute context-aware bounds per (MCC, City, Weekday) stratum
- Preserve province totals exactly (invariant)
- Apply bounded contribution (K) using transaction-weighted percentile method
- Winsorize amounts per MCC using percentile caps

## Common Patterns

### Creating Spark Session
```python
spark = SparkSession.builder \
    .appName("TransactionSDC") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()
```

### Loading Data
```python
df = spark.read.parquet(input_path)  # For Parquet
df = spark.read.option("header", "true").csv(csv_path)  # For CSV
```

### Configuration Pattern
```python
from core.config import Config
config = Config()
config.data.input_path = "data/transactions.parquet"
config.privacy.noise_level = 0.15  # 15% relative noise
config.privacy.contribution_bound_method = "transaction_weighted_percentile"
config.validate()
```

### Pipeline Execution
```python
from core.pipeline import DPPipeline
pipeline = DPPipeline(config)
result = pipeline.run()
if not result['success']:
    raise RuntimeError(f"Pipeline failed: {result.get('errors')}")
```

### SDC Noise Application
```python
# Multiplicative jitter with context-aware bounds
noise_factor = np.random.normal(1.0, noise_level)
noisy_value = original_value * noise_factor

# Clamp to plausibility bounds
noisy_value = np.clip(noisy_value, min_bound, max_bound)
```

## What NOT to Do
- ❌ Don't use Pandas for large datasets (> 100K rows)
- ❌ Don't modify province invariants (they must be exact)
- ❌ Don't use global noise bounds (use context-aware)
- ❌ Don't log raw transaction data
- ❌ Don't create new Spark sessions if one already exists
- ❌ Don't use CSV for large datasets (use Parquet)
- ❌ Don't collect large DataFrames to driver memory
- ❌ Don't break ratio preservation (avg_amount, tx_per_card)
- ❌ Don't apply noise to province-level aggregates (invariants)

## SDC vs Formal DP
- **Current Approach**: SDC (utility-first) for secure enclave deployment
- **Legacy Code**: DP modules (budget.py, primitives.py) kept for future use but not active
- **Primary Protection**: Physical isolation in secure enclave
- **Secondary Protection**: SDC plausibility-based noise
- **Focus**: Maximize utility while maintaining plausibility

