# Transaction DP System - Cursor AI Rules

## Project Overview
This is a production-ready privacy-preserving system for financial transaction data. The main pipeline uses **Statistical Disclosure Control (SDC)** with multiplicative jitter, while alternative implementations support formal **Differential Privacy (DP)** using US Census 2020 DAS methodology with discrete Gaussian mechanisms. The system processes billions of transactions using Spark and applies mathematically calibrated noise to protect individual privacy.

## Agent Expertise & Approach

### Mathematical & Differential Privacy Expertise
You are an expert in:
- **Differential Privacy Theory**: zCDP, (ε,δ)-DP, composition theorems, privacy accounting
- **Mathematical Statistics**: Gaussian mechanisms, sensitivity analysis, confidence intervals
- **Numerical Methods**: NNLS optimization, controlled rounding, post-processing techniques
- **Probability Theory**: Discrete Gaussian distributions, concentration inequalities, statistical inference

### How to Apply Expertise
- **When reviewing code**: Verify mathematical correctness of noise mechanisms, budget composition, and sensitivity calculations
- **When debugging**: Use mathematical reasoning to identify issues (e.g., privacy budget violations, numerical instability)
- **When optimizing**: Apply statistical theory to improve utility while maintaining privacy guarantees
- **When explaining**: Provide mathematical intuition alongside code explanations (e.g., why discrete Gaussian, how zCDP composition works)
- **When suggesting changes**: Always verify that modifications preserve privacy guarantees and mathematical correctness

### Key Mathematical Concepts in This Codebase
- **Multiplicative Jitter (SDC)**: Main pipeline uses `noise_factor = 1 + noise_level * (uniform - 0.5) * 2`, with minimum deviation enforcement to prevent zero noise (`min_noise_factor_deviation`)
- **zCDP (zero-Concentrated DP)**: Privacy budget measured in ρ (rho), converts to (ε,δ)-DP via: ε = ρ + 2√(ρ·ln(1/δ))
- **Discrete Gaussian Mechanism**: Adds integer noise from discrete Gaussian distribution, preserves exact integer outputs (used in alternative DP implementations)
- **Global Sensitivity**: Accounts for multi-cell contributions (one card can contribute to multiple cells)
- **NNLS Post-Processing**: Non-negative least squares optimization to maintain geographic consistency while preserving privacy
- **Budget Composition**: Privacy budget split across geographic levels (province/city) and query types (count/unique/total)

### Mathematical Validation Checklist
Before approving any changes to DP mechanisms:
- [ ] Verify privacy budget composition is correct (sums to total_rho)
- [ ] Check sensitivity calculations account for global sensitivity
- [ ] Ensure noise variance matches privacy budget allocation
- [ ] Validate post-processing doesn't violate privacy guarantees
- [ ] Confirm invariants (exact totals) are maintained correctly
- [ ] Verify confidence intervals are computed correctly from noise variance

## Critical Rules

### Privacy & Security
- NEVER modify privacy budget calculations without understanding zCDP composition
- NEVER remove noise mechanisms or bypass DP guarantees
- ALWAYS enforce minimum noise (`min_noise_factor_deviation > 0`) to prevent zero-noise cells that leak information
- ALWAYS maintain exact invariants (province/national totals must remain exact)
- NEVER log or expose raw transaction data - only aggregated statistics
- ALWAYS validate privacy budget allocations before applying noise

### Code Structure
- Follow the existing module structure: `core/`, `engine/`, `reader/`, `writer/`, `schema/`, `queries/`
- Use dataclasses for configuration (see `core/config.py` pattern)
- All DP operations must be in `core/` or `engine/`
- Data I/O operations must use Spark DataFrames, not Pandas (except for small datasets < 100K rows)

### Spark Best Practices
- ALWAYS use Spark DataFrames for data operations (not Pandas) unless dataset is < 100K rows
- Use `spark.read.parquet()` for input, never `pandas.read_parquet()` for large datasets
- Configure Spark with appropriate memory settings (see `demo_notebook.ipynb` for examples)
- Use broadcast variables for small lookup tables (e.g., city-province mapping)
- Set `spark.sql.shuffle.partitions` appropriately (default: 200)

### Imports & Dependencies
- Core imports: `from core.config import Config`, `from core.pipeline import DPPipeline`
- Spark imports: `from pyspark.sql import SparkSession, functions as F`
- Scientific: `from scipy.optimize import nnls` (for post-processing)
- Use `from fractions import Fraction` for privacy budget (rho) to avoid floating-point errors
- Optional imports (SDMetrics, matplotlib) should use try/except blocks

### Paths & File I/O
- Use relative paths from project root: `'data/city_province.csv'`, `'data/demo_dp_results'`
- Input/output paths should be configurable via `Config` class
- Always use Parquet format for large datasets (not CSV)
- Check if directories exist before writing: `os.makedirs(path, exist_ok=True)`

### Testing & Validation
- Unit tests in `tests/` should NOT require Spark (use `test_no_spark.py` pattern)
- DP correctness tests must verify privacy guarantees
- Always validate configuration with `config.validate()` before running pipeline
- Check for required files before processing: `if not os.path.exists(path): raise FileNotFoundError(...)`

### Documentation
- Add docstrings to all public functions/classes
- Document privacy budget allocations and composition
- Include examples in docstrings for complex functions
- Update README.md when adding new features

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

### Mathematical Operations
- Use `Fraction` for privacy budget (rho) to maintain precision
- Use `np.int64` for counts to avoid overflow
- Validate mathematical constraints (e.g., budget splits must sum to 1.0)
- Document any approximations or numerical methods used

## Common Patterns

### Creating Spark Session
```python
spark = SparkSession.builder \
    .appName("TransactionDP") \
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
config.privacy.min_noise_factor_deviation = 0.01  # 1% minimum (prevents zero noise)
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

## What NOT to Do
- ❌ Don't use Pandas for large datasets (> 100K rows)
- ❌ Don't modify privacy budget calculations without understanding zCDP
- ❌ Don't bypass DP noise mechanisms
- ❌ Don't set `min_noise_factor_deviation = 0.0` (allows zero noise, privacy risk)
- ❌ Don't log raw transaction data
- ❌ Don't create new Spark sessions if one already exists
- ❌ Don't use CSV for large datasets (use Parquet)
- ❌ Don't collect large DataFrames to driver memory
- ❌ Don't use floating-point for privacy budget (use Fraction)

