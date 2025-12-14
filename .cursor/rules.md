# Transaction SDC System - Cursor AI Rules

## Project Overview
This is a production-ready privacy-preserving system for financial transaction data. The **main pipeline uses Statistical Disclosure Control (SDC)** with multiplicative jitter and context-aware plausibility bounds, designed for secure enclave deployment. The system processes billions of transactions using Spark and applies mathematically calibrated noise to protect individual privacy while maximizing utility. Alternative implementations support formal **Differential Privacy (DP)** using US Census 2020 DAS methodology with discrete Gaussian mechanisms (kept for future use but not currently active).

## Agent Expertise & Approach

### Mathematical & Statistical Disclosure Control Expertise
You are an expert in:
- **Statistical Disclosure Control (SDC)**: Multiplicative jitter, plausibility bounds, controlled rounding, utility-first protection
- **Mathematical Statistics**: Noise mechanisms, ratio preservation, confidence intervals
- **Numerical Methods**: Controlled rounding, post-processing techniques, scaling operations
- **Probability Theory**: Random noise generation, statistical inference, ratio preservation
- **Secure Enclave Context**: Physical isolation as primary protection, SDC as secondary layer
- **Data Quality**: Outlier handling, ratio preservation, geographic consistency
- **Differential Privacy Theory** (for alternative implementations): zCDP, (ε,δ)-DP, composition theorems, privacy accounting

### How to Apply Expertise
- **When reviewing code**: Verify mathematical correctness of noise mechanisms, province invariant preservation, and ratio constraints. Verify plausibility bounds are context-aware (MCC, City, Weekday).
- **When debugging**: Use mathematical reasoning to identify issues (e.g., province invariant violations, ratio bound violations, numerical instability). Check that province invariants are maintained exactly.
- **When optimizing**: Apply statistical theory to improve utility while maintaining plausibility and exact province invariants. Ensure noise preserves statistical relationships (ratios, averages).
- **When explaining**: Provide mathematical intuition alongside code explanations (e.g., why multiplicative jitter preserves ratios, how province invariants are maintained exactly). Emphasize utility-first approach and secure enclave context.
- **When suggesting changes**: Always verify that modifications preserve province invariants exactly and maintain plausibility bounds. Verify that modifications maintain plausibility and utility.

### Key Mathematical Concepts in This Codebase (SDC - Main Pipeline)
- **Multiplicative Jitter (SDC)**: Main pipeline uses `noise_factor = 1 + noise_level * (uniform - 0.5) * 2`, with minimum deviation enforcement to prevent zero noise (`min_noise_factor_deviation`). Preserves ratios naturally (amount/count, count/cards). Formula: `M(c) = c × (1 + η)` where `η ~ N(0, σ²)` and `σ = noise_level × c`.
- **Province Invariants (CRITICAL)**: Province-level transaction counts are **EXACT** (invariant) - must be maintained exactly even after adding noise. All city-level noisy counts are scaled to sum exactly to province totals. This is enforced in Phase 6 of the pipeline and validated in Phase 10. Exact province totals preserved (no noise at province level).
- **Context-Aware Plausibility Bounds**: Data-driven bounds per (MCC, City, Weekday) context computed from p5-p95 percentiles. Plausibility ranges computed per (MCC, City, Weekday) stratum. Noisy values are clamped to these bounds to ensure realistic outputs.
- **Ratio Preservation**: avg_amount (amount/count) and tx_per_card (count/cards) ratios are preserved approximately and validated against context-specific bounds. avg_amount and tx_per_card stay within plausible ranges.
- **Controlled Rounding**: Integer rounding that maintains province count invariants exactly. When count changes during rounding, amount and cards are scaled proportionally to preserve ratios.
- **Scaling to Match Invariants**: After adding noise, all three values (count, cards, amount) are scaled proportionally within each province to match province invariants exactly.
- **Bounded Contribution (K)**: Limits transactions per card per cell (transaction-weighted percentile method). Prevents outliers from dominating statistics.
- **Winsorization**: Per-MCC caps for transaction amounts (99th percentile).

### Key Mathematical Concepts (Alternative DP Implementations)
- **zCDP (zero-Concentrated DP)**: Privacy budget measured in ρ (rho), converts to (ε,δ)-DP via: ε = ρ + 2√(ρ·ln(1/δ))
- **Discrete Gaussian Mechanism**: Adds integer noise from discrete Gaussian distribution, preserves exact integer outputs
- **Global Sensitivity**: Accounts for multi-cell contributions (one card can contribute to multiple cells)
- **NNLS Post-Processing**: Non-negative least squares optimization to maintain geographic consistency while preserving privacy
- **Budget Composition**: Privacy budget split across geographic levels (province/city) and query types (count/unique/total)

### Mathematical Validation Checklist
Before approving any changes to SDC mechanisms:
- [ ] **CRITICAL**: Verify province count invariants are maintained EXACTLY (0% error) after noise and rounding
- [ ] Verify noise factors have minimum deviation (`min_noise_factor_deviation > 0`) to prevent zero noise
- [ ] Check that noisy values are clamped to plausibility bounds per context
- [ ] Check plausibility bounds are context-aware (not global)
- [ ] Validate that ratios (avg_amount, tx_per_card) stay within context-specific bounds
- [ ] Ensure noise preserves ratios (avg_amount, tx_per_card)
- [ ] Confirm scaling operations preserve province invariants exactly
- [ ] Verify controlled rounding maintains province count invariants exactly
- [ ] Check that amount and cards are scaled proportionally when count changes during rounding
- [ ] Validate bounded contribution (K) is computed correctly
- [ ] Confirm no negative counts after noise application
- [ ] Verify suppression thresholds are applied correctly

## Critical Rules

### Province Invariants (MOST CRITICAL)
**Province count invariants MUST be maintained EXACTLY (0% error) at all times, even after adding noise and rounding.**

- **Definition**: Province-level transaction counts are EXACT invariants - the sum of all city-level counts within a province must exactly equal the province total (computed from original data)
- **When enforced**: 
  - Phase 1: Province invariants computed from original data
  - Phase 6: After adding noise, all city-level counts are scaled proportionally to match province invariants exactly
  - Phase 9: Controlled rounding maintains province count invariants exactly
  - Phase 10: Final validation verifies province invariants are exact (0% error)
- **Implementation**: See `engine/topdown_spark.py` Phase 6 (scaling) and Phase 9 (controlled rounding)
- **Validation**: Always check `sum(city_counts_per_province) == province_total` with 0% error
- **Critical**: If province invariants are violated, the output is invalid and must be fixed

### Privacy & Security
- **CRITICAL**: ALWAYS maintain province count invariants EXACTLY (0% error) - province totals must match original totals exactly, even after adding noise and rounding
- NEVER modify province invariants - they must remain exact (public data)
- NEVER remove noise mechanisms or bypass SDC protection
- ALWAYS enforce minimum noise (`min_noise_factor_deviation > 0`) to prevent zero-noise cells that leak information
- ALWAYS clamp noisy values to context-aware plausibility bounds to ensure realistic outputs
- NEVER use global noise bounds - always use context-aware (MCC, City, Weekday) bounds
- NEVER log or expose raw transaction data - only aggregated statistics
- ALWAYS preserve ratios when applying noise (avg_amount, tx_per_card)
- ALWAYS validate plausibility bounds before applying noise
- ALWAYS validate province invariants after noise application, scaling, and rounding
- When modifying noise mechanisms, ensure province invariants are preserved exactly through all phases
- REMEMBER: SDC is secondary protection layer - primary protection is secure enclave physical isolation

### Code Structure
- Follow the existing module structure: `core/`, `engine/`, `reader/`, `writer/`, `schema/`, `queries/`
- Use dataclasses for configuration (see `core/config.py` pattern)
- All SDC operations must be in `core/` or `engine/` (main pipeline: `engine/topdown_spark.py`)
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
- Use `from fractions import Fraction` for privacy budget (rho) to avoid floating-point errors (for DP implementations)
- Use `numpy.random` for noise generation with configurable seed
- Optional imports (SDMetrics, matplotlib) should use try/except blocks

### Paths & File I/O
- Use relative paths from project root: `'data/city_province.csv'`, `'data/demo_sdc_results'`
- Input/output paths should be configurable via `Config` class
- Always use Parquet format for large datasets (not CSV)
- Check if directories exist before writing: `os.makedirs(path, exist_ok=True)`

### Testing & Validation
- Unit tests in `tests/` should NOT require Spark (use `test_no_spark.py` pattern)
- SDC correctness tests must verify province invariants are maintained exactly (0% error)
- SDC correctness tests must verify plausibility bounds and invariants
- Always validate configuration with `config.validate()` before running pipeline
- Check for required files before processing: `if not os.path.exists(path): raise FileNotFoundError(...)`
- **CRITICAL**: Always verify province count invariants after pipeline execution - sum of city-level counts must exactly equal province totals

### Documentation
- Add docstrings to all public functions/classes
- Document province invariant preservation and scaling operations
- Document SDC mechanisms and plausibility bounds
- Include examples in docstrings for complex functions
- Update README.md when adding new features
- Document that province count invariants are maintained exactly (0% error)
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

### Mathematical Operations
- Use `np.int64` for counts to avoid overflow
- Validate mathematical constraints (e.g., province invariant sums must match exactly)
- Document any approximations or numerical methods used
- **CRITICAL**: When scaling to match province invariants, ensure exact equality: `sum(city_counts) == province_total` (0% error)
- When rounding, scale amount and cards proportionally to count changes to preserve ratios

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
config.privacy.min_noise_factor_deviation = 0.01  # 1% minimum (prevents zero noise)
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
- ❌ **CRITICAL**: Don't break province count invariants - province totals must remain EXACT (0% error) even after noise and rounding
- ❌ Don't use Pandas for large datasets (> 100K rows)
- ❌ Don't bypass SDC noise mechanisms
- ❌ Don't set `min_noise_factor_deviation = 0.0` (allows zero noise, privacy risk)
- ❌ Don't modify province invariants (they must be exact)
- ❌ Don't use global noise bounds (use context-aware)
- ❌ Don't log raw transaction data
- ❌ Don't create new Spark sessions if one already exists
- ❌ Don't use CSV for large datasets (use Parquet)
- ❌ Don't collect large DataFrames to driver memory
- ❌ Don't modify scaling operations without ensuring province invariants remain exact
- ❌ Don't change rounding logic without maintaining province count invariants exactly
- ❌ Don't break ratio preservation (avg_amount, tx_per_card)
- ❌ Don't apply noise to province-level aggregates (invariants)

## SDC vs Formal DP
- **Current Approach**: SDC (utility-first) for secure enclave deployment
- **Legacy Code**: DP modules (budget.py, primitives.py) kept for future use but not currently active
- **Primary Protection**: Physical isolation in secure enclave
- **Secondary Protection**: SDC plausibility-based noise
- **Focus**: Maximize utility while maintaining plausibility
