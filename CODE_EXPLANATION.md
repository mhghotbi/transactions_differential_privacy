# Code Explanation Request: Transaction DP System

Explain this code with **gradually increasing complexity** across 4 levels:

---

## 📊 Level 1: High-Level Overview (ELI5)

### What does this code do in one sentence?
This system adds carefully calibrated random noise to financial transaction statistics to protect individual privacy while keeping the data useful for analysis.

### What real-world problem does it solve?
Banks and payment companies need to share transaction patterns (how much people spend in each city, which merchants are popular) without revealing any individual's transactions. This code lets them publish aggregate statistics while mathematically guaranteeing that no one can learn about any single person's transactions.

### Simple Analogy
Imagine you want to know how many people live on a street, but you can't ask anyone directly. Instead, everyone flips a coin - if heads, they say "yes I live here", if tails, they say the opposite of the truth. You can still estimate the real count from all the answers, but you can't be sure about any single person. This code does something similar with transaction data - it adds "statistical static" that hides individuals but preserves the overall signal.

---

## 📊 Level 2: Architecture & Flow

### System Architecture Diagram (ASCII)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        TRANSACTION DP SYSTEM                                │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐    ┌───────────┐ │
│  │   READER     │    │ PREPROCESSOR │    │   ENGINE     │    │  WRITER   │ │
│  │              │───▶│              │───▶│              │───▶│           │ │
│  │ spark_reader │    │ winsorize    │    │ topdown      │    │ parquet   │ │
│  │              │    │ aggregate    │    │ noise inject │    │ output    │ │
│  └──────────────┘    └──────────────┘    └──────────────┘    └───────────┘ │
│         │                   │                   │                          │
│         ▼                   ▼                   ▼                          │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐                  │
│  │   SCHEMA     │    │    CORE      │    │   QUERIES    │                  │
│  │              │    │              │    │              │                  │
│  │ geography.py │    │ budget.py    │    │ transaction_ │                  │
│  │ histogram.py │    │ primitives.py│    │ queries.py   │                  │
│  └──────────────┘    └──────────────┘    └──────────────┘                  │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Data Flow Pipeline

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  Raw CSV    │     │  Preprocess │     │  Aggregate  │     │  Add Noise  │     │  Protected  │
│ Transactions│────▶│ Winsorize   │────▶│ to Histogram│────▶│ Top-Down DP │────▶│  Parquet    │
│             │     │ amounts     │     │ (city,mcc)  │     │             │     │  Output     │
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
     1M rows            cap at 99%        ~15K cells         Gaussian noise      ~15K cells
                        percentile                           per query           + noise
```

### Control Flow Diagram

```
                              [Start]
                                 │
                                 ▼
                        ┌────────────────┐
                        │ Load Config    │
                        │ (default.ini)  │
                        └───────┬────────┘
                                │
                                ▼
                        ┌────────────────┐     ┌─────────────┐
                        │ Validate       │────▶│ Error:      │
                        │ Config?        │ No  │ Invalid     │
                        └───────┬────────┘     │ Config      │
                                │ Yes          └─────────────┘
                                ▼
                        ┌────────────────┐
                        │ Initialize     │
                        │ SparkSession   │
                        └───────┬────────┘
                                │
                                ▼
                        ┌────────────────┐
                        │ Read CSV Data  │
                        │ (spark_reader) │
                        └───────┬────────┘
                                │
                                ▼
                        ┌────────────────┐
                        │ Load Geography │
                        │ city→province  │
                        └───────┬────────┘
                                │
                                ▼
                        ┌────────────────┐
                        │ Preprocess:    │
                        │ - Winsorize    │
                        │ - Build Hist   │
                        └───────┬────────┘
                                │
                                ▼
                        ┌────────────────┐
                        │ Allocate       │
                        │ Privacy Budget │
                        └───────┬────────┘
                                │
                                ▼
                        ┌────────────────┐
                        │ Top-Down Noise │
                        │ Province→City  │
                        └───────┬────────┘
                                │
                                ▼
                        ┌────────────────┐
                        │ Post-Process   │
                        │ Non-negative   │
                        └───────┬────────┘
                                │
                                ▼
                        ┌────────────────┐
                        │ Write Parquet  │
                        │ (partitioned)  │
                        └───────┬────────┘
                                │
                                ▼
                              [End]
```

### Component Responsibilities

| Component | Role | Key Methods |
|-----------|------|-------------|
| `config.py` | Load and validate configuration | `Config.from_ini()`, `validate()` |
| `core/budget.py` | zCDP budget allocation & composition | `Budget.allocate()`, `compute_sigma_for_query()` |
| `core/primitives.py` | Discrete Gaussian noise mechanism | `DiscreteGaussianMechanism.add_noise()` |
| `core/pipeline.py` | Orchestrate entire workflow | `DPPipeline.run()` |
| `core/postprocessing.py` | NNLS optimization | `NNLSPostProcessor.solve()` |
| `core/rounder.py` | Controlled rounding | `CensusControlledRounder.round()` |
| `core/invariants.py` | Exact totals management | `InvariantManager.compute_invariants_from_spark()` |
| `core/suppression.py` | Cell suppression | `SuppressionManager.apply()` |
| `core/confidence.py` | Confidence intervals | `ConfidenceCalculator.add_intervals_to_dataframe()` |
| `core/sensitivity.py` | Global sensitivity | `GlobalSensitivityCalculator.compute_l2_sensitivity()` |
| `core/bounded_contribution.py` | Bound card contributions | `BoundedContributionCalculator.compute_k_from_spark()` |
| `schema/geography.py` | Province/City hierarchy from CSV | `Geography.from_csv()` |
| `schema/histogram.py` | Multi-dimensional histogram structure | `TransactionHistogram.from_spark_df()` |
| `reader/spark_reader.py` | Read transaction data via Spark | `TransactionReader.read()` |
| `reader/preprocessor.py` | Winsorization + aggregation | `TransactionPreprocessor.process()` |
| `reader/preprocessor_distributed.py` | **Production scale (10B+)** | `ProductionPipeline.run()` |
| `engine/topdown.py` | Hierarchical noise injection | `TopDownEngine.run()` |
| `queries/transaction_queries.py` | Define 4 main queries | `TransactionWorkload.get_query_specs()` |
| `writer/parquet_writer.py` | Write protected output | `ParquetWriter.write()` |

### Configuration System

#### Configuration Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     CONFIGURATION FLOW                                       │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌──────────────┐      ┌──────────────┐      ┌──────────────┐               │
│  │ default.ini  │      │   Config     │      │  Components  │               │
│  │              │─────▶│   Object     │─────▶│              │               │
│  │ [privacy]    │      │              │      │  Budget      │               │
│  │ [data]       │      │ PrivacyConfig│      │  Preprocessor│               │
│  │ [spark]      │      │ DataConfig   │      │  DPEngine    │               │
│  │ [columns]    │      │ SparkConfig  │      │  Writer      │               │
│  └──────────────┘      └──────────────┘      └──────────────┘               │
│         │                     │                     │                        │
│         │                     ▼                     │                        │
│         │              ┌──────────────┐             │                        │
│         │              │   Validate   │             │                        │
│         │              │  - Splits=1  │             │                        │
│         │              │  - rho > 0   │             │                        │
│         └─────────────▶│  - paths ok  │◀────────────┘                        │
│                        └──────────────┘                                      │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

#### Config Classes Structure

```python
@dataclass
class Config:
    privacy: PrivacyConfig    # Privacy-related settings
    data: DataConfig          # Data paths and processing
    spark: SparkConfig        # Spark cluster settings
    columns: Dict[str, str]   # Column name mappings

@dataclass
class PrivacyConfig:
    # Budget
    total_rho: Fraction       # zCDP parameter (e.g., 1/4)
    delta: float              # (ε,δ)-DP delta (e.g., 1e-10)
    
    # Allocation
    geographic_split: Dict[str, float]  # {"province": 0.2, "city": 0.8}
    query_split: Dict[str, float]       # {"transaction_count": 0.25, ...}
    
    # Bounded Contribution
    contribution_bound_method: str      # "iqr", "percentile", "fixed"
    contribution_bound_iqr_multiplier: float  # 1.5
    contribution_bound_fixed: int       # 5
    contribution_bound_percentile: float # 99.0
    
    # Suppression
    suppression_threshold: int          # 10
    suppression_method: str             # "flag", "null", "value"
    
    # Confidence Intervals
    confidence_levels: List[float]      # [0.90]
    include_relative_moe: bool          # True
    
    # Sensitivity
    sensitivity_method: str             # "local", "global", "fixed"
    fixed_max_cells_per_card: int       # 100

@dataclass
class DataConfig:
    input_path: str           # Path to input data
    output_path: str          # Path for output
    city_province_path: str   # Path to city_province.csv
    input_format: str         # "parquet" or "csv"
    winsorize_percentile: float  # 99.0
    winsorize_cap: Optional[float]  # Override cap
    date_column: str          # Column name for date
    date_format: str          # Date format string
    num_days: int             # Days in reporting period

@dataclass
class SparkConfig:
    app_name: str             # Application name
    master: str               # Spark master URL
    executor_memory: str      # Memory per executor
    driver_memory: str        # Driver memory
    shuffle_partitions: int   # Shuffle partition count
```

#### How Config Flows to Components

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                CONFIG → COMPONENT MAPPING                                    │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  [privacy] section                                                           │
│  ─────────────────                                                           │
│  total_rho ──────────────────────────────▶ Budget.total_rho                 │
│  delta ──────────────────────────────────▶ Budget.delta                     │
│  geographic_split_* ─────────────────────▶ Budget.geographic_split          │
│  query_split_* ──────────────────────────▶ Budget.query_split               │
│                                                                              │
│  contribution_bound_* ───────────────────▶ BoundedContributionCalculator    │
│  suppression_* ──────────────────────────▶ SuppressionManager               │
│  confidence_* ───────────────────────────▶ ConfidenceCalculator             │
│  sensitivity_* ──────────────────────────▶ GlobalSensitivityCalculator      │
│                                                                              │
│  [data] section                                                              │
│  ──────────────                                                              │
│  input_path ─────────────────────────────▶ SparkReader                      │
│  output_path ────────────────────────────▶ ParquetWriter                    │
│  city_province_path ─────────────────────▶ Geography.from_csv()             │
│  winsorize_* ────────────────────────────▶ Preprocessor                     │
│  num_days ───────────────────────────────▶ BudgetAllocator                  │
│                                                                              │
│  [spark] section                                                             │
│  ───────────────                                                             │
│  app_name ───────────────────────────────▶ SparkSession.builder.appName()   │
│  master ─────────────────────────────────▶ SparkSession.builder.master()    │
│  executor_memory ────────────────────────▶ spark.executor.memory            │
│  driver_memory ──────────────────────────▶ spark.driver.memory              │
│  shuffle_partitions ─────────────────────▶ spark.sql.shuffle.partitions     │
│                                                                              │
│  [columns] section                                                           │
│  ─────────────────                                                           │
│  transaction_id ─────────────────────────▶ Column name in DataFrame         │
│  amount ─────────────────────────────────▶ Column name for amount           │
│  card_number ────────────────────────────▶ Column name for card             │
│  ...                                                                         │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

#### Loading and Using Config

```python
# Method 1: Load from INI file
from core.config import Config

config = Config.from_ini("configs/default.ini")
config.validate()  # Raises ValueError if invalid

# Method 2: Create programmatically
from core.config import Config, PrivacyConfig
from fractions import Fraction

config = Config()
config.privacy.total_rho = Fraction(1, 4)
config.privacy.suppression_threshold = 15
config.data.input_path = "/data/transactions.parquet"
config.validate()

# Method 3: Modify and save
config = Config.from_ini("configs/default.ini")
config.privacy.total_rho = Fraction(1, 2)
config.to_ini("configs/custom.ini")

# Using config in pipeline
from core.pipeline import DPPipeline

pipeline = DPPipeline(spark, config, geography, budget)
pipeline.run()
```

#### Validation Rules

```python
def validate(self):
    # Privacy validation
    assert sum(geographic_split.values()) == 1.0
    assert sum(query_split.values()) == 1.0
    assert total_rho > 0
    assert delta > 0 and delta < 1
    assert contribution_bound_method in ("iqr", "percentile", "fixed")
    assert suppression_threshold >= 0
    assert suppression_method in ("flag", "null", "value")
    assert sensitivity_method in ("local", "global", "fixed")
    assert all(0 < level < 1 for level in confidence_levels)
    
    # Data validation
    assert input_path is not empty
    assert output_path is not empty
    assert 0 < winsorize_percentile <= 100
```

### Production Scale Architecture (10B+ Records)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│              PRODUCTION DISTRIBUTED ARCHITECTURE                            │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                        SPARK CLUSTER                                │    │
│  │  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐      ┌─────────┐   │    │
│  │  │Executor │ │Executor │ │Executor │ │Executor │ ...  │Executor │   │    │
│  │  │   1     │ │   2     │ │   3     │ │   4     │      │  100    │   │    │
│  │  │  32GB   │ │  32GB   │ │  32GB   │ │  32GB   │      │  32GB   │   │    │
│  │  └────┬────┘ └────┬────┘ └────┬────┘ └────┬────┘      └────┬────┘   │    │
│  │       │           │           │           │                │        │    │
│  │       └───────────┴───────────┴───────────┴────────────────┘        │    │
│  │                               │                                     │    │
│  │                       ┌───────┴───────┐                             │    │
│  │                       │    DRIVER     │                             │    │
│  │                       │    16GB       │                             │    │
│  │                       │ (coord only)  │                             │    │
│  │                       └───────────────┘                             │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                             │
│  KEY OPTIMIZATIONS:                                                         │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │ ✓ NO collect() to driver - everything stays distributed             │    │
│  │ ✓ Broadcast join for geography (500 cities << 10B rows)             │    │
│  │ ✓ Distributed noise via Spark SQL randn()                           │    │
│  │ ✓ Partitioned output by province                                    │    │
│  │ ✓ Checkpointing for fault tolerance                                 │    │
│  │ ✓ Adaptive query execution for skew handling                        │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Why Basic vs Distributed?**

| Aspect | Basic (`preprocessor.py`) | Distributed (`preprocessor_distributed.py`) |
|--------|---------------------------|---------------------------------------------|
| Scale | Up to ~10M rows | 10B+ rows |
| Noise | Exact Discrete Gaussian | Exact OR Approximate |
| Memory | Driver collects histogram | Fully distributed |
| Use case | Testing, small production | Large-scale production |

### Census 2020 DAS Compatibility

The `CensusDASEngine` class exactly replicates the US Census Bureau's 2020 methodology:

```
┌─────────────────────────────────────────────────────────────────────┐
│         CENSUS DAS-STYLE WITH PROVINCE-MONTH INVARIANTS              │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  STEP 1: Compute Province-Month Invariants (PUBLIC DATA)           │
│  ═══════════════════════════════════════════════════════            │
│                                                                      │
│    Province A           Province B           Province C              │
│    ┌─────────┐          ┌─────────┐          ┌─────────┐            │
│    │ Public: │          │ Public: │          │ Public: │            │
│    │ 10,000  │          │  5,000  │          │  8,000  │            │
│    │ (EXACT) │          │ (EXACT) │          │ (EXACT) │            │
│    └─────────┘          └─────────┘          └─────────┘            │
│    No noise added - these are publicly published statistics         │
│                                                                      │
│  STEP 2: Cell-Level Noise (100% of budget)                          │
│  ═══════════════════════════════════════                            │
│                                                                      │
│    Province A (cells: city × mcc × day)                             │
│    ┌────────────────────────────────────────┐                       │
│    │  City 1, MCC 5411, Day 1: 2500 → 2502 │                       │
│    │  City 1, MCC 5411, Day 2: 1800 → 1803 │                       │
│    │  City 2, MCC 5812, Day 1: 3000 → 2998 │                       │
│    │  City 2, MCC 5812, Day 2: 2700 → 2701 │                       │
│    │  ... (all cells get noise)              │                       │
│    └────────────────────────────────────────┘                       │
│    Noisy cell sum = 10,004 (doesn't match public 10,000 yet)        │
│                                                                      │
│  STEP 3: NNLS Post-Processing (Enforce Province Constraint)         │
│  ═══════════════════════════════════════════════════════            │
│                                                                      │
│    Problem: Cell sum (10,004) ≠ Province public invariant (10,000)  │
│                                                                      │
│    Solution: NNLS optimization                                       │
│    minimize   Σ (x_cell - noisy_cell)²                              │
│    subject to Σ x_cell = 10,000 (province invariant)                │
│               x_cell ≥ 0 (non-negativity)                            │
│                                                                      │
│    Result: Adjusted cells sum to exactly 10,000 ✓                   │
│                                                                      │
│  STEP 4: Controlled Rounding                                        │
│  ═══════════════════════════                                        │
│                                                                      │
│    Round to integers while preserving province sum = 10,000         │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

**Key Census DAS Features Implemented:**

| Feature | Implementation |
|---------|----------------|
| Exact Discrete Gaussian | `_discrete_gaussian()` with rational arithmetic |
| Cryptographic RNG | Python `secrets` module |
| Province-Month Invariants | Exact public data (no noise) |
| Cell-Level Noise | Full budget to (city, mcc, day) cells |
| NNLS Post-Processing | Enforces province-month constraints |
| Controlled Rounding | Integer outputs preserving sums |
| Budget composition | zCDP additive composition |
| Post-processing | Non-negativity (free under DP) |

**Usage:**

```bash
# Full Census DAS methodology
python examples/run_production.py \
    --input data/transactions.parquet \
    --output output/protected \
    --rho 1 \
    --census-das

# Exact mechanism only (no consistency)
python examples/run_production.py \
    --input data/transactions.parquet \
    --output output/protected \
    --rho 1 \
    --exact

# Fast approximate (for testing)
python examples/run_production.py \
    --input data/transactions.parquet \
    --output output/protected \
    --rho 1 \
    --approximate
```

---

## 📊 Level 3: Scientific & Theoretical Foundations

### 🔬 Core Mathematical Concepts

#### Zero-Concentrated Differential Privacy (zCDP)

This code uses **zCDP** (Bun & Steinke, 2016), which provides tighter composition than (ε,δ)-DP.

**Definition**: A mechanism M satisfies ρ-zCDP if for all neighboring databases D, D':

```
D_α(M(D) || M(D')) ≤ ρα    for all α > 1
```

where D_α is the α-Rényi divergence.

#### Discrete Gaussian Mechanism

For a query f with sensitivity Δ, the Discrete Gaussian mechanism adds noise:

```
M(D) = f(D) + η,    where η ~ N_Z(0, σ²)
```

**Privacy-Noise Relationship**:
```
σ² = Δ² / (2ρ)
```

| Variable | Meaning |
|----------|---------|
| σ² | Variance of Gaussian noise |
| Δ | Query sensitivity (max change from one person) |
| ρ | Privacy budget (zCDP parameter) |

**Example**: For ρ=1, Δ=1: σ² = 1/(2×1) = 0.5, so σ ≈ 0.707

#### Query Sensitivities

| Query | Sensitivity (Δ) | Reasoning |
|-------|-----------------|-----------|
| transaction_count | K (bounded) | One card adds at most K transactions per cell |
| unique_cards | 1 | One card contributes at most 1 to count distinct |
| unique_acceptors | 1 | One card affects at most 1 acceptor count per cell |
| total_amount | W (winsorized cap) | After winsorization, max contribution is capped |

#### Bounded Contribution (K)

```
┌─────────────────────────────────────────────────────────────────────┐
│                    BOUNDED CONTRIBUTION                              │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  Problem: One card could make many transactions in a single cell    │
│           (city, mcc, day), making sensitivity unbounded.           │
│                                                                      │
│  Solution: Bound contributions using IQR method                     │
│                                                                      │
│  IQR Method:                                                        │
│  ───────────                                                        │
│    Q1 = 25th percentile of transactions per card-cell              │
│    Q3 = 75th percentile                                             │
│    IQR = Q3 - Q1                                                    │
│    K = ceil(Q3 + 1.5 * IQR)                                        │
│                                                                      │
│  Example:                                                           │
│    Distribution: [1, 1, 1, 2, 2, 3, 3, 5, 10, 50]                  │
│    Q1 = 1, Q3 = 3, IQR = 2                                         │
│    K = ceil(3 + 1.5 * 2) = ceil(6) = 6                            │
│                                                                      │
│    Card with 50 transactions → clipped to 6                        │
│    Sensitivity = 6 (not 50!)                                        │
│                                                                      │
│  Why IQR?                                                           │
│  ─────────                                                          │
│    - Statistical outlier detection                                  │
│    - Robust to extreme values                                       │
│    - Same approach used in boxplots                                 │
│    - Census 2020 also bounds household contributions               │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

**Configuration:**

```ini
[privacy]
# Method: 'iqr' (auto), 'percentile', or 'fixed'
contribution_bound_method = iqr

# IQR multiplier (default 1.5)
contribution_bound_iqr_multiplier = 1.5

# Fixed K if method = fixed
contribution_bound_fixed = 5
```

#### Budget Allocation (Default: ρ = 0.25 monthly)

```
┌─────────────────────────────────────────────────────────────────────┐
│                    BUDGET ALLOCATION TREE                            │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  Total Monthly:  ρ = 0.25                                           │
│  ════════════════════════                                            │
│         │                                                            │
│         ├──── Province-Month: 0% (PUBLIC DATA - no noise)            │
│         │     These totals are published exactly                    │
│         │                                                            │
│         └──── Cell Level (100%) ───→ ρ = 0.25                       │
│                   │                                                  │
│                   ├── transaction_count (33%) → ρ = 0.083           │
│                   ├── unique_cards (33%)      → ρ = 0.083           │
│                   └── total_amount (34%)      → ρ = 0.084           │
│                                                                      │
│  Note: Full budget allocated to cell level since province-month    │
│        totals are public invariants (no privacy cost)                │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

#### Parallel Composition for Days

```
┌─────────────────────────────────────────────────────────────────────┐
│                    PARALLEL COMPOSITION FOR DAYS                     │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  Key Assumption:                                                     │
│  ───────────────                                                     │
│  Each card contributes to at most one cell (city, mcc) per day      │
│                                                                      │
│  Result:                                                             │
│  ───────                                                             │
│  Day 1: ρ = 0.05 per query  ─┐                                      │
│  Day 2: ρ = 0.05 per query   │                                      │
│  Day 3: ρ = 0.05 per query   │ Parallel Composition                 │
│  ...                          ├─→ Total = 0.05 (NOT × 30!)          │
│  Day 30: ρ = 0.05 per query ─┘                                      │
│                                                                      │
│  Why? Days are disjoint - a transaction cannot exist                │
│  in both Day 1 and Day 2 simultaneously.                            │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

#### Sigma Values per Query (Cell Level)

| Query | ρ | σ | Typical Noise (95%) |
|-------|-----|------|---------------------|
| transaction_count | 0.083 | 2.45 | ±5 |
| unique_cards | 0.083 | 2.45 | ±5 |
| total_amount | 0.084 | 2.44 × cap | depends on cap |

**Note**: Province-month totals are EXACT (public data) - no noise added.
All privacy budget is allocated to cell-level (city, mcc, day) measurements.

#### Practical Impact Examples

```
Large Cell (1000 transactions):
  True count: 1000
  σ = 2.45 → noise typically ±5
  Output: ~995 to 1005
  Relative error: ~0.5%

Small Cell (10 transactions):
  True count: 10  
  σ = 2.45 → noise typically ±5
  Output: ~5 to 15
  Relative error: ~50% (high for small cells)

Province-Month Total:
  Public value: 10,000 (EXACT - no noise)
  All cells sum to exactly 10,000 after NNLS
```

#### Continuous Release & Annual Privacy

```
┌─────────────────────────────────────────────────────────────────────┐
│                  ANNUAL PRIVACY COMPOSITION                          │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  Monthly releases with ρ = 0.25:                                    │
│                                                                      │
│    Month 1:  ρ = 0.25  ─┐                                           │
│    Month 2:  ρ = 0.25   │                                           │
│    ...                   ├─→ Annual: ρ = 12 × 0.25 = 3.0            │
│    Month 12: ρ = 0.25  ─┘                                           │
│                                                                      │
│  Conversion to (ε, δ)-DP (δ = 10⁻¹⁰):                              │
│    ε = ρ + 2√(ρ × ln(1/δ))                                         │
│    ε = 3 + 2√(3 × 23) ≈ 3 + 16.6 ≈ 19.6                            │
│                                                                      │
│  Comparison with Census 2020:                                        │
│    Census 2020: ε ≈ 17 (one-time release every 10 years)           │
│    Your system: ε ≈ 20 per year (continuous monthly)               │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

### 📐 Theorem/Lemma References

```
Theorem 1 (Bun & Steinke, 2016 - zCDP Composition):
If M₁ satisfies ρ₁-zCDP and M₂ satisfies ρ₂-zCDP,
then (M₁, M₂) satisfies (ρ₁ + ρ₂)-zCDP.
                    ↓
Implementation: BudgetAllocator.compose() simply sums ρ values
```

```
Theorem 2 (Discrete Gaussian Mechanism):
For sensitivity-Δ query, the Discrete Gaussian mechanism
with σ² = Δ²/(2ρ) satisfies ρ-zCDP.
                    ↓
Implementation: DiscreteGaussianMechanism._compute_sigma()
```

```
Theorem 3 (Post-Processing):
If M satisfies ρ-zCDP, then g(M) satisfies ρ-zCDP for any function g.
                    ↓
Implementation: Non-negativity clamping in post-processing is free!
```

### ⚙️ Algorithm Analysis

| Aspect | Value | Explanation |
|--------|-------|-------------|
| Time Complexity | O(n + h) | n = input records, h = histogram cells |
| Space Complexity | O(h) | h = cities × MCCs × days ≈ 480 × 100 × 30 |
| Privacy Cost (ρ) | User-specified | Default: ρ = 1 (converts to ε ≈ 2.5) |
| Per-Query ρ | ρ_total / 4 | Equal split among 4 queries |
| Per-Level ρ | 20% Province, 80% City | Geographic budget split |

### 🎯 Design Trade-offs

| Choice | Alternative | Why This? |
|--------|-------------|-----------|
| **zCDP** | (ε,δ)-DP | Tighter composition, simpler budget tracking |
| **Discrete Gaussian** | Laplace | Better utility for same privacy, integer outputs |
| **Top-Down** | Bottom-Up | Consistency across hierarchy levels |
| **Winsorization** | Truncation | Preserves more data, smoother distribution |
| **Spark** | Pandas | Scales to billions of transactions |
| **Per-cell noise** | Per-record noise | Output perturbation is more efficient |

---

## 📊 Level 4: Deep Dive - Implementation Details

### Critical Code Sections

```
┌─────────────────────────────────────────────────────────────────────┐
│ Function: DiscreteGaussianMechanism.add_noise()                     │
├─────────────────────────────────────────────────────────────────────┤
│ WHAT: Add discrete Gaussian noise to integer counts                 │
│ WHY:  Discrete values avoid floating-point attacks, exact sampling  │
│ HOW:  1. Compute σ² from ρ and Δ                                    │
│       2. Sample from N_Z(0, σ²) using rejection sampling            │
│       3. Add noise to true count                                    │
│ MATH: σ² = Δ²/(2ρ), output = count + DiscreteGaussian(σ)           │
└─────────────────────────────────────────────────────────────────────┘
```

```
┌─────────────────────────────────────────────────────────────────────┐
│ Function: BudgetAllocator.allocate()                                │
├─────────────────────────────────────────────────────────────────────┤
│ WHAT: Split total privacy budget across queries and geo levels      │
│ WHY:  Different queries/levels need different noise amounts         │
│ HOW:  1. Split by geography: ρ_prov = 0.2ρ, ρ_city = 0.8ρ          │
│       2. Split by query: ρ_q = ρ_level / 4 for each query           │
│ MATH: ρ_total = ρ_province + ρ_city (composition)                   │
└─────────────────────────────────────────────────────────────────────┘
```

```
┌─────────────────────────────────────────────────────────────────────┐
│ Function: Preprocessor.winsorize()                                  │
├─────────────────────────────────────────────────────────────────────┤
│ WHAT: Cap extreme transaction amounts at 99th percentile            │
│ WHY:  Bounds sensitivity for total_amount query                     │
│ HOW:  1. Compute 99th percentile of amounts                         │
│       2. Replace values > p99 with p99                              │
│ MATH: amount_capped = min(amount, percentile_99)                    │
└─────────────────────────────────────────────────────────────────────┘
```

```
┌─────────────────────────────────────────────────────────────────────┐
│ Function: TopDownEngine.run()                                       │
├─────────────────────────────────────────────────────────────────────┤
│ WHAT: Apply noise hierarchically from Province → City               │
│ WHY:  Ensures consistency between aggregation levels                │
│ HOW:  1. Aggregate to province level, add noise                     │
│       2. Aggregate to city level, add noise                         │
│       3. Adjust city totals to sum to noisy province totals         │
│ MATH: Uses least-squares optimization for consistency               │
└─────────────────────────────────────────────────────────────────────┘
```

### Worked Example (with Numbers)

```
═══════════════════════════════════════════════════════════════════════
                    COMPLETE WORKED EXAMPLE
═══════════════════════════════════════════════════════════════════════

INPUT:
  Privacy budget: ρ = 1
  Raw data for Tehran, MCC=5411 (grocery), Day 1:
    - transaction_count = 1000
    - unique_cards = 850
    - unique_acceptors = 45
    - total_amount = 5,000,000 (after winsorization)

───────────────────────────────────────────────────────────────────────
STEP 1 - Budget Allocation:
───────────────────────────────────────────────────────────────────────
  Total ρ = 1
  
  Geographic split:
    ρ_province = 0.2 × 1 = 0.2
    ρ_city     = 0.8 × 1 = 0.8
  
  Query split (at city level):
    ρ_per_query = 0.8 / 4 = 0.2
  
  Verify composition:
    4 queries × 0.2 = 0.8 ✓

───────────────────────────────────────────────────────────────────────
STEP 2 - Compute σ for each query:
───────────────────────────────────────────────────────────────────────
  Formula: σ² = Δ² / (2ρ)
  
  For transaction_count (Δ=1, ρ=0.2):
    σ² = 1² / (2 × 0.2) = 1/0.4 = 2.5
    σ = √2.5 ≈ 1.58
  
  For unique_cards (Δ=1, ρ=0.2):
    σ² = 2.5, σ ≈ 1.58
  
  For unique_acceptors (Δ=1, ρ=0.2):
    σ² = 2.5, σ ≈ 1.58
  
  For total_amount (Δ=50000 winsorized cap, ρ=0.2):
    σ² = 50000² / 0.4 = 6.25 × 10⁹
    σ ≈ 79,057

───────────────────────────────────────────────────────────────────────
STEP 3 - Sample Noise:
───────────────────────────────────────────────────────────────────────
  noise ~ DiscreteGaussian(σ)
  
  Sampled values (example):
    noise_count      = +3      (from σ=1.58)
    noise_cards      = -2      (from σ=1.58)
    noise_acceptors  = +1      (from σ=1.58)
    noise_amount     = +45,231 (from σ=79,057)

───────────────────────────────────────────────────────────────────────
STEP 4 - Add Noise:
───────────────────────────────────────────────────────────────────────
  protected_count     = 1000 + 3      = 1003
  protected_cards     = 850 + (-2)    = 848
  protected_acceptors = 45 + 1        = 46
  protected_amount    = 5,000,000 + 45,231 = 5,045,231

───────────────────────────────────────────────────────────────────────
STEP 5 - Post-Process:
───────────────────────────────────────────────────────────────────────
  Ensure non-negative (all OK in this example):
    1003 ≥ 0 ✓
    848 ≥ 0 ✓
    46 ≥ 0 ✓
    5,045,231 ≥ 0 ✓

═══════════════════════════════════════════════════════════════════════
OUTPUT (for this cell):
═══════════════════════════════════════════════════════════════════════
  {
    "province": "تهران",
    "city": "تهران",
    "mcc": 5411,
    "day": 1,
    "transaction_count": 1003,      // true: 1000, error: 0.3%
    "unique_cards": 848,            // true: 850, error: 0.2%
    "unique_acceptors": 46,         // true: 45, error: 2.2%
    "total_amount": 5045231         // true: 5000000, error: 0.9%
  }
```

### State Diagram (Pipeline States)

```
┌─────────────┐
│ INITIALIZED │
└──────┬──────┘
       │ load_config()
       ▼
┌─────────────┐
│ CONFIGURED  │
└──────┬──────┘
       │ create_spark_session()
       ▼
┌─────────────┐
│ SPARK_READY │
└──────┬──────┘
       │ read_data()
       ▼
┌─────────────┐     error     ┌─────────────┐
│ DATA_LOADED │──────────────▶│   FAILED    │
└──────┬──────┘               └─────────────┘
       │ preprocess()                ▲
       ▼                             │
┌─────────────┐                      │
│ PREPROCESSED│──────────────────────┘
└──────┬──────┘     error
       │ apply_dp()
       ▼
┌─────────────┐
│ DP_APPLIED  │
└──────┬──────┘
       │ write_output()
       ▼
┌─────────────┐
│  COMPLETED  │
└─────────────┘
```

---

## 🔧 Edge Cases & Error Handling

| Edge Case | How Handled | Location |
|-----------|-------------|----------|
| Negative noise makes count < 0 | Clamped to 0 | `TopDownEngine._post_process()` |
| Zero privacy budget (ρ=0) | Raise ValueError | `BudgetAllocator.validate()` |
| Unknown city in data | Mapped to "Unknown" province | `GeographicHierarchy.get_province()` |
| Empty histogram cell | Kept as 0, noise still added | `Histogram.to_array()` |
| Amount > winsorization cap | Capped at 99th percentile | `Preprocessor.winsorize()` |
| Invalid date format | Spark fails with clear error | `TransactionReader.read()` |
| Missing columns in CSV | Raise KeyError with column name | `TransactionReader._validate_schema()` |
| Division by zero in budget | Checked before division | `BudgetAllocator.allocate()` |

---

## 📚 References

### Primary Papers
- **Bun & Steinke (2016)**: "Concentrated Differential Privacy: Simplifications, Extensions, and Lower Bounds" - zCDP definition and composition
- **Canonne et al. (2020)**: "Discrete Gaussian for Differential Privacy" - Exact sampling algorithm
- **Abowd et al. (2022)**: "The 2020 Census Disclosure Avoidance System TopDown Algorithm" - Top-down mechanism

### Related Implementations
- [US Census Bureau DAS](https://github.com/uscensusbureau/DAS_2020_Redistricting_Production_Code) - Original Census implementation
- [Google DP Library](https://github.com/google/differential-privacy) - Reference implementations
- [OpenDP](https://github.com/opendp/opendp) - Framework for DP

### Standards
- NIST SP 800-188: De-Identifying Government Datasets
- Census Bureau Disclosure Avoidance guidelines

---

## 🧪 Validation & Testing

### Test Suite Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                    TEST HIERARCHY                                    │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  test_no_spark.py           test_dp_correctness.py                  │
│  ├── Config                  ├── STATISTICAL (7 tests)              │
│  ├── Budget                  │   ├── Mean ≈ 0                       │
│  ├── Primitives              │   ├── Variance = σ²                  │
│  ├── Geography               │   ├── Skewness ≈ 0                   │
│  ├── Histogram               │   ├── Kurtosis ≈ 0                   │
│  └── Queries                 │   ├── Integer outputs                │
│                              │   ├── Chi-squared fit                │
│                              │   └── Independence                   │
│                              │                                       │
│                              ├── PRIVACY (6 tests)                  │
│                              │   ├── Sensitivity bounds             │
│                              │   ├── Budget composition             │
│                              │   ├── Membership inference           │
│                              │   ├── Reconstruction attack          │
│                              │   ├── Differencing attack            │
│                              │   └── Multiple query attack          │
│                              │                                       │
│                              ├── CORRECTNESS (5 tests)              │
│                              │   ├── Post-processing                │
│                              │   ├── Budget allocation              │
│                              │   ├── Sigma computation              │
│                              │   └── Edge cases                     │
│                              │                                       │
│                              ├── UTILITY (5 tests)                  │
│                              │   ├── Unbiasedness                   │
│                              │   ├── Error bounds                   │
│                              │   ├── Budget trade-off               │
│                              │   └── Relative error scaling         │
│                              │                                       │
│                              └── ADVERSARIAL (2 tests)              │
│                                  ├── Repeated query attack          │
│                                  └── Auxiliary info attack          │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

### Running Tests

```bash
# Basic unit tests (no Spark required)
python tests/test_no_spark.py

# Comprehensive DP correctness tests
python tests/test_dp_correctness.py

# Integration tests (requires Spark)
python examples/quick_test.py
```

### Test Categories Explained

#### 1. Statistical Tests

| Test | What It Checks | Pass Criteria |
|------|----------------|---------------|
| Mean ≈ 0 | Noise is centered | z-score < 3.0 |
| Variance = σ² | Noise magnitude correct | Within 10% of theory |
| Skewness ≈ 0 | Distribution symmetric | \|skew\| < 0.1 |
| Kurtosis ≈ 0 | Gaussian shape | \|kurt\| < 0.3 |
| Integer outputs | Discrete Gaussian | All samples ∈ ℤ |
| Chi-squared | Distribution fit | p-value > 0.01 |
| Independence | No autocorrelation | \|r\| < 3/√n |

#### 2. Privacy Tests (Attack Simulations)

```
┌─────────────────────────────────────────────────────────────────────┐
│ MEMBERSHIP INFERENCE ATTACK                                          │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  Attacker Goal: Determine if target record is in database            │
│                                                                      │
│  Setup:                                                              │
│    D₀: count = 100 (without target)                                 │
│    D₁: count = 101 (with target)                                    │
│                                                                      │
│  Attack:                                                             │
│    1. Observe noisy output                                           │
│    2. Guess "member" if output > 100.5                               │
│                                                                      │
│  Success Metric:                                                     │
│    accuracy ≈ 50% means DP is working (random guess)                │
│    accuracy >> 50% means privacy breach                              │
│                                                                      │
│  Our Result: accuracy ≈ 55% (advantage < 0.1) ✓                     │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

```
┌─────────────────────────────────────────────────────────────────────┐
│ DIFFERENCING ATTACK                                                  │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  Attacker Goal: Infer individual's value from aggregate queries      │
│                                                                      │
│  Setup:                                                              │
│    Q₁ = noisy(sum(D))           // All records                      │
│    Q₂ = noisy(sum(D - {target})) // Without target                  │
│                                                                      │
│  Attack:                                                             │
│    target_value ≈ Q₁ - Q₂                                           │
│                                                                      │
│  Why DP Protects:                                                    │
│    Both queries add independent noise                                │
│    Combined noise variance = 2σ²                                    │
│    Error in difference is √2 times single query error               │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

```
┌─────────────────────────────────────────────────────────────────────┐
│ RECONSTRUCTION ATTACK                                                │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  Attacker Goal: Exactly recover true value                           │
│                                                                      │
│  Result: Exact recovery rate < 50%                                  │
│  (With σ ≈ 0.7, probability of exact match is low)                  │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

#### 3. Utility Tests

| Test | Formula | Verification |
|------|---------|--------------|
| Unbiasedness | E[M(x)] = x | Mean of outputs = true value |
| MAE bound | MAE ≈ σ√(2/π) | Within 25% of theory |
| Budget trade-off | σ² ∝ 1/ρ | More budget → less error |
| Relative error | rel_err ∝ 1/count | Large counts have tiny % error |

---

## 📋 Complete Function Reference

### Core Module (`core/`)

#### `primitives.py` - Discrete Gaussian Mechanism

| Function | Purpose | Parameters |
|----------|---------|------------|
| `discrete_gaussian_scalar(sigma_sq, rng)` | Sample single value from N_Z(0, σ²) | σ² as (num, denom) tuple |
| `discrete_gaussian_vector(sigma_sq, size, rng)` | Sample vector of noise values | σ², count, optional RNG |
| `discrete_laplace_scalar(s, t, rng)` | Sample from Discrete Laplace | Scale params s/t |
| `bernoulli_exp_scalar(gamma, rng)` | Sample Bernoulli(exp(-γ)) exactly | γ as (num, denom) |
| `floorsqrt(num, denom)` | Exact floor(√(num/denom)) | Integer arithmetic only |
| `add_discrete_gaussian_noise(arr, rho, sens)` | Apply noise to numpy array | zCDP param ρ, sensitivity |
| `compute_discrete_gaussian_variance(sigma_sq)` | Get actual variance of discrete dist | σ² parameter |

#### `budget.py` - Privacy Budget Management

| Class/Method | Purpose |
|--------------|---------|
| `Budget(total_rho, delta, geo_split, query_split)` | Main budget manager |
| `Budget.get_geo_level_budget(level)` | Get ρ for province/city |
| `Budget.get_query_budget(query, level)` | Get ρ for specific query at level |
| `Budget.compute_sigma_for_query(query, level, sens)` | Compute σ from budget |
| `Budget.total_epsilon` | Convert ρ to (ε,δ)-DP |
| `BudgetAllocator(budget, num_days)` | Allocate across time dimension |
| `BudgetAllocation` | Dataclass holding allocation info |

### Schema Module (`schema/`)

#### `geography.py` - Geographic Hierarchy

| Class/Method | Purpose |
|--------------|---------|
| `Geography.from_csv(path)` | Load city→province mapping |
| `Geography.get_province(city)` | Look up province for city |
| `Geography.province_codes` | List of province codes |
| `Province` | Dataclass: code, name, cities |

#### `histogram.py` - Multi-dimensional Histogram

| Class/Method | Purpose |
|--------------|---------|
| `TransactionHistogram(dims, labels)` | Create histogram structure |
| `TransactionHistogram.set_value(p, c, m, d, query, val)` | Set cell value |
| `TransactionHistogram.get_query_array(query)` | Get 4D numpy array for query |
| `TransactionHistogram.aggregate_to_province(query)` | Sum over cities |
| `TransactionHistogram.copy()` | Deep copy histogram |
| `TransactionHistogram.QUERIES` | List of 4 query names |

### Engine Module (`engine/`)

#### `topdown.py` - Top-Down DP Engine

| Class/Method | Purpose |
|--------------|---------|
| `TopDownEngine(spark, config, geo, budget)` | Initialize engine |
| `TopDownEngine.run(histogram)` | Apply full DP pipeline |
| `TopDownEngine._apply_province_level_noise()` | Step 1: Province noise |
| `TopDownEngine._apply_city_level_noise()` | Step 2: City noise |
| `TopDownEngine._post_process()` | Step 3: Non-negativity |
| `TopDownEngine._get_sensitivity(query)` | Return Δ for query |
| `SimpleEngine` | Flat noise (no hierarchy) |

### Reader Module (`reader/`)

#### `spark_reader.py` - Data Reading

| Function | Purpose |
|----------|---------|
| `TransactionReader.read(path)` | Read CSV into Spark DataFrame |
| `TransactionReader._add_province_columns(df, geo)` | Join with geography |

#### `preprocessor.py` - Data Preprocessing

| Class/Method | Purpose |
|--------------|---------|
| `TransactionPreprocessor(spark, config, geo)` | Initialize |
| `TransactionPreprocessor.process(df)` | Full preprocessing pipeline |
| `_compute_winsorize_cap(df)` | Calculate 99th percentile |
| `_apply_winsorization(df)` | Cap extreme amounts |
| `_create_indices(df)` | Create day/city/mcc indices |
| `_aggregate_to_histogram(df)` | Build histogram from DataFrame |

### Queries Module (`queries/`)

#### `transaction_queries.py` - Query Definitions

| Class | Query | Sensitivity |
|-------|-------|-------------|
| `TransactionCountQuery` | count(*) | Δ = 1 |
| `UniqueCardsQuery` | count(distinct card) | Δ = 1 |
| `UniqueAcceptorsQuery` | count(distinct acceptor) | Δ = 1 |
| `TotalAmountQuery` | sum(amount) | Δ = winsorize_cap |
| `TransactionWorkload` | All 4 queries combined | Budget allocation |

---

## 🔬 Theoretical Guarantees

### Privacy Guarantee

For our implementation with ρ-zCDP:

```
Theorem: The complete pipeline satisfies ρ-zCDP where:
  
  ρ_total = ρ_province + ρ_city
          = Σ(ρ_query_at_province) + Σ(ρ_query_at_city)

Conversion to (ε, δ)-DP:
  ε = ρ + 2√(ρ · ln(1/δ))
  
Example (ρ=1, δ=10⁻¹⁰):
  ε = 1 + 2√(1 · ln(10¹⁰)) = 1 + 2√23 ≈ 10.6
```

### Utility Guarantee

```
For count queries with ρ-zCDP:
  σ² = 1/(2ρ)
  
Expected Error:
  E[|noise|] = σ · √(2/π) ≈ 0.798σ
  
95th Percentile Error:
  |noise| < 1.96σ with 95% probability

Relative Error for count n:
  rel_err ≈ σ/n = 1/(n·√(2ρ))
```

### Post-Processing Theorem

```
Theorem (Free Post-Processing):
If M satisfies ρ-zCDP and g is any function,
then g ∘ M also satisfies ρ-zCDP.

Applied Operations (all free):
  - Rounding to integers
  - Clamping to non-negative
  - Aggregation to higher levels
  - Format conversion
```

---

## 🛡️ Census 2020 Compliance Features

This section explains the additional features added to match US Census 2020 DAS methodology.

### 1. Cell Suppression

#### What is Suppression?

Cells with very small counts are **suppressed** (hidden) to prevent disclosure of individuals even after DP noise.

```
┌─────────────────────────────────────────────────────────────────────┐
│                    SUPPRESSION RULES                                 │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  Before Suppression:                                                 │
│    City A, MCC 1234: transaction_count = 3  (noisy)                 │
│    City A, MCC 5678: transaction_count = 150 (noisy)                │
│                                                                      │
│  After Suppression (threshold = 10):                                │
│    City A, MCC 1234: SUPPRESSED (count < 10)                        │
│    City A, MCC 5678: 150 ✓                                          │
│                                                                      │
│  Why?                                                                │
│    Even with noise, a count of 3 reveals "very few" transactions    │
│    This could identify individuals in small groups                   │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

#### Suppression Methods

| Method | Behavior | Use Case |
|--------|----------|----------|
| `flag` | Add `is_suppressed=True` column | Analysis can filter |
| `null` | Set suppressed values to NULL | Database compatibility |
| `value` | Set to sentinel (e.g., -1) | Legacy systems |

#### Configuration

```ini
[privacy]
suppression_threshold = 10      # Minimum count to release
suppression_method = flag       # flag, null, or value
suppression_sentinel = -1       # For value method
```

#### Complementary Suppression

If one cell in a group is suppressed, others may need suppression too:

```
Province A cities (before):
  City 1: 5  → SUPPRESSED
  City 2: 100
  City 3: 95
  Province Total: 200 (exact invariant)

Problem: 
  Attacker computes: City 1 = 200 - 100 - 95 = 5

Solution (Complementary Suppression):
  City 1: SUPPRESSED
  City 2: SUPPRESSED (complementary)
  City 3: 95
  
Now attacker can only know: City 1 + City 2 = 105
```

---

### 2. Confidence Intervals

#### Why Confidence Intervals?

Data users need to know the **uncertainty** in released values. We provide:

- **MOE (Margin of Error)**: ± range around the value
- **CI (Confidence Interval)**: [lower, upper] bounds
- **Relative MOE**: MOE as percentage of value

#### Mathematical Basis

For Discrete Gaussian with variance σ²:

```
Standard Error:     SE = σ
90% MOE:           MOE₉₀ = 1.645 × σ
95% MOE:           MOE₉₅ = 1.960 × σ
99% MOE:           MOE₉₉ = 2.576 × σ

Confidence Interval:
  CI₉₀ = [value - MOE₉₀, value + MOE₉₀]
```

#### Example

```
Protected value:     transaction_count = 1,234
σ (from budget):     σ = 15.8

90% Confidence Interval:
  MOE₉₀ = 1.645 × 15.8 = 26.0
  CI₉₀ = [1,234 - 26, 1,234 + 26] = [1,208, 1,260]

Interpretation:
  "We are 90% confident the true count is between 1,208 and 1,260"

Relative MOE:
  rel_MOE = 26 / 1,234 = 2.1%
  "Error is about 2% of the value"
```

#### Output Schema

```
Original columns:
  transaction_count, unique_cards, ...

With confidence intervals (90%):
  transaction_count
  transaction_count_moe_90        # Margin of error
  transaction_count_ci_lower_90   # Lower bound
  transaction_count_ci_upper_90   # Upper bound
  transaction_count_rel_moe_90    # Relative MOE (optional)
```

#### Configuration

```ini
[privacy]
confidence_levels = 0.90          # Can be: 0.90,0.95 for multiple
include_relative_moe = true       # Include percentage error
```

---

### 3. Global Sensitivity

#### The Problem

A single card can appear in **multiple cells**:

```
Card #1234 transactions:
  - City A, MCC 5411 (grocery), Day 1
  - City A, MCC 5411 (grocery), Day 2
  - City B, MCC 5812 (restaurant), Day 1
  - City C, MCC 5812 (restaurant), Day 3

This card affects 4 different (city, mcc, day) cells!
```

#### Local vs Global Sensitivity

```
┌─────────────────────────────────────────────────────────────────────┐
│                    SENSITIVITY COMPARISON                            │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  LOCAL SENSITIVITY (incorrect for our case):                         │
│    Assumes each card affects 1 cell                                 │
│    L2 sensitivity = K                                               │
│    ❌ Underestimates true sensitivity                               │
│                                                                      │
│  GLOBAL SENSITIVITY (correct):                                       │
│    Card can appear in M cells                                       │
│    Each cell affected by at most K transactions                     │
│    L2 sensitivity = √M × K                                          │
│    ✅ Correct privacy guarantee                                     │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

#### L2 Sensitivity Formula

For a card appearing in M cells with K transactions per cell:

```
                    ┌─────────────────────────────────┐
                    │                                 │
                    │   Δ₂ = √(M × K²) = √M × K      │
                    │                                 │
                    └─────────────────────────────────┘

Where:
  M = Maximum cells any card appears in
  K = Per-cell contribution bound (from bounded contribution)
```

#### Sensitivity by Query

| Query | Sensitivity | Example (M=100, K=5, W=10M) |
|-------|-------------|----------------------------|
| transaction_count | √M × K | √100 × 5 = 50 |
| unique_cards | √M × 1 | √100 × 1 = 10 |
| unique_acceptors | √M × 1 | √100 × 1 = 10 |
| total_amount | √M × K × W | √100 × 5 × 10⁷ = 5×10⁸ |

#### Configuration

```ini
[privacy]
# Method: local (Δ=K), global (Δ=√M×K), fixed (Δ=√fixed×K)
sensitivity_method = global

# For fixed method only
fixed_max_cells_per_card = 100
```

#### Impact on Noise

Higher sensitivity means more noise:

```
σ² = Δ₂² / (2ρ)

Example comparison:
  Local (Δ=5):   σ² = 25/(2×0.25) = 50    → σ = 7.1
  Global (Δ=50): σ² = 2500/(2×0.25) = 5000 → σ = 70.7

Global sensitivity adds 10x more noise in this example!
But this is NECESSARY for correct privacy.
```

---

### 4. Complete Census 2020 Pipeline

#### Full Configuration Example

```ini
[privacy]
# Budget (monthly)
total_rho = 1/4
delta = 1e-10

# Geographic allocation
geographic_split_province = 0.2
geographic_split_city = 0.8

# Query allocation
query_split_transaction_count = 0.25
query_split_unique_cards = 0.25
query_split_unique_acceptors = 0.25
query_split_total_amount = 0.25

# Bounded Contribution
contribution_bound_method = iqr
contribution_bound_iqr_multiplier = 1.5
contribution_bound_percentile = 99

# Suppression
suppression_threshold = 10
suppression_method = flag

# Confidence Intervals
confidence_levels = 0.90
include_relative_moe = true

# Global Sensitivity
sensitivity_method = global
```

#### Pipeline Execution Flow

```
┌─────────────────────────────────────────────────────────────────────┐
│              COMPLETE CENSUS 2020-STYLE PIPELINE                     │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  1. LOAD DATA                                                        │
│     └── Read transactions from Parquet/CSV                          │
│                                                                      │
│  2. BOUNDED CONTRIBUTION                                             │
│     ├── Compute K using IQR method                                  │
│     └── Clip transactions per card-cell to K                        │
│                                                                      │
│  3. COMPUTE GLOBAL SENSITIVITY                                       │
│     ├── Find D_max = max cells per card                              │
│     └── Δ₂ = √D_max × K for each query                              │
│                                                                      │
│  4. COMPUTE PROVINCE-MONTH INVARIANTS (PUBLIC DATA)                 │
│     ├── Province-month totals (EXACT - no noise)                     │
│     └── These match publicly published statistics                  │
│                                                                      │
│  5. ADD NOISE (cell level: city, mcc, day) - FULL BUDGET             │
│     ├── σ² = Δ₂² / (2ρ) where ρ = total_rho (100% to cells)         │
│     └── noise ~ Discrete Gaussian(σ²)                               │
│                                                                      │
│  6. NNLS POST-PROCESSING                                             │
│     └── Adjust cell values to sum to province-month invariant       │
│         (minimize distortion while matching public totals)           │
│                                                                      │
│  7. CONTROLLED ROUNDING                                              │
│     └── Round to integers preserving province-month sums            │
│                                                                      │
│  8. ADD CONFIDENCE INTERVALS                                         │
│     └── MOE, CI lower/upper for each query                          │
│                                                                      │
│  9. APPLY SUPPRESSION                                                │
│     └── Suppress cells with count < threshold                       │
│                                                                      │
│  10. WRITE OUTPUT                                                    │
│      └── Partitioned Parquet with metadata                          │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

#### Output Example

```
┌──────────────┬─────────────────┬──────┬─────────┬───────────────────┬─────────────┬────────────┬────────────┬──────────────┬──────────────┐
│ province_name│ acceptor_city   │ mcc  │ day_idx │ transaction_count │ tc_moe_90   │ tc_ci_low  │ tc_ci_high │ is_suppressed│ supp_reason  │
├──────────────┼─────────────────┼──────┼─────────┼───────────────────┼─────────────┼────────────┼────────────┼──────────────┼──────────────┤
│ Tehran       │ Tehran          │ 5411 │ 0       │ 15234             │ 116.4       │ 15117.6    │ 15350.4    │ false        │ null         │
│ Tehran       │ Tehran          │ 5812 │ 0       │ 8921              │ 116.4       │ 8804.6     │ 9037.4     │ false        │ null         │
│ Tehran       │ Karaj           │ 5411 │ 0       │ 3                 │ 116.4       │ -113.4     │ 119.4      │ true         │ count < 10   │
│ Isfahan      │ Isfahan         │ 5411 │ 0       │ 7823              │ 116.4       │ 7706.6     │ 7939.4     │ false        │ null         │
└──────────────┴─────────────────┴──────┴─────────┴───────────────────┴─────────────┴────────────┴────────────┴──────────────┴──────────────┘
```

---

## 📊 Comparison with US Census 2020

| Feature | Census 2020 | Our Implementation |
|---------|-------------|-------------------|
| **Mechanism** | Discrete Gaussian | Discrete Gaussian ✅ |
| **Framework** | zCDP | zCDP ✅ |
| **Hierarchy** | 6 levels (Nation→Block) | 2 levels (Province→City) |
| **NNLS** | Yes | Yes ✅ |
| **Controlled Rounding** | Yes | Yes ✅ |
| **Invariants** | Total population exact | Monthly totals exact ✅ |
| **Suppression** | Yes | Yes ✅ |
| **Confidence Intervals** | Published separately | Included in output ✅ |
| **Global Sensitivity** | N/A (one residence) | √M × K ✅ |
| **Bounded Contribution** | 1 person = 1 record | K transactions/cell ✅ |

### Key Differences Explained

1. **Geography**: Census has 6 levels because US has complex hierarchy. We have 2 levels (Province → City) which is sufficient for transaction data.

2. **Global Sensitivity**: Census doesn't need this because each person has exactly one residence. In transaction data, a card can appear in many (city, mcc, day) cells.

3. **Bounded Contribution**: Census counts people (1 per cell). We count transactions (K per cell after clipping).

4. **Release Frequency**: Census releases once per decade. We release monthly, so annual ε accumulates to 12× monthly ε.

