# System Architecture

## Overview

This is a **Statistical Disclosure Control (SDC)** system for financial transaction data. The main pipeline uses multiplicative jitter with context-aware plausibility bounds, designed for secure enclave deployment.

## High-Level Architecture

```
┌──────────────┐    ┌──────────────┐    ┌──────────────┐    ┌───────────┐
│   READER     │    │ PREPROCESSOR │    │   ENGINE     │    │  WRITER   │
│              │───▶│              │───▶│              │───▶│           │
│ spark_reader │    │ winsorize    │    │ topdown_spark│    │ parquet   │
│              │    │ bound contrib│    │ plausibility │    │ output    │
│              │    │ aggregate    │    │ ratio preserve│   │           │
└──────────────┘    └──────────────┘    └──────────────┘    └───────────┘
```

Alternative detailed view:

```
┌─────────────────┐
│  Raw Transactions│
│   (10B+ rows)   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Spark Reader   │
│  (Parquet I/O)  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Preprocessor   │
│  (Winsorize, K) │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Histogram      │
│  (Prov×City×MCC│
│   ×Day×Weekday) │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Context-Aware  │
│  Bounds Calc    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  SDC Engine     │
│  (Multiplicative│
│   Jitter)       │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Ratio Preserve │
│  (Invariants)   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Protected      │
│  Output         │
│  (Parquet)      │
└─────────────────┘
```

## Component Responsibilities

### Core Modules

| Module | Responsibility | Key Classes |
|--------|---------------|-------------|
| `core/config.py` | Configuration management | `Config`, `PrivacyConfig`, `DataConfig`, `SparkConfig` |
| `core/pipeline.py` | Pipeline orchestration | `DPPipeline`, `PipelineResult` |
| `core/bounded_contribution.py` | Bound card contributions (K) | `BoundedContributionCalculator` |
| `core/plausibility_bounds.py` | Data-driven bounds | `PlausibilityBoundsCalculator` |
| `core/suppression.py` | Cell suppression | `SuppressionManager` |
| `core/invariants.py` | Exact totals management | `InvariantManager`, `InvariantEnforcer` |
| `core/rounder.py` | Controlled rounding | `CensusControlledRounder` |

#### `config.py` Details
- **Purpose**: Configuration management and validation
- **SDC Settings**:
  - `noise_level`: Relative noise level (default: 0.15 = 15%)
  - `noise_seed`: Random seed for reproducibility
  - `contribution_bound_method`: Method for computing K (transaction_weighted_percentile recommended)
  - `mcc_cap_percentile`: Percentile for per-MCC winsorization (default: 99.0)
  - `suppression_threshold`: Minimum count threshold for suppression

#### `pipeline.py` Details
- **Purpose**: Orchestrate entire SDC workflow
- **Responsibilities**:
  - Coordinate data reading, preprocessing, SDC application, and writing
  - Manage Spark session lifecycle
  - Handle errors and validation
  - Return execution results
- **Note**: Class name is `DPPipeline` for historical reasons, but implements SDC

#### `bounded_contribution.py` Details
- **Purpose**: Compute and apply bounded contribution (K)
- **Methods**:
  - `transaction_weighted_percentile`: Recommended - minimizes data loss
  - `iqr`: Interquartile range method
  - `percentile`: Simple percentile
  - `fixed`: Fixed value
- **Purpose**: Limits transactions per card per cell to prevent outliers

### Schema Modules

| Module | Responsibility | Key Classes |
|--------|---------------|-------------|
| `schema/geography.py` | Province/City hierarchy | `Geography` |
| `schema/histogram_spark.py` | Multi-dimensional histogram | `SparkHistogram` |

#### `geography.py` Details
- **Purpose**: Geographic hierarchy management
- **Responsibilities**:
  - Manage province-city relationships
  - Validate geographic consistency
  - Support invariant enforcement

#### `histogram_spark.py` Details
- **Purpose**: Multi-dimensional histogram structure
- **Responsibilities**:
  - Represent aggregated data structure
  - Support hierarchical queries
  - Maintain cell-level statistics

### Reader Modules

| Module | Responsibility | Key Classes |
|--------|---------------|-------------|
| `reader/spark_reader.py` | Read transaction data | `SparkTransactionReader` |
| `reader/preprocessor.py` | Preprocessing pipeline | `TransactionPreprocessor` |

#### `preprocessor.py` Details
- **Purpose**: Data preprocessing and aggregation
- **Responsibilities**:
  - Winsorize transaction amounts (per-MCC caps)
  - Apply bounded contribution (K)
  - Aggregate to histogram cells: (province, city, mcc, day, weekday)
  - Compute indices and metadata

### Engine Modules

| Module | Responsibility | Key Classes |
|--------|---------------|-------------|
| `engine/topdown_spark.py` | SDC noise application | `TopDownSparkEngine` |

#### `topdown_spark.py` Details
- **Purpose**: SDC engine implementation
- **Responsibilities**:
  - Compute context-aware plausibility bounds per (MCC, City, Weekday)
  - Apply multiplicative jitter: `M(c) = c × (1 + η)`
  - Preserve province invariants (exact totals)
  - Maintain ratio preservation (avg_amount, tx_per_card)

### Writer Modules

| Module | Responsibility | Key Classes |
|--------|---------------|-------------|
| `writer/parquet_writer.py` | Write protected output | `ParquetWriter` |

#### `parquet_writer.py` Details
- **Purpose**: Write protected output
- **Responsibilities**:
  - Write partitioned Parquet files (by province)
  - Include suppression flags
  - Apply output formatting

## Key Design Patterns

### 1. Pipeline Pattern
- `DPPipeline` orchestrates the entire workflow
- Each stage (read, preprocess, apply SDC, write) is a separate component
- Results flow through the pipeline as Spark DataFrames

### 2. Configuration Pattern
- All configuration in `Config` dataclass
- Loaded from INI files via `Config.from_ini()`
- Validated before pipeline execution

### 3. Spark-First Pattern
- All data operations use Spark DataFrames
- Pandas only for small datasets (< 100K rows) or evaluation
- Broadcast variables for small lookup tables

### 4. Invariant Preservation Pattern
- Province invariants computed once (Phase 1)
- All noisy values scaled to match invariants (Phase 6)
- Controlled rounding maintains invariants exactly (Phase 9)
- Final validation verifies invariants (Phase 10)

## Critical Invariants

### Province Count Invariants (MOST CRITICAL)
- **Definition**: `sum(city_counts_per_province) == province_total` (0% error)
- **Enforced at**: Phase 6 (scaling), Phase 9 (rounding), Phase 10 (validation)
- **Implementation**: See `engine/topdown_spark.py` Phase 6 and Phase 9

### Ratio Constraints
- `avg_amount = amount / count` within context-specific bounds
- `tx_per_card = count / cards` within context-specific bounds
- Enforced after noise application and scaling

### Logical Consistency
- If `count == 0`, then `cards == 0` and `amount == 0`
- If `count > 0`, then `cards >= 1` and `cards <= count`
- `amount >= 0` always

## Data Flow

### Input Data Schema
```
- pspiin: PSP identifier (optional)
- acceptorid: Acceptor/merchant identifier
- card_number: Card identifier (privacy unit)
- transaction_date: Date of transaction
- transaction_amount: Transaction amount
- city: City of the acceptor
- mcc: Merchant Category Code
```

### Processing Steps

1. **Read**: Raw transactions from Parquet/CSV → Spark DataFrame
2. **Preprocess**: 
   - Winsorize amounts (cap outliers per MCC at 99th percentile)
   - Bound contributions (K transactions per card per cell)
   - Aggregate to histogram: (province, city, mcc, day, weekday) → (count, cards, amount)
3. **Apply SDC**:
   - Compute province invariants (exact totals)
   - Compute plausibility bounds per (MCC, City, Weekday) context
   - Apply multiplicative jitter to all three values (count, cards, amount)
   - Clamp to bounds and validate ratios
   - Scale to match province invariants exactly
   - Controlled rounding maintaining invariants
4. **Write**: Protected histogram to Parquet

### Output Schema
```
- province_code: Province code
- province_name: Province name
- acceptor_city: City name
- mcc: Merchant Category Code
- day_idx: Day index (0-29)
- weekday: Weekday (0=Monday, 6=Sunday)
- transaction_count: Protected count
- unique_cards: Protected unique card count
- total_amount: Protected total amount
- avg_amount: Average transaction amount
- tx_per_card: Transactions per card
- is_suppressed: Suppression flag
```

## SDC Architecture

### Noise Mechanism: Multiplicative Jitter
```
M(c) = c × (1 + η),    where η ~ N(0, σ²)
σ = noise_level × c    (relative noise, e.g., 15%)
```

### Context-Aware Bounds
- **Stratum**: (MCC, City, Weekday)
- **Computation**: Data-driven from historical patterns
- **Purpose**: Ensure noise produces plausible values
- **Example**: Small city + Restaurant MCC + Weekday → different bounds than Large city + Gas MCC + Weekend

### Province Invariants
- **Province Totals**: Exact (no noise applied)
- **Enforcement**: City-level values adjusted to sum to province totals
- **Rationale**: Province totals are public data (known from other sources)

### Ratio Preservation
- **avg_amount**: `total_amount / transaction_count` stays within plausible range
- **tx_per_card**: `transaction_count / unique_cards` stays within plausible range
- **Method**: Adjust derived statistics after noise application

### Bounded Contribution (K)
- **Purpose**: Prevent outliers from dominating statistics
- **Method**: Transaction-weighted percentile (recommended)
- **Impact**: Limits each card to K transactions per cell
- **Benefit**: Improves utility by reducing outlier influence

## Data Structures

### SparkHistogram
- Wraps Spark DataFrame with histogram structure
- Dimensions: (province_idx, city_idx, mcc_idx, day_idx)
- Values: transaction_count, unique_cards, total_amount
- Processing dimension: weekday (used for bounds, not in final output)

### Geography
- Province/City hierarchy loaded from CSV
- Provides index mappings: city_name → city_idx, province_name → province_idx
- Used for aggregation and invariant computation

## Spark Optimizations

1. **Repartitioning**: By province_idx to minimize shuffles
2. **Broadcasting**: Small lookup tables (bounds, invariants)
3. **Caching**: Intermediate results reused multiple times
4. **Adaptive Execution**: Enabled for dynamic optimization

## Technology Stack

- **Language**: Python 3.8+
- **Distributed Computing**: Apache Spark 3.5+
- **Scientific Computing**: NumPy, SciPy
- **Data Format**: Parquet (columnar storage)
- **Java**: Required for Spark (Java 8 or 11)

## Scalability Considerations

- **Horizontal Scaling**: Spark cluster with multiple executors
- **Partitioning**: Data partitioned by province for parallel processing
- **Broadcast Variables**: Small lookup tables (city-province mapping, MCC caps)
- **Memory Management**: Configurable Spark memory settings
- **Shuffle Optimization**: Appropriate partition count for joins

## Error Handling

- Early validation: config, file existence
- Specific exceptions: `FileNotFoundError`, `ValueError`, `RuntimeError`
- Logging: Appropriate levels (error, warning, info)
- Pipeline results: Success/failure tracked in `PipelineResult`
- Graceful Degradation: Continue processing when possible

## Thread Safety

- Spark sessions: Reuse existing if available (notebook-friendly)
- No shared mutable state between components
- Each pipeline run is independent

## Secure Enclave Context

- **Primary Protection**: Physical isolation in secure enclave
- **Secondary Protection**: SDC plausibility-based noise
- **Deployment**: All processing occurs within secure enclave
- **Data Access**: No raw data leaves secure enclave
- **Output**: Only protected statistics are released
