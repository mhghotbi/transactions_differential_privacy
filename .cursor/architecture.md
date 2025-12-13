# System Architecture

## High-Level Architecture

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

## Component Architecture

### Core Modules (`core/`)

#### `config.py`
- **Purpose**: Configuration management and validation
- **Key Classes**: `Config`, `PrivacyConfig`, `DataConfig`, `SparkConfig`
- **SDC Settings**:
  - `noise_level`: Relative noise level (default: 0.15 = 15%)
  - `noise_seed`: Random seed for reproducibility
  - `contribution_bound_method`: Method for computing K (transaction_weighted_percentile recommended)
  - `mcc_cap_percentile`: Percentile for per-MCC winsorization (default: 99.0)
  - `suppression_threshold`: Minimum count threshold for suppression

#### `pipeline.py`
- **Purpose**: Orchestrate entire SDC workflow
- **Key Classes**: `DPPipeline`, `PipelineResult`
- **Responsibilities**:
  - Coordinate data reading, preprocessing, SDC application, and writing
  - Manage Spark session lifecycle
  - Handle errors and validation
  - Return execution results
- **Note**: Class name is `DPPipeline` for historical reasons, but implements SDC

#### `bounded_contribution.py`
- **Purpose**: Compute and apply bounded contribution (K)
- **Key Classes**: `BoundedContributionCalculator`
- **Methods**:
  - `transaction_weighted_percentile`: Recommended - minimizes data loss
  - `iqr`: Interquartile range method
  - `percentile`: Simple percentile
  - `fixed`: Fixed value
- **Purpose**: Limits transactions per card per cell to prevent outliers

#### `reader/preprocessor.py`
- **Purpose**: Data preprocessing and aggregation
- **Key Classes**: `Preprocessor`
- **Responsibilities**:
  - Winsorize transaction amounts (per-MCC caps)
  - Apply bounded contribution (K)
  - Aggregate to histogram cells: (province, city, mcc, day, weekday)
  - Compute indices and metadata

#### `engine/topdown_spark.py` (or similar)
- **Purpose**: SDC engine implementation
- **Key Classes**: `TopDownSparkEngine` (or similar)
- **Responsibilities**:
  - Compute context-aware plausibility bounds per (MCC, City, Weekday)
  - Apply multiplicative jitter: `M(c) = c × (1 + η)`
  - Preserve province invariants (exact totals)
  - Maintain ratio preservation (avg_amount, tx_per_card)

#### `writer/parquet_writer.py`
- **Purpose**: Write protected output
- **Key Classes**: `ParquetWriter`
- **Responsibilities**:
  - Write partitioned Parquet files (by province)
  - Include suppression flags
  - Apply output formatting

### Schema (`schema/`)

#### `geography.py`
- **Purpose**: Geographic hierarchy management
- **Key Classes**: `Geography`
- **Responsibilities**:
  - Manage province-city relationships
  - Validate geographic consistency
  - Support invariant enforcement

#### `histogram.py`
- **Purpose**: Multi-dimensional histogram structure
- **Key Classes**: `Histogram`
- **Responsibilities**:
  - Represent aggregated data structure
  - Support hierarchical queries
  - Maintain cell-level statistics

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

1. **Read**: Load transactions from Parquet files
2. **Preprocess**:
   - Winsorize amounts per MCC (99th percentile caps)
   - Apply bounded contribution (K) per card per cell
   - Compute weekday indices
3. **Aggregate**: Create histogram cells `(province, city, mcc, day, weekday)`
4. **Compute Bounds**: Calculate plausibility bounds per (MCC, City, Weekday) context
5. **Apply SDC**: Multiplicative jitter with context-aware clamping
6. **Preserve Invariants**: Ensure province totals remain exact
7. **Preserve Ratios**: Maintain plausible avg_amount and tx_per_card
8. **Suppress**: Hide small cells below threshold
9. **Write**: Output protected statistics

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

- **Validation**: Early validation of configuration and data
- **Logging**: Structured logging at appropriate levels
- **Graceful Degradation**: Continue processing when possible
- **Result Tracking**: Return detailed results with errors

## Secure Enclave Context

- **Primary Protection**: Physical isolation in secure enclave
- **Secondary Protection**: SDC plausibility-based noise
- **Deployment**: All processing occurs within secure enclave
- **Data Access**: No raw data leaves secure enclave
- **Output**: Only protected statistics are released

