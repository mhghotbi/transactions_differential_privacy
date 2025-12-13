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
│  (Aggregation)  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Histogram      │
│  (City×MCC×Day) │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Budget         │
│  Allocation     │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Top-Down       │
│  DP Engine      │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Post-Processing│
│  (NNLS, Round)  │
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
- **Responsibilities**:
  - Load configuration from INI files
  - Validate configuration parameters
  - Provide type-safe access to settings

#### `budget.py`
- **Purpose**: Privacy budget allocation and composition
- **Key Classes**: `Budget`, `BudgetAllocator`, `MonthlyBudgetManager`
- **Responsibilities**:
  - Allocate privacy budget across geographic levels and query types
  - Track budget consumption
  - Compute noise scale (σ) from budget allocation
  - Handle sequential and parallel composition

#### `primitives.py`
- **Purpose**: Core DP mechanisms
- **Key Classes**: `DiscreteGaussianMechanism`
- **Responsibilities**:
  - Implement discrete Gaussian noise mechanism
  - Generate integer noise samples
  - Maintain exact integer outputs

#### `sensitivity.py`
- **Purpose**: Sensitivity analysis for user-level DP
- **Key Classes**: `GlobalSensitivityCalculator`, `UserLevelSensitivity`
- **Responsibilities**:
  - Compute global sensitivity accounting for multi-cell contributions
  - Calculate D_max (max cells per card)
  - Compute K bound (max transactions per card per cell)
  - Handle stratified sensitivity by MCC groups

#### `pipeline.py`
- **Purpose**: Orchestrate entire DP workflow
- **Key Classes**: `DPPipeline`, `PipelineResult`
- **Responsibilities**:
  - Coordinate data reading, processing, and writing
  - Manage Spark session lifecycle
  - Handle errors and validation
  - Return execution results

#### `postprocessing.py`
- **Purpose**: Post-processing to maintain consistency
- **Key Classes**: `NNLSPostProcessor`
- **Responsibilities**:
  - Non-negative least squares optimization
  - Maintain geographic consistency (city sums = province totals)
  - Preserve privacy guarantees

#### `rounder.py`
- **Purpose**: Controlled rounding to integers
- **Key Classes**: `ControlledRounder`
- **Responsibilities**:
  - Round noisy values to integers
  - Maintain exact totals

#### `invariants.py`
- **Purpose**: Maintain exact invariants
- **Key Classes**: `InvariantManager`
- **Responsibilities**:
  - Enforce exact province and national totals
  - Adjust city-level values to match province totals

#### `suppression.py`
- **Purpose**: Cell suppression for small counts
- **Key Classes**: `SuppressionManager`
- **Responsibilities**:
  - Identify cells below suppression threshold
  - Apply suppression (flag/null/value)
  - Protect against reconstruction attacks

#### `confidence.py`
- **Purpose**: Confidence interval computation
- **Key Classes**: `ConfidenceIntervalCalculator`
- **Responsibilities**:
  - Compute margins of error from noise variance
  - Generate confidence intervals at specified levels
  - Account for post-processing effects

### Engine (`engine/`)

#### `topdown.py`
- **Purpose**: Top-down DP algorithm implementation
- **Key Classes**: `TopDownEngine`
- **Responsibilities**:
  - Apply noise hierarchically (province → city)
  - Coordinate with budget allocator
  - Manage user-level DP parameters
  - Handle stratified sensitivity

### Reader (`reader/`)

#### `spark_reader.py`
- **Purpose**: Data reading with Spark
- **Key Classes**: `SparkReader`
- **Responsibilities**:
  - Read Parquet files efficiently
  - Handle schema validation
  - Support partitioned data

#### `preprocessor.py` / `preprocessor_distributed.py`
- **Purpose**: Data preprocessing and aggregation
- **Key Classes**: `Preprocessor`, `DistributedPreprocessor`
- **Responsibilities**:
  - Aggregate transactions to histogram cells
  - Compute user-level statistics (D_max, K)
  - Handle data quality issues

### Writer (`writer/`)

#### `parquet_writer.py`
- **Purpose**: Write protected output
- **Key Classes**: `ParquetWriter`
- **Responsibilities**:
  - Write partitioned Parquet files
  - Include confidence intervals
  - Apply suppression flags

### Schema (`schema/`)

#### `geography.py`
- **Purpose**: Geographic hierarchy management
- **Key Classes**: `Geography`
- **Responsibilities**:
  - Manage province-city relationships
  - Validate geographic consistency

#### `histogram.py`
- **Purpose**: Multi-dimensional histogram structure
- **Key Classes**: `Histogram`
- **Responsibilities**:
  - Represent aggregated data structure
  - Support hierarchical queries

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
2. **Preprocess**: Aggregate to `(city, mcc, day)` cells
3. **Analyze**: Compute D_max, K, and sensitivity parameters
4. **Allocate**: Split privacy budget across levels/queries
5. **Noise**: Apply discrete Gaussian noise hierarchically
6. **Post-process**: NNLS optimization for consistency
7. **Round**: Controlled rounding to integers
8. **Suppress**: Hide small cells
9. **CI**: Compute confidence intervals
10. **Write**: Output protected statistics

### Output Schema
```
- province_name: Province name
- acceptor_city: City name
- mcc: Merchant Category Code
- day_idx: Day index (0-29)
- transaction_count: Protected count
- transaction_count_moe_90: 90% Margin of Error
- transaction_count_ci_lower_90: CI lower bound
- transaction_count_ci_upper_90: CI upper bound
- unique_cards: Protected unique card count
- unique_acceptors: Protected unique acceptor count
- total_amount: Protected total amount
- is_suppressed: Suppression flag
```

## Privacy Architecture

### Privacy Framework: zCDP (zero-Concentrated DP)
- **Privacy Unit**: Card (user-level DP)
- **Mechanism**: Discrete Gaussian
- **Composition**: Sequential (across days) and parallel (across queries)

### Budget Allocation
```
Total ρ = 0.25 per month
├── Geographic Split
│   ├── Province: 20%
│   └── City: 80%
└── Query Split (per geographic level)
    ├── transaction_count: 34%
    ├── unique_cards: 33%
    └── total_amount: 33%
```

### Sensitivity Analysis
- **Global Sensitivity**: √(D_max × K)
  - D_max: Maximum distinct cells per card
  - K: Maximum transactions per card per cell (bounded contribution)
- **Stratified Sensitivity**: Different sensitivity by MCC group

## Technology Stack

- **Language**: Python 3.11+
- **Distributed Computing**: Apache Spark 3.5+
- **Scientific Computing**: NumPy, SciPy
- **Data Format**: Parquet (columnar storage)
- **Java**: Required for Spark (Java 8 or 11)

## Scalability Considerations

- **Horizontal Scaling**: Spark cluster with multiple executors
- **Partitioning**: Data partitioned by province for parallel processing
- **Broadcast Variables**: Small lookup tables (city-province mapping)
- **Memory Management**: Configurable Spark memory settings
- **Shuffle Optimization**: Appropriate partition count for joins

## Error Handling

- **Validation**: Early validation of configuration and data
- **Logging**: Structured logging at appropriate levels
- **Graceful Degradation**: Continue processing when possible
- **Result Tracking**: Return detailed results with errors

