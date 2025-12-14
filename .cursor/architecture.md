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

## Data Flow

1. **Read**: Raw transactions from Parquet/CSV → Spark DataFrame
2. **Preprocess**: 
   - Winsorize amounts (cap outliers)
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

### Schema Modules

| Module | Responsibility | Key Classes |
|--------|---------------|-------------|
| `schema/geography.py` | Province/City hierarchy | `Geography` |
| `schema/histogram_spark.py` | Multi-dimensional histogram | `SparkHistogram` |

### Reader Modules

| Module | Responsibility | Key Classes |
|--------|---------------|-------------|
| `reader/spark_reader.py` | Read transaction data | `SparkTransactionReader` |
| `reader/preprocessor.py` | Preprocessing pipeline | `TransactionPreprocessor` |

### Engine Modules

| Module | Responsibility | Key Classes |
|--------|---------------|-------------|
| `engine/topdown_spark.py` | SDC noise application | `TopDownSparkEngine` |

### Writer Modules

| Module | Responsibility | Key Classes |
|--------|---------------|-------------|
| `writer/parquet_writer.py` | Write protected output | `ParquetWriter` |

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

## Error Handling

- Early validation: config, file existence
- Specific exceptions: `FileNotFoundError`, `ValueError`, `RuntimeError`
- Logging: Appropriate levels (error, warning, info)
- Pipeline results: Success/failure tracked in `PipelineResult`

## Thread Safety

- Spark sessions: Reuse existing if available (notebook-friendly)
- No shared mutable state between components
- Each pipeline run is independent

