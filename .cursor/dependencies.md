# Module Dependencies

## Dependency Graph

```
core/pipeline.py
├── core/config.py
├── reader/spark_reader.py
│   ├── core/config.py
│   └── schema/geography.py
├── reader/preprocessor.py
│   ├── core/config.py
│   ├── schema/geography.py
│   ├── core/bounded_contribution.py
│   └── schema/histogram_spark.py
├── engine/topdown_spark.py
│   ├── core/config.py
│   ├── schema/geography.py
│   ├── schema/histogram_spark.py
│   ├── core/plausibility_bounds.py (implicit, computed in engine)
│   └── core/rounder.py (implicit, in _controlled_rounding)
└── writer/parquet_writer.py
    └── core/config.py
```

## Core Module Dependencies

### `core/config.py`
- **Dependencies**: None (base module)
- **Used by**: All modules
- **Purpose**: Configuration management
- **Refactoring Notes**: Can be refactored independently, but changes affect all modules

### `core/pipeline.py`
- **Dependencies**: 
  - `core/config.py`
  - `reader/spark_reader.py`
  - `reader/preprocessor.py`
  - `engine/topdown_spark.py`
  - `writer/parquet_writer.py`
- **Used by**: Entry points (examples, scripts)
- **Purpose**: Pipeline orchestration
- **Refactoring Notes**: High-level orchestration, safe to refactor structure

### `core/bounded_contribution.py`
- **Dependencies**: None (pure Spark operations)
- **Used by**: `reader/preprocessor.py`
- **Purpose**: Compute K (contribution bound)
- **Refactoring Notes**: Can be refactored independently

### `core/plausibility_bounds.py`
- **Dependencies**: None (pure Spark operations)
- **Used by**: `engine/topdown_spark.py` (implicitly, logic is in engine)
- **Purpose**: Compute plausibility bounds per context
- **Refactoring Notes**: Currently logic is in `topdown_spark.py`, could be extracted

### `core/rounder.py`
- **Dependencies**: None (pure Python/NumPy)
- **Used by**: `engine/topdown_spark.py` (implicitly, logic is in engine)
- **Purpose**: Controlled rounding
- **Refactoring Notes**: Currently logic is in `topdown_spark.py._controlled_rounding()`, could be extracted

### `core/invariants.py`
- **Dependencies**: None (pure Spark operations)
- **Used by**: Alternative DP implementations (not main SDC pipeline)
- **Purpose**: Manage exact totals
- **Refactoring Notes**: Not used in main pipeline, safe to refactor

## Schema Module Dependencies

### `schema/geography.py`
- **Dependencies**: None (pure Python)
- **Used by**: 
  - `core/pipeline.py`
  - `reader/spark_reader.py`
  - `reader/preprocessor.py`
  - `engine/topdown_spark.py`
- **Purpose**: Province/City hierarchy
- **Refactoring Notes**: Core data structure, refactor carefully

### `schema/histogram_spark.py`
- **Dependencies**: 
  - `schema/geography.py`
- **Used by**: 
  - `reader/preprocessor.py`
  - `engine/topdown_spark.py`
  - `writer/parquet_writer.py`
- **Purpose**: Multi-dimensional histogram wrapper
- **Refactoring Notes**: Core data structure, refactor carefully

## Reader Module Dependencies

### `reader/spark_reader.py`
- **Dependencies**: 
  - `core/config.py`
  - `schema/geography.py`
- **Used by**: `core/pipeline.py`
- **Purpose**: Read transaction data
- **Refactoring Notes**: Can be refactored independently

### `reader/preprocessor.py`
- **Dependencies**: 
  - `core/config.py`
  - `schema/geography.py`
  - `core/bounded_contribution.py`
  - `schema/histogram_spark.py`
- **Used by**: `core/pipeline.py`
- **Purpose**: Preprocessing (winsorization, bounded contribution, aggregation)
- **Refactoring Notes**: Can refactor internal structure, but preserve output format

## Engine Module Dependencies

### `engine/topdown_spark.py`
- **Dependencies**: 
  - `core/config.py`
  - `schema/geography.py`
  - `schema/histogram_spark.py`
- **Used by**: `core/pipeline.py`
- **Purpose**: Apply SDC noise
- **Refactoring Notes**: **CRITICAL** - Contains province invariant logic, refactor with extreme caution

## Writer Module Dependencies

### `writer/parquet_writer.py`
- **Dependencies**: 
  - `core/config.py`
- **Used by**: `core/pipeline.py`
- **Purpose**: Write protected output
- **Refactoring Notes**: Can be refactored independently

## External Dependencies

### Spark
- **Used by**: All modules that process data
- **Version**: Compatible with Spark 3.x
- **Refactoring Notes**: Spark API changes require updates across modules

### NumPy/Pandas
- **Used by**: 
  - `engine/topdown_spark.py` (controlled rounding, pandas UDFs)
  - Tests
- **Refactoring Notes**: Minimal usage, mostly in rounding logic

## Circular Dependencies

**None detected** - The dependency graph is acyclic.

## Dependency Injection Points

### Spark Session
- **Injected at**: `core/pipeline.py`
- **Passed to**: All components that need Spark
- **Pattern**: Constructor injection

### Config
- **Injected at**: `core/pipeline.py`
- **Passed to**: All components
- **Pattern**: Constructor injection

### Geography
- **Injected at**: `core/pipeline.py`
- **Passed to**: Reader, Preprocessor, Engine
- **Pattern**: Constructor injection

## Refactoring Impact Analysis

### Low Impact (Safe to Refactor)
- `core/config.py` - Changes affect all, but structure is simple
- `writer/parquet_writer.py` - Isolated, only writes output
- `reader/spark_reader.py` - Isolated, only reads input
- `core/bounded_contribution.py` - Used only in preprocessor

### Medium Impact (Refactor with Care)
- `reader/preprocessor.py` - Output format must match engine input
- `schema/geography.py` - Used by many modules
- `schema/histogram_spark.py` - Core data structure

### High Impact (Refactor with Extreme Caution)
- `engine/topdown_spark.py` - Contains critical invariant logic
- `core/pipeline.py` - Orchestrates everything

## Suggested Dependency Improvements

1. **Extract Plausibility Bounds**: Move logic from `topdown_spark.py` to `core/plausibility_bounds.py`
2. **Extract Rounding**: Move `_controlled_rounding()` from `topdown_spark.py` to `core/rounder.py`
3. **Interface for Engines**: Create abstract base class for engines (future extensibility)

