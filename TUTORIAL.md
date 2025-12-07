# Transaction DP System - Linux Tutorial

## Prerequisites

- Python 3.8+
- Java 8 or 11 (OpenJDK recommended)
- At least 8GB RAM

## Quick Start

### 1. Check Java Version

```bash
java -version
# Should show: openjdk version "1.8.x" or "11.x.x"

# If not installed:
sudo apt update
sudo apt install openjdk-11-jdk
```

### 2. Create Virtual Environment (Recommended)

```bash
cd /path/to/transactions_differential_privacy

python3 -m venv venv
source venv/bin/activate
```

### 3. Install Dependencies

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

### 4. Verify city_province.csv

The `city_province.csv` should be in the `data/` directory:

```bash
# Should be at: data/city_province.csv
ls -la data/city_province.csv
```

### 5. Run Unit Tests (No Spark)

```bash
# Basic unit tests
python tests/test_no_spark.py

# DP correctness tests (statistical verification)
python tests/test_dp_correctness.py
```

Expected output:
```
============================================================
Transaction DP System - Unit Tests (No Spark)
============================================================

Testing Config...
  ✓ Config OK
Testing Budget...
  ✓ Budget OK
Testing Primitives...
  ✓ Primitives OK
...
============================================================
Results: 7 passed, 0 failed
============================================================
```

### 6. Run Quick Test (With Spark, 10K records)

```bash
python examples/quick_test.py
```

### 7. Generate Sample Data (1 Million Records)

```bash
python examples/generate_sample_data.py \
    --num-records 1000000 \
    --output data/sample_transactions.csv
```

### 8. Run Full Pipeline

```bash
python examples/run_pipeline.py \
    --num-records 1000000 \
    --rho 1
```

Or with existing data:

```bash
python examples/run_pipeline.py \
    --run-only \
    --input data/sample_transactions.csv \
    --output output/protected \
    --rho 1
```

## Configuration Options

### Privacy Budget (rho)

```bash
# Higher rho = less noise, less privacy
--rho 2

# Lower rho = more noise, more privacy
--rho 1/2
--rho 0.5
```

### Number of Records

```bash
--num-records 100000    # 100K (quick test)
--num-records 1000000   # 1M (default)
--num-records 10000000  # 10M (large scale)
```

## Output Files

After running, output is in `output/dp_protected/`:

```
output/dp_protected/
├── protected_data/           # Parquet files (partitioned by province)
│   ├── province_name=تهران/
│   ├── province_name=اصفهان/
│   └── ...
└── metadata.json             # Execution metadata
```

## Reading Output in Python

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ReadOutput").getOrCreate()

# Read protected data
df = spark.read.parquet("output/dp_protected/protected_data")
df.show()

# Aggregate by province
df.groupBy("province_name").agg(
    {"transaction_count": "sum", "total_amount": "sum"}
).show()
```

---

## Spark Configuration Guide

### Configuration Hierarchy

Spark settings can be configured in multiple places (priority order):

```
1. spark-submit command line (highest priority)
2. SparkSession.builder.config() in code
3. default.ini [spark] section
4. spark-defaults.conf (cluster default)
```

### [spark] Section in default.ini

```ini
[spark]
# Application name (appears in Spark UI)
app_name = TransactionDP

# Master URL
# local[*]     - Local mode, all cores
# local[4]     - Local mode, 4 cores
# spark://host:7077 - Standalone cluster
# yarn         - YARN cluster
master = local[*]

# Memory settings
executor_memory = 4g    # Memory per executor
driver_memory = 2g      # Memory for driver

# Parallelism
shuffle_partitions = 200  # Number of partitions after shuffle
```

### Recommended Settings by Scale

#### Small (< 10M rows) - Local Development

```ini
[spark]
master = local[*]
executor_memory = 4g
driver_memory = 2g
shuffle_partitions = 50
```

#### Medium (10M - 100M rows) - Single Server

```ini
[spark]
master = local[*]
executor_memory = 16g
driver_memory = 8g
shuffle_partitions = 200
```

#### Large (100M - 1B rows) - Small Cluster

```bash
spark-submit \
    --master yarn \
    --deploy-mode client \
    --driver-memory 8g \
    --executor-memory 16g \
    --executor-cores 4 \
    --num-executors 20 \
    --conf spark.sql.shuffle.partitions=500 \
    run_production.py ...
```

#### Production (1B - 10B rows) - Large Cluster

```bash
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --driver-memory 16g \
    --executor-memory 32g \
    --executor-cores 4 \
    --num-executors 100 \
    --conf spark.sql.shuffle.partitions=2000 \
    --conf spark.default.parallelism=2000 \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.sql.adaptive.coalescePartitions.enabled=true \
    --conf spark.sql.adaptive.skewJoin.enabled=true \
    --conf spark.memory.fraction=0.8 \
    --conf spark.memory.storageFraction=0.3 \
    --conf spark.sql.parquet.compression.codec=zstd \
    --conf spark.sql.autoBroadcastJoinThreshold=100MB \
    --conf spark.network.timeout=600s \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    run_production.py ...
```

### Key Spark Settings Explained

| Setting | Default | Description |
|---------|---------|-------------|
| `spark.executor.memory` | 1g | Memory per executor. Set 60-80% of node RAM |
| `spark.driver.memory` | 1g | Memory for driver. Increase for large collects |
| `spark.executor.cores` | 1 | Cores per executor. Usually 4-5 |
| `spark.sql.shuffle.partitions` | 200 | Partitions after join/groupBy. Set to 2-3× total cores |
| `spark.sql.adaptive.enabled` | false | Enable Adaptive Query Execution (recommended) |
| `spark.memory.fraction` | 0.6 | Fraction of heap for execution/storage |
| `spark.serializer` | Java | Use Kryo for faster serialization |

### Adaptive Query Execution (AQE)

AQE optimizes queries at runtime. Enable with:

```bash
--conf spark.sql.adaptive.enabled=true
--conf spark.sql.adaptive.coalescePartitions.enabled=true
--conf spark.sql.adaptive.skewJoin.enabled=true
```

Benefits:
- Auto-adjusts partition count
- Handles skewed data (some cities have 100× more transactions)
- Optimizes join strategies

### Memory Tuning

```
┌─────────────────────────────────────────────────────────────────────┐
│                    EXECUTOR MEMORY LAYOUT                            │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  Total Executor Memory: 32GB                                        │
│  ├── Reserved (300MB): 300MB                                        │
│  └── Usable: 31.7GB                                                 │
│      ├── spark.memory.fraction (0.6): 19GB                          │
│      │   ├── Execution: Shuffles, joins, aggregations               │
│      │   └── Storage: Cached data, broadcast variables              │
│      └── User Memory (0.4): 12.7GB                                  │
│          └── User data structures, UDF overhead                     │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

For DP workloads:
- Set `spark.memory.fraction = 0.8` (more for computation)
- Set `spark.memory.storageFraction = 0.3` (less for caching)

### Programmatic Configuration

In Python code:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("TransactionDP") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "8g") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .getOrCreate()
```

### Environment Variables

```bash
# Java
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Spark
export SPARK_HOME=/opt/spark
export PATH=$SPARK_HOME/bin:$PATH

# Memory (alternative to config)
export SPARK_DRIVER_MEMORY=4g
export SPARK_EXECUTOR_MEMORY=8g

# Python (for PySpark)
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3
```

### Cluster-Specific Settings

#### YARN

```bash
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --queue production \
    --conf spark.yarn.maxAppAttempts=2 \
    --conf spark.yarn.executor.memoryOverhead=4g \
    ...
```

#### Kubernetes

```bash
spark-submit \
    --master k8s://https://<k8s-master>:6443 \
    --deploy-mode cluster \
    --conf spark.kubernetes.container.image=your-spark-image:latest \
    --conf spark.kubernetes.namespace=spark \
    --conf spark.kubernetes.executor.request.cores=4 \
    ...
```

#### Standalone Cluster

```bash
spark-submit \
    --master spark://master-node:7077 \
    --deploy-mode client \
    --total-executor-cores 100 \
    ...
```

---

## Function Configuration Reference

### Core Functions and Their Configs

| Function/Class | Config Source | Key Parameters |
|----------------|---------------|----------------|
| `DPPipeline` | `Config` object | All sections |
| `Budget` | `[privacy]` section | `total_rho`, `delta`, splits |
| `DistributedPreprocessor` | `Config` + `checkpoint_dir` | Spark configs |
| `CensusDASEngine` | `Budget` object | `enforce_consistency`, `use_nnls` |
| `SuppressionManager` | `[privacy]` section | `threshold`, `method` |
| `ConfidenceCalculator` | `[privacy]` section | `confidence_levels` |
| `GlobalSensitivityCalculator` | `[privacy]` section | `sensitivity_method` |
| `BoundedContributionCalculator` | `[privacy]` section | `contribution_bound_*` |

### Config Loading Order

```python
# 1. Load from INI file
config = Config.from_ini("configs/default.ini")

# 2. Override programmatically
config.privacy.total_rho = Fraction(1, 2)
config.spark.executor_memory = "16g"

# 3. Validate
config.validate()

# 4. Use in pipeline
pipeline = DPPipeline(spark, config, geography, budget)
```

### Custom Config Example

```python
from core.config import Config, PrivacyConfig, DataConfig, SparkConfig
from fractions import Fraction

# Create custom config
config = Config()

# Privacy settings
config.privacy.total_rho = Fraction(1, 4)
config.privacy.suppression_threshold = 15
config.privacy.confidence_levels = [0.90, 0.95]
config.privacy.sensitivity_method = "global"

# Data settings
config.data.input_path = "hdfs:///data/transactions.parquet"
config.data.output_path = "hdfs:///output/protected"
config.data.winsorize_percentile = 99.5

# Spark settings
config.spark.executor_memory = "32g"
config.spark.driver_memory = "16g"
config.spark.shuffle_partitions = 2000

# Validate and save
config.validate()
config.to_ini("configs/production.ini")
```

---

## Troubleshooting

### Java Not Found

```bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
```

### Out of Memory

Edit Spark memory settings in config or run with:

```bash
export SPARK_DRIVER_MEMORY=4g
export SPARK_EXECUTOR_MEMORY=4g
```

### Permission Denied

```bash
chmod +x examples/*.py tests/*.py main.py
```

## Full Command Reference

```bash
# Unit tests
python tests/test_no_spark.py

# DP correctness tests
python tests/test_dp_correctness.py

# Generate data only
python examples/generate_sample_data.py -n 1000000 -o data/transactions.csv

# Run pipeline (generate + DP)
python examples/run_pipeline.py -n 1000000 --rho 1

# Run pipeline on existing data
python examples/run_pipeline.py --run-only -i data/transactions.csv -o output/

# Quick test
python examples/quick_test.py

# Main CLI
python main.py --config configs/default.ini
```

---

## Production Scale (10B+ Records)

For production workloads with billions of records, use the distributed pipeline.

### Spark Cluster Setup

```bash
# Example: 100 executors, 32GB each = 3.2TB total memory
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --driver-memory 16g \
    --executor-memory 32g \
    --executor-cores 4 \
    --num-executors 100 \
    --conf spark.sql.shuffle.partitions=2000 \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.sql.adaptive.coalescePartitions.enabled=true \
    run_production.py \
    --input hdfs:///data/transactions \
    --output hdfs:///output/protected \
    --rho 1 \
    --checkpoint hdfs:///tmp/dp_checkpoint
```

### Local Production Test

```bash
# Test production pipeline locally (small data)
python examples/run_production.py \
    --input data/sample.parquet \
    --output output/protected \
    --rho 1 \
    --local
```

### Census DAS Mode (Full Census 2020 Compliance)

```bash
# Full Census 2020 DAS methodology:
# - Exact Discrete Gaussian
# - NNLS post-processing
# - Controlled rounding
# - Monthly invariants
python examples/run_production.py \
    --input data/sample.parquet \
    --output output/protected \
    --rho 0.25 \
    --census-das \
    --local
```

### Invariant Structure

```
INVARIANTS (EXACT - PUBLIC DATA):
  - Province-month totals (all queries)
  - These match publicly published statistics
  - No noise added (no privacy cost)

NOISY (with DP protection):
  - Cell-level values (city, mcc, day)
  - Full privacy budget allocated here
  - Adjusted via NNLS to sum to province-month invariants
```

### Production Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                    PRODUCTION PIPELINE (10B+)                        │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐           │
│  │   INPUT      │    │   SPARK      │    │   OUTPUT     │           │
│  │  (HDFS/S3)   │───▶│   CLUSTER    │───▶│  (HDFS/S3)   │           │
│  │  Parquet     │    │  Distributed │    │  Partitioned │           │
│  │  10B rows    │    │  Processing  │    │  Parquet     │           │
│  └──────────────┘    └──────────────┘    └──────────────┘           │
│                             │                                        │
│                             ▼                                        │
│                      ┌──────────────┐                               │
│                      │  KEY DESIGN  │                               │
│                      ├──────────────┤                               │
│                      │ • No collect()│                               │
│                      │ • Broadcast   │                               │
│                      │   geography   │                               │
│                      │ • Distributed │                               │
│                      │   noise       │                               │
│                      │ • Checkpoint  │                               │
│                      └──────────────┘                               │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

### Estimated Runtimes

| Data Size | Cluster Size | Expected Runtime |
|-----------|--------------|------------------|
| 1M rows   | Local (8 cores) | 1-2 minutes |
| 100M rows | 10 executors | 5-10 minutes |
| 1B rows   | 50 executors | 30-60 minutes |
| 10B rows  | 100 executors | 2-4 hours |

### Memory Requirements

```
Per Executor:
  - Base memory: 32GB
  - Overhead: 8GB (for off-heap)
  - Total: 40GB per executor

For 10B rows:
  - Recommended: 100 executors = 4TB total memory
  - Minimum: 50 executors = 2TB (may spill to disk)
```

---

## Census 2020 Compliance Options

### Suppression Configuration

Suppress cells with counts below threshold:

```bash
# Edit default.ini or pass via config
[privacy]
suppression_threshold = 10      # Hide cells with count < 10
suppression_method = flag       # Options: flag, null, value
```

Or via command line (if supported):

```bash
python examples/run_production.py \
    --input data/transactions.parquet \
    --output output/protected \
    --rho 0.25 \
    --census-das \
    --suppression-threshold 10 \
    --local
```

### Confidence Intervals

Include margin of error and confidence intervals in output:

```ini
[privacy]
confidence_levels = 0.90        # 90% CI (can be: 0.90,0.95)
include_relative_moe = true     # Include MOE as percentage
```

Output columns will include:
- `transaction_count` - Protected value
- `transaction_count_moe_90` - Margin of error (±)
- `transaction_count_ci_lower_90` - Lower bound
- `transaction_count_ci_upper_90` - Upper bound
- `transaction_count_rel_moe_90` - Relative MOE (percentage)

### Global Sensitivity

Configure how sensitivity is computed:

```ini
[privacy]
# Options: local, global, fixed
sensitivity_method = global

# For 'fixed' method only:
fixed_max_cells_per_card = 100
```

| Method | Sensitivity | When to Use |
|--------|-------------|-------------|
| `local` | K | Each card in 1 cell only (rare) |
| `global` | √M × K | Compute M from data (recommended) |
| `fixed` | √100 × K | Known upper bound on M |

### Bounded Contribution

Limit transactions per card per cell:

```ini
[privacy]
# Method: iqr (auto), percentile, fixed
contribution_bound_method = iqr

# IQR method: K = Q3 + multiplier * IQR
contribution_bound_iqr_multiplier = 1.5

# Fixed method
contribution_bound_fixed = 5

# Percentile method
contribution_bound_percentile = 99
```

---

## Reading Output with Confidence Intervals

### Python (PySpark)

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ReadDP").getOrCreate()
df = spark.read.parquet("output/protected")

# Show with confidence intervals
df.select(
    "province_name",
    "acceptor_city", 
    "mcc",
    "transaction_count",
    "transaction_count_moe_90",
    "transaction_count_ci_lower_90",
    "transaction_count_ci_upper_90",
    "is_suppressed"
).show()

# Filter out suppressed cells
df_public = df.filter(df.is_suppressed == False)
df_public.show()

# Aggregate with uncertainty
from pyspark.sql import functions as F

province_totals = df_public.groupBy("province_name").agg(
    F.sum("transaction_count").alias("total_count"),
    # MOE of sum = sqrt(sum of MOE^2)
    F.sqrt(F.sum(F.pow("transaction_count_moe_90", 2))).alias("total_moe")
)
province_totals.show()
```

### Python (Pandas)

```python
import pandas as pd

# Read Parquet
df = pd.read_parquet("output/protected")

# Filter suppressed
df_public = df[df["is_suppressed"] == False]

# Show sample
print(df_public[[
    "province_name", 
    "acceptor_city",
    "transaction_count",
    "transaction_count_moe_90"
]].head(10))

# Plot with error bars
import matplotlib.pyplot as plt

city_totals = df_public.groupby("acceptor_city").agg({
    "transaction_count": "sum",
    "transaction_count_moe_90": lambda x: (x**2).sum()**0.5
}).head(20)

plt.figure(figsize=(12, 6))
plt.bar(city_totals.index, city_totals["transaction_count"], 
        yerr=city_totals["transaction_count_moe_90"], capsize=3)
plt.xticks(rotation=45, ha="right")
plt.ylabel("Transaction Count")
plt.title("Transaction Counts by City (with 90% CI)")
plt.tight_layout()
plt.savefig("city_counts.png")
```

---

## Complete Configuration Reference

### default.ini (Full Example)

```ini
[privacy]
# Privacy budget (monthly)
total_rho = 1/4
delta = 1e-10

# Geographic allocation (NOTE: Province-month are public invariants)
# All budget goes to cell level (city, mcc, day)
# Geographic split is not used - full budget to cells
geographic_split_province = 0.0
geographic_split_city = 1.0

# Query allocation (must sum to 1.0)
query_split_transaction_count = 0.25
query_split_unique_cards = 0.25
query_split_unique_acceptors = 0.25
query_split_total_amount = 0.25

# Bounded Contribution
contribution_bound_method = iqr
contribution_bound_iqr_multiplier = 1.5
contribution_bound_fixed = 5
contribution_bound_percentile = 99

# Suppression
suppression_threshold = 10
suppression_method = flag
suppression_sentinel = -1

# Confidence Intervals
confidence_levels = 0.90
include_relative_moe = true

# Global Sensitivity
sensitivity_method = global
fixed_max_cells_per_card = 100

[data]
input_path = data/transactions.parquet
output_path = output/protected/
city_province_path = data/city_province.csv
input_format = parquet
winsorize_percentile = 99.0
date_column = transaction_date
date_format = %Y-%m-%d
num_days = 30

[spark]
app_name = TransactionDP
master = local[*]
executor_memory = 4g
driver_memory = 2g
shuffle_partitions = 200

[columns]
transaction_id = transaction_id
amount = amount
transaction_date = transaction_date
card_number = card_number
acceptor_id = acceptor_id
acceptor_city = acceptor_city
mcc = mcc
```

---

## Validation Checklist

Before deploying to production, verify:

### Unit Tests
```bash
python tests/test_no_spark.py
# Expected: All tests pass
```

### DP Correctness Tests
```bash
python tests/test_dp_correctness.py
# Expected: All 25 tests pass
```

### Sample Run
```bash
python examples/run_pipeline.py --num-records 10000 --rho 0.25
# Expected: Output in output/dp_protected/
```

### Output Verification
```python
# Check output has expected columns
df = spark.read.parquet("output/dp_protected/protected_data")
assert "transaction_count" in df.columns
assert "transaction_count_moe_90" in df.columns
assert "is_suppressed" in df.columns

# Check invariants (province sums should match)
province_sums = df.groupBy("province_name").agg(F.sum("transaction_count"))
# Compare with true province totals (from invariant manager)
```

---

## Troubleshooting New Features

### Suppression Issues

**Problem**: Too many cells suppressed
```
Solution: Lower suppression_threshold or increase data
```

**Problem**: Suppressed cells still visible
```
Solution: Use suppression_method = null instead of flag
```

### Confidence Interval Issues

**Problem**: CI columns not in output
```
Solution: Ensure confidence_levels is set in config
```

**Problem**: Very wide confidence intervals
```
Solution: Increase rho (more privacy budget) or aggregate to higher level
```

### Sensitivity Issues

**Problem**: Noise is too high
```
Cause: Global sensitivity √M × K is large
Solution: 
  1. Use sensitivity_method = fixed with reasonable M
  2. Tighten bounded contribution (lower K)
  3. Increase privacy budget (higher rho)
```

**Problem**: Privacy guarantee incorrect
```
Cause: Using local sensitivity when cards span multiple cells
Solution: Always use sensitivity_method = global for transaction data
```

