# Transaction DP Examples

This folder contains example scripts for testing the Transaction DP System.

## Prerequisites

```bash
pip install -r requirements.txt
```

Also, the `city_province.csv` file must be present in the `data/` directory.

## Available Scripts

### 1. Quick Test

Quick test with 10K records to verify the system works correctly:

```bash
python examples/quick_test.py
```

### 2. Generate Sample Data

Generate CSV with synthetic transaction data:

```bash
# Generate 1 million records (default)
python examples/generate_sample_data.py

# Generate 500K records to custom path
python examples/generate_sample_data.py --num-records 500000 --output my_data.csv

# All options
python examples/generate_sample_data.py \
    --num-records 1000000 \
    --output data/sample_transactions.csv \
    --city-province data/city_province.csv \
    --num-days 30 \
    --num-cards 50000 \
    --num-acceptors 10000 \
    --seed 42
```

### 3. Run Pipeline

Run the complete pipeline including data generation and DP application:

```bash
# Full run (generate data + apply DP)
python examples/run_pipeline.py

# Only generate data
python examples/run_pipeline.py --generate-only

# Only run DP on existing data
python examples/run_pipeline.py --run-only --input data/my_data.csv

# With different privacy budget
python examples/run_pipeline.py --rho 1/2

# Quick test with fewer records
python examples/run_pipeline.py --num-records 100000
```

### 4. Production Pipeline (Census DAS Style)

For production scale with Census 2020-compliant DP:

```bash
# Full Census DAS methodology (exact Discrete Gaussian + NNLS + invariants)
python examples/run_production.py \
    --input data/transactions.parquet \
    --output output/protected \
    --rho 0.25 \
    --census-das \
    --local

# Exact mechanism only (no NNLS post-processing)
python examples/run_production.py \
    --input data/transactions.parquet \
    --output output/protected \
    --rho 0.25 \
    --exact \
    --local

# Fast approximate (for testing)
python examples/run_production.py \
    --input data/transactions.parquet \
    --output output/protected \
    --rho 0.25 \
    --approximate \
    --local
```

### Privacy Budget Explanation

Default: `rho = 0.25` per month

| Level | Budget | Sigma | Typical Noise |
|-------|--------|-------|---------------|
| Province | 0.0125 per query | 6.32 | ±12 |
| City | 0.05 per query | 3.16 | ±6 |

Annual privacy (12 monthly releases): `rho = 3`, `epsilon ≈ 20`

### Invariant Structure

```
EXACT (no noise added):
  - National monthly totals
  - Province monthly totals

NOISY (with post-processing):
  - City daily values → adjusted to sum to province daily
  - Province daily values → adjusted to sum to monthly invariant
```

## Output

After successful execution, output includes:

1. **`output/dp_protected/protected_data/`** - Parquet files with protected data
2. **`output/dp_protected/metadata.json`** - Execution metadata including privacy parameters

## Sample Data Structure

Generated data includes these columns:

| Column | Type | Description |
|--------|------|-------------|
| transaction_id | string | Unique transaction identifier |
| amount | float | Transaction amount (Rials) |
| transaction_date | date | Transaction date |
| card_number | string | 16-digit card number |
| acceptor_id | string | Acceptor/merchant identifier |
| acceptor_city | string | Acceptor city |
| mcc | string | Merchant Category Code |

## Sample Output

```
╔════════════════════════════════════════════════════════════╗
║          Transaction DP System - Example Runner            ║
╚════════════════════════════════════════════════════════════╝

Step 1: Generating sample transaction data
  Loaded 480 unique cities
  Generating 1,000,000 transactions...
  Done!

Step 2: Running DP Pipeline
  Initializing Spark session...
  Loading geography...
  Applying differential privacy...
  Writing protected data...

Results
  Success:          True
  Total Records:    1,000,000
  Budget Used:      rho = 1
  Duration:         45.23 seconds
```
