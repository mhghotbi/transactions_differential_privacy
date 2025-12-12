# Transaction SDC Examples

This folder contains example scripts for testing the Transaction SDC System (Statistical Disclosure Control).

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

Run the complete pipeline including data generation and SDC application:

```bash
# Full run (generate data + apply SDC)
python examples/run_pipeline.py

# Only generate data
python examples/run_pipeline.py --generate-only

# Only run SDC on existing data
python examples/run_pipeline.py --run-only --input data/my_data.csv

# Quick test with fewer records
python examples/run_pipeline.py --num-records 100000
```

### 4. Production Pipeline

For production scale with SDC protection:

```bash
# Full SDC pipeline (context-aware plausibility-based noise)
python examples/run_production.py \
    --input data/transactions.parquet \
    --output output/protected \
    --local

# With custom noise level (default: 0.15 = 15%)
python examples/run_production.py \
    --input data/transactions.parquet \
    --output output/protected \
    --noise-level 0.20 \
    --local
```

### SDC Configuration

Default noise settings:
- Count noise: 15% relative (multiplicative jitter)
- Cards jitter: 5% (for derived unique_cards)
- Amount jitter: 5% (for derived total_amount)

### Invariant Structure

```
EXACT (no noise added):
  - Province-level transaction counts (invariant)
  - Province-level totals match public statistics exactly

PROTECTED (with context-aware noise):
  - City-level counts: multiplicative jitter with plausibility bounds
  - Ratios preserved: avg_amount and tx_per_card stay within plausible ranges
  - Controlled rounding: maintains province invariants exactly
```

## Output

After successful execution, output includes:

1. **`output/sdc_protected/protected_data/`** - Parquet files with protected data
2. **`output/sdc_protected/metadata.json`** - Execution metadata including SDC parameters

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
║          Transaction SDC System - Example Runner            ║
╚════════════════════════════════════════════════════════════╝

Step 1: Generating sample transaction data
  Loaded 480 unique cities
  Generating 1,000,000 transactions...
  Done!

Step 2: Running SDC Pipeline
  Initializing Spark session...
  Loading geography...
  Applying Statistical Disclosure Control...
  Writing protected data...

Results
  Success:          True
  Total Records:    1,000,000
  Noise Level:      15% relative
  Duration:         45.23 seconds
```
