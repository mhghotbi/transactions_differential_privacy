# Transaction DP System

A production-ready Differential Privacy system for financial transaction data, implementing **US Census 2020 DAS methodology**.

## Overview

This system adds mathematically calibrated noise to transaction statistics, enabling data sharing while protecting individual privacy.

```
Raw Transactions  →  Aggregate by (City, MCC, Day)  →  Add DP Noise  →  NNLS + Rounding  →  Protected Output
   10B rows              ~10M cells                     Gaussian          (match public)      Parquet + CI
                                                          (cell level)      province totals
```

## Key Features

| Feature | Description |
|---------|-------------|
| **Discrete Gaussian** | Exact integer noise (Census 2020 algorithm) |
| **zCDP Composition** | Tight privacy accounting |
| **NNLS Post-Processing** | Geographic consistency |
| **Controlled Rounding** | Integer outputs |
| **Province-Month Invariants** | Exact province-month totals (public data) |
| **Cell Suppression** | Hide small counts |
| **Confidence Intervals** | Quantified uncertainty |
| **Global Sensitivity** | Correct for multi-cell cards |
| **Bounded Contribution** | Transaction-weighted percentile (minimizes data loss) |

## Quick Start

### Prerequisites

- Python 3.8+
- Java 8 or 11
- 8GB+ RAM

### Installation

```bash
# Clone repository
cd /path/to/census_dp

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### Run Tests

```bash
# Unit tests (no Spark required)
python tests/test_no_spark.py

# DP correctness tests
python tests/test_dp_correctness.py
```

### Quick Example

```bash
# Generate sample data and run DP pipeline
python examples/run_pipeline.py --num-records 100000 --rho 0.25
```

### Production (10B+ rows)

```bash
spark-submit \
    --master yarn \
    --num-executors 100 \
    --executor-memory 32g \
    run_production.py \
    --input hdfs:///data/transactions \
    --output hdfs:///output/protected \
    --rho 0.25 \
    --census-das
```

## Output Format

```
output/protected/
├── province_code=1/
│   ├── city_code=101/
│   │   └── part-00000.parquet
│   └── city_code=102/
│       └── part-00000.parquet
└── province_code=2/
    └── ...
```

Each row contains:

| Column | Description |
|--------|-------------|
| `province_code` | Province code (integer) |
| `city_code` | City code (integer) |
| `mcc` | Merchant Category Code |
| `day_idx` | Day index (0-29) |
| `transaction_date` | Transaction date string |
| `transaction_count` | Protected count |
| `unique_cards` | Protected unique card count |
| `transaction_amount_sum` | Protected total amount |
| `is_suppressed` | True if count < threshold (if suppression enabled) |

## Configuration

Edit `configs/default.ini`:

```ini
[privacy]
# Monthly privacy budget
total_rho = 1/4                    # ρ = 0.25 → ε ≈ 5 per month

# Bounded contribution (IMPORTANT: affects data loss)
contribution_bound_method = transaction_weighted_percentile  # Keep 99% of transactions
contribution_bound_percentile = 99.0                        # Target retention percentage

# Suppression
suppression_threshold = 5          # Hide cells with count < 5

# Confidence intervals
confidence_levels = 0.90           # 90% CI

# Sensitivity
sensitivity_method = global        # Account for multi-cell cards
```

## Privacy Guarantees

| Time Period | zCDP (ρ) | (ε, δ)-DP |
|-------------|----------|-----------|
| Monthly | 0.25 | ε ≈ 5, δ = 10⁻¹⁰ |
| Annual | 3.0 | ε ≈ 20, δ = 10⁻¹⁰ |

## Documentation

| Document | Description |
|----------|-------------|
| [TUTORIAL.md](TUTORIAL.md) | Step-by-step usage guide |
| [CODE_EXPLANATION.md](CODE_EXPLANATION.md) | Technical deep-dive |
| [PRIVACY_PROOF.md](PRIVACY_PROOF.md) | Formal privacy analysis |

## Project Structure

```
transactions_differential_privacy/
├── main.py                 # CLI entry point
├── demo_notebook.ipynb     # Interactive demo notebook
├── requirements.txt        # Python dependencies
├── README.md               # This file
├── configs/
│   └── default.ini         # Default configuration
├── core/
│   ├── config.py           # Configuration management
│   ├── budget.py           # Privacy budget allocation
│   ├── primitives.py       # Discrete Gaussian mechanism
│   ├── pipeline.py         # DP pipeline orchestration
│   ├── postprocessing.py   # NNLS optimization
│   ├── rounder.py          # Controlled rounding
│   ├── invariants.py       # Exact totals management
│   ├── suppression.py      # Cell suppression
│   ├── confidence.py       # Confidence intervals
│   └── sensitivity.py      # Global sensitivity
├── data/
│   └── city_province.csv   # City-Province mapping
├── schema/
│   ├── geography.py        # Province/City hierarchy
│   └── histogram.py        # Multi-dimensional structure
├── reader/
│   ├── spark_reader.py     # Data reading
│   ├── preprocessor.py     # Basic preprocessing
│   └── preprocessor_distributed.py  # Production scale
├── engine/
│   └── topdown.py          # Top-down DP engine
├── queries/
│   └── transaction_queries.py  # Query definitions
├── writer/
│   └── parquet_writer.py   # Output writing
├── examples/
│   ├── generate_sample_data.py
│   ├── run_pipeline.py
│   ├── run_production.py
│   └── quick_test.py
├── tests/
│   ├── test_no_spark.py    # Unit tests
│   └── test_dp_correctness.py  # DP verification
└── scripts/                # Helper scripts
```

## Comparison with US Census 2020

| Aspect | Census 2020 | This System |
|--------|-------------|-------------|
| Mechanism | Discrete Gaussian | Discrete Gaussian ✓ |
| Framework | zCDP | zCDP ✓ |
| NNLS | Yes | Yes ✓ |
| Rounding | Yes | Yes ✓ |
| Invariants | Population totals | Province-month totals (public) ✓ |
| Suppression | Yes | Yes ✓ |
| Geography | 6 levels | 2 levels |
| Privacy Unit | Person | Card-Month |

## References

1. Abowd et al. (2022). "The 2020 Census Disclosure Avoidance System TopDown Algorithm"
2. Canonne et al. (2020). "The Discrete Gaussian for Differential Privacy"
3. Bun & Steinke (2016). "Concentrated Differential Privacy"

## License

Internal use only. Contact data governance for external distribution.

