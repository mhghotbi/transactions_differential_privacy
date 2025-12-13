# Transaction Privacy System

A production-ready privacy-preserving system for financial transaction data with **two implementation approaches**:

1. **Differential Privacy (DP)** - Formal privacy guarantees using US Census 2020 DAS methodology
2. **Statistical Disclosure Control (SDC)** - Utility-first approach for secure enclave deployment

## Overview

This system adds mathematically calibrated noise to transaction statistics, enabling data sharing while protecting individual privacy.

### Differential Privacy Implementation (Formal DP)

```
Raw Transactions  →  Aggregate by (City, MCC, Day)  →  Add DP Noise  →  NNLS + Rounding  →  Protected Output
   10B rows              ~10M cells                     Discrete          (match public)      Parquet + CI
                                                          Gaussian          province totals
                                                          (cell level)
```

**Use when:** You need formal privacy guarantees (ε, δ)-DP with provable protection against inference attacks.

### Statistical Disclosure Control Implementation (Utility-First SDC)

```
Raw Transactions  →  Aggregate by (City, MCC, Day)  →  Add SDC Noise  →  Ratio Preserve  →  Protected Output
   10B rows              ~10M cells                     Multiplicative    (plausibility)      Parquet + CI
                                                          Jitter           bounds
                                                          (context-aware)
```

**Use when:** You're deploying in a secure enclave with physical isolation and prioritize utility over formal privacy proofs.

## Key Features

### Differential Privacy Features

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

### Statistical Disclosure Control Features

| Feature | Description |
|---------|-------------|
| **Multiplicative Jitter** | Preserves ratios naturally (amount/count, count/cards) |
| **Context-Aware Bounds** | Data-driven plausibility ranges per (MCC, City, Weekday) |
| **Province Invariants** | Exact province totals (no noise at province level) |
| **Ratio Preservation** | Maintains realistic relationships between queries |
| **Bounded Contribution** | Transaction-weighted percentile (minimizes data loss) |
| **Cell Suppression** | Hide small counts |

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

### Quick Examples

#### Using Statistical Disclosure Control (SDC) - Default

```bash
# Generate sample data and run SDC pipeline (utility-first)
python examples/run_pipeline.py --num-records 100000
```

This uses `TopDownSparkEngine` with multiplicative jitter and plausibility bounds.

#### Using Differential Privacy (DP) - Formal Privacy

```bash
# Run DP pipeline with formal privacy guarantees
python examples/run_production.py \
    --input data/transactions.parquet \
    --output output/protected \
    --rho 0.25 \
    --census-das
```

This uses `CensusDASEngine` with Discrete Gaussian mechanism matching Census 2020 DAS.

### Production (10B+ rows)

#### DP Implementation (Formal Privacy)

```bash
spark-submit \
    --master yarn \
    --num-executors 100 \
    --executor-memory 32g \
    examples/run_production.py \
    --input hdfs:///data/transactions \
    --output hdfs:///output/protected \
    --rho 0.25 \
    --census-das
```

#### SDC Implementation (Utility-First)

```bash
# Uses same pipeline but with SDC engine (configured via config.ini)
python examples/run_pipeline.py \
    --input hdfs:///data/transactions \
    --output hdfs:///output/protected
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

### Differential Privacy Configuration

Edit `configs/default.ini` for DP implementation:

```ini
[privacy]
# Monthly privacy budget (for DP implementation)
total_rho = 1/4                    # ρ = 0.25 → ε ≈ 5 per month

# Bounded contribution (IMPORTANT: affects data loss)
# RECOMMENDED: transaction_weighted_percentile minimizes data loss compared to simple percentile
contribution_bound_method = transaction_weighted_percentile  # Keep 99% of transactions
contribution_bound_percentile = 99.0                        # Target retention percentage

# Suppression
suppression_threshold = 5          # Hide cells with count < 5

# Confidence intervals
confidence_levels = 0.90           # 90% CI

# Sensitivity
sensitivity_method = global        # Account for multi-cell cards
```

### Statistical Disclosure Control Configuration

For SDC implementation, configure noise levels:

```ini
[privacy]
# SDC noise levels (utility-first)
noise_level = 0.15          # 15% relative noise for counts
cards_jitter = 0.05         # 5% jitter for unique_cards
amount_jitter = 0.05        # 5% jitter for total_amount

# Bounded contribution
contribution_bound_method = transaction_weighted_percentile
contribution_bound_percentile = 99.0

# Suppression
suppression_threshold = 5
```

## Privacy Guarantees

### Differential Privacy (DP Implementation)

| Time Period | zCDP (ρ) | (ε, δ)-DP |
|-------------|----------|-----------|
| Monthly | 0.25 | ε ≈ 5, δ = 10⁻¹⁰ |
| Annual | 3.0 | ε ≈ 20, δ = 10⁻¹⁰ |

**Formal Guarantee:** Provides provable (ε, δ)-differential privacy protection against inference attacks.

### Statistical Disclosure Control (SDC Implementation)

**Protection Layers:**
1. Physical isolation (secure enclave) - primary protection
2. Context-aware plausibility bounds - prevents obvious outliers
3. Multiplicative jitter - adds realistic variation
4. Suppression - hides small cells

**Utility Guarantee:** For multiplicative jitter with `noise_level = 0.15`:
- Expected relative error: ~15%
- 95th percentile error: < 29% with 95% probability
- Province totals: EXACT (0% error)

**Note:** SDC does not provide formal privacy guarantees but prioritizes utility in secure enclave deployments.

## Documentation

| Document | Description |
|----------|-------------|
| [TUTORIAL.md](TUTORIAL.md) | Step-by-step usage guide |
| [CODE_EXPLANATION.md](CODE_EXPLANATION.md) | Technical deep-dive |
| [PRIVACY_PROOF.md](PRIVACY_PROOF.md) | Formal privacy analysis |

## Implementation Comparison

| Aspect | DP Implementation | SDC Implementation |
|--------|------------------|-------------------|
| **Engine** | `CensusDASEngine` | `TopDownSparkEngine` |
| **Noise Mechanism** | Discrete Gaussian | Multiplicative jitter |
| **Privacy Framework** | zCDP | Context-aware bounds |
| **Privacy Guarantee** | Formal (ε, δ)-DP | Utility-first (no formal proof) |
| **Use Case** | Public release, formal guarantees | Secure enclave, utility priority |
| **Entry Point** | `examples/run_production.py` | `examples/run_pipeline.py` |
| **Post-Processing** | NNLS optimization | Ratio-preserving rounding |
| **Invariants** | Province-month totals (exact) | Province totals (exact) |

## Project Structure

```
transactions_differential_privacy/
├── main.py                 # CLI entry point
├── demo_notebook.ipynb     # Interactive demo notebook
├── requirements.txt        # Python dependencies
├── README.md               # This file
├── CODE_EXPLANATION.md     # Technical deep-dive (SDC implementation)
├── configs/
│   └── default.ini         # Default configuration
├── core/
│   ├── config.py           # Configuration management
│   ├── budget.py           # Privacy budget allocation (DP)
│   ├── primitives.py       # Discrete Gaussian mechanism (DP)
│   ├── pipeline.py         # SDC pipeline orchestration
│   ├── postprocessing.py   # NNLS optimization (DP)
│   ├── rounder.py          # Controlled rounding
│   ├── invariants.py       # Exact totals management
│   ├── suppression.py      # Cell suppression
│   ├── confidence.py       # Confidence intervals
│   ├── sensitivity.py      # Global sensitivity (DP)
│   ├── plausibility_bounds.py  # SDC plausibility bounds
│   └── bounded_contribution.py # Bounded contribution (both)
├── data/
│   └── city_province.csv   # City-Province mapping
├── schema/
│   ├── geography.py        # Province/City hierarchy
│   └── histogram.py        # Multi-dimensional structure
├── reader/
│   ├── spark_reader.py     # Data reading
│   ├── preprocessor.py     # Basic preprocessing
│   └── preprocessor_distributed.py  # Production scale (DP)
├── engine/
│   └── topdown_spark.py    # SDC engine (TopDownSparkEngine)
├── queries/
│   └── transaction_queries.py  # Query definitions
├── writer/
│   └── parquet_writer.py   # Output writing
├── examples/
│   ├── generate_sample_data.py
│   ├── run_pipeline.py     # SDC pipeline (default)
│   ├── run_production.py  # DP pipeline (formal privacy)
│   └── quick_test.py
├── tests/
│   ├── test_no_spark.py    # Unit tests
│   └── test_dp_correctness.py  # DP verification
└── scripts/                # Helper scripts
```

## Comparison with US Census 2020

### DP Implementation

| Aspect | Census 2020 | DP Implementation |
|--------|-------------|-------------------|
| Mechanism | Discrete Gaussian | Discrete Gaussian ✓ |
| Framework | zCDP | zCDP ✓ |
| NNLS | Yes | Yes ✓ |
| Rounding | Yes | Yes ✓ |
| Invariants | Population totals | Province-month totals (public) ✓ |
| Suppression | Yes | Yes ✓ |
| Geography | 6 levels | 2 levels |
| Privacy Unit | Person | Card-Month |

### SDC Implementation

| Aspect | Census 2020 | SDC Implementation |
|--------|-------------|-------------------|
| Approach | Formal DP (zCDP) | SDC (utility-first) |
| Mechanism | Discrete Gaussian | Multiplicative jitter |
| Framework | zCDP with budget | Context-aware bounds |
| Post-Processing | NNLS optimization | Ratio-preserving rounding ✓ |
| Invariants | Population totals | Province totals (exact) ✓ |
| Suppression | Yes | Yes ✓ |
| Geography | 6 levels | 2 levels |
| Privacy Unit | Person | Card-Month |

## References

1. Abowd et al. (2022). "The 2020 Census Disclosure Avoidance System TopDown Algorithm"
2. Canonne et al. (2020). "The Discrete Gaussian for Differential Privacy"
3. Bun & Steinke (2016). "Concentrated Differential Privacy"

## License

Internal use only. Contact data governance for external distribution.

