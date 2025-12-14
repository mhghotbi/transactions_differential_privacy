# Glossary

Technical terms and definitions used in the Transaction SDC System.

---

## A

### avg_amount
Average transaction amount per cell: `total_amount / transaction_count`. Must remain plausible after noise application.

---

## B

### Bounded Contribution (K)
Maximum number of transactions allowed per card per cell (city, mcc, day). Prevents outliers from dominating statistics. Computed using transaction-weighted percentile method (recommended).

### Broadcast Variable
Spark optimization for small lookup tables. Broadcasts small data to all executors to avoid shuffle during joins.

---

## C

### Cell
A single entry in the histogram: `(province, city, mcc, day, weekday)`. Represents aggregated statistics for that combination.

### Context-Aware Bounds
Plausibility bounds computed per (MCC, City, Weekday) stratum. Different contexts have different plausible ranges (e.g., small city + Restaurant MCC ≠ large city + Gas MCC).

### Controlled Rounding
Rounding noisy values to integers while maintaining exact totals (province invariants).

---

## D

### D_max
Maximum number of distinct cells any card appears in. Used in formal DP (not active in SDC). Legacy term.

### Discrete Gaussian Mechanism
Formal DP mechanism that adds integer noise from discrete Gaussian distribution. Legacy code (not active in SDC).

---

## E

### ε (epsilon)
Privacy parameter in (ε, δ)-DP. Measures privacy loss. Legacy term (not used in SDC).

---

## F

### Formal DP
Differential Privacy with mathematical privacy guarantees (ε, δ). Not currently active - system uses SDC instead.

---

## G

### Global Sensitivity
Sensitivity calculation accounting for multi-cell contributions. Used in formal DP (not active in SDC). Legacy term.

---

## H

### Histogram
Multi-dimensional aggregated data structure. Cells indexed by (province, city, mcc, day, weekday).

---

## I

### Invariant
Exact value that must be preserved. In this system: province totals are invariants (no noise applied).

### IQR Method
Interquartile range method for computing bounded contribution (K). Alternative to transaction-weighted percentile.

---

## J

### Jitter
Noise added to values. In SDC: multiplicative jitter `M(c) = c × (1 + η)`.

---

## K

### K (Bounded Contribution)
Maximum transactions per card per cell. Prevents outliers from dominating statistics. Computed using transaction-weighted percentile (recommended).

---

## M

### MCC (Merchant Category Code)
Code categorizing merchant type (e.g., Restaurant, Gas Station, Grocery). Used for context-aware bounds and per-MCC winsorization.

### Multiplicative Jitter
Noise mechanism: `M(c) = c × (1 + η)` where `η ~ N(0, σ²)`. Preserves ratios naturally.

---

## N

### Noise Level
Relative noise level (e.g., 0.15 = 15%). Standard deviation of noise is `noise_level × value`.

### NNLS (Non-Negative Least Squares)
Optimization method for post-processing. Legacy code (not active in SDC).

---

## P

### Parquet
Columnar storage format. Used for all large datasets (not CSV).

### Plausibility Bounds
Range of plausible values for a cell. Computed per (MCC, City, Weekday) context. Noise is clamped to these bounds.

### Province Invariant
Exact province total that must be preserved. No noise applied at province level. City-level values adjusted to sum to exact province totals.

---

## R

### Ratio Preservation
Maintaining plausible ratios after noise application:
- `avg_amount = total_amount / transaction_count`
- `tx_per_card = transaction_count / unique_cards`

### Relative Noise
Noise proportional to value magnitude. Standard deviation `σ = noise_level × value`.

### ρ (rho)
Privacy parameter in zCDP (zero-Concentrated DP). Legacy term (not used in SDC).

---

## S

### SDC (Statistical Disclosure Control)
Utility-first privacy protection approach. Used in this system (not formal DP). Designed for secure enclave deployment.

### Secure Enclave
Physically isolated computing environment. Provides primary protection. SDC is secondary protection layer.

### Stratum
Grouping dimension for context-aware bounds: (MCC, City, Weekday). Each stratum has its own plausibility bounds.

### Suppression
Hiding small cells below threshold. Prevents reconstruction attacks on small counts.

---

## T

### Transaction-Weighted Percentile
Method for computing bounded contribution (K). Minimizes data loss by weighting by transaction volume, not just card count. Recommended method.

### tx_per_card
Transactions per card: `transaction_count / unique_cards`. Must remain plausible after noise application.

---

## U

### Unique Cards
Count of distinct cards in a cell. Protected statistic (noise applied).

### User-Level DP
Formal DP protecting entire card history (not individual transactions). Legacy term (not active in SDC).

### Utility-First
SDC approach prioritizing statistical utility over formal privacy guarantees. Appropriate for secure enclave deployment.

---

## W

### Weekday
Day of week (0=Monday, 6=Sunday). Used for context-aware bounds.

### Winsorization
Capping outliers at percentile threshold. In this system: per-MCC winsorization at 99th percentile.

---

## Z

### zCDP (zero-Concentrated DP)
Privacy framework using ρ parameter. Legacy term (not used in SDC).

---

## Common Abbreviations

- **DP**: Differential Privacy (legacy, not active)
- **SDC**: Statistical Disclosure Control (current approach)
- **MCC**: Merchant Category Code
- **IQR**: Interquartile Range
- **NNLS**: Non-Negative Least Squares (legacy)
- **OOM**: Out of Memory

---

## Related Concepts

### Secure Enclave Context
- **Primary Protection**: Physical isolation
- **Secondary Protection**: SDC plausibility-based noise
- **Deployment**: All processing within secure enclave
- **Output**: Only protected statistics released

### SDC vs Formal DP
- **SDC**: Utility-first, plausibility-based, no formal guarantees
- **Formal DP**: Mathematical guarantees (ε, δ), privacy budget tracking
- **Current System**: SDC (formal DP code kept for future use)

