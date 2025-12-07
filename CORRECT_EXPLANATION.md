# Correct Explanation: How Budget Actually Works

## The Real Answer

**This is NOT parallel composition across cells.**
**This is ONE vector query with L2 sensitivity that accounts for multi-cell contributions.**

## What Actually Happens

### The Query
You're running **ONE query** that outputs a **vector** of all cell values:

```
f(D) = [cell_1_value, cell_2_value, ..., cell_1,000,000_value]
```

This is **one mechanism** that outputs **many values**.

### The Sensitivity

Looking at `core/sensitivity.py` line 355:
```python
sensitivity = math.sqrt(self._max_cells_per_card) * k_bound
```

**L2 Sensitivity = sqrt(D_max) × K**

Where:
- D_max = maximum number of cells any card appears in (e.g., 100)
- K = maximum contribution per cell (e.g., 5)

**Example**: If a card can appear in 100 cells with max 5 transactions per cell:
- L2 Sensitivity = sqrt(100) × 5 = 10 × 5 = 50

### Why This Sensitivity?

When you remove one card:
- It can affect up to D_max cells
- Each cell changes by at most K
- L2 norm of the change = sqrt(D_max × K²) = sqrt(D_max) × K

### The Privacy Cost

```python
# ONE mechanism adding noise to ALL cells
noisy_values = add_discrete_gaussian_noise(
    all_cell_values,  # Vector of 1,000,000 values
    rho=0.083,        # Budget for this ONE query
    sensitivity=50     # L2 sensitivity of the ENTIRE vector
)
```

**Privacy Cost**: 0.083 (NOT 1,000,000 × 0.083)

**Why?** Because this is **ONE mechanism** with **ONE sensitivity** (50), not 1,000,000 separate mechanisms.

## The Key Insight

**The sensitivity already accounts for the fact that one card affects multiple cells.**

- ❌ NOT: "Each cell is a separate query with sensitivity 1, so 1M cells = 1M × sensitivity"
- ✅ YES: "All cells together form ONE vector query with sensitivity sqrt(D_max) × K"

## Analogy

Think of it like measuring the height of 1,000,000 people:

- ❌ NOT: Measure person 1 (costs 1 unit), measure person 2 (costs 1 unit), ... = 1,000,000 units
- ✅ YES: Measure all people at once with one mechanism (costs 1 unit), but the sensitivity is higher because one person's height affects the entire vector

## Total Privacy Cost

```
Query 1 (transaction_count):  
  - ONE vector query on all cells
  - L2 sensitivity = sqrt(D_max) × K
  - Budget: ρ/3
  - Cost: ρ/3

Query 2 (unique_cards):
  - ONE vector query on all cells  
  - L2 sensitivity = sqrt(D_max) × 1
  - Budget: ρ/3
  - Cost: ρ/3

Query 3 (total_amount):
  - ONE vector query on all cells
  - L2 sensitivity = sqrt(D_max) × cap
  - Budget: ρ/3
  - Cost: ρ/3

─────────────────────────────────────
Total: ρ/3 + ρ/3 + ρ/3 = ρ
```

## Summary

1. **One vector query per query type** (not one query per cell)
2. **Sensitivity accounts for multi-cell contributions** (sqrt(D_max) × K)
3. **Each query type costs ρ/3** (not number_of_cells × ρ/3)
4. **Total cost = ρ** (sum across 3 query types)

The confusion was thinking each cell gets its own mechanism. Actually, all cells together form ONE vector query with ONE sensitivity.

