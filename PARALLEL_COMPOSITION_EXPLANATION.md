# Parallel Composition Explained with Concrete Example

## The Confusion

**Question**: If we have 1,000,000 cells and we add noise to all of them, how can the total privacy cost be only ρ/3? Shouldn't it be 1,000,000 × ρ/3?

**Answer**: No! Because of **Parallel Composition**.

## What is Parallel Composition?

**Rule**: If you query **disjoint** (non-overlapping) datasets with the same budget ρ, the total privacy cost = **max(ρ, ρ, ..., ρ) = ρ**, NOT sum.

## Concrete Example

### Scenario
- You have 3 cells:
  - Cell A: (Province 1, City 1, MCC 5411, Day 1) = 100 transactions
  - Cell B: (Province 2, City 5, MCC 5411, Day 1) = 50 transactions  
  - Cell C: (Province 1, City 2, MCC 5812, Day 2) = 75 transactions

- Budget: ρ = 0.25 total
- Query split: transaction_count gets ρ/3 = 0.083

### What Happens in Code

```python
# Step 1: Get budget for transaction_count query
rho = 0.083  # This is the budget for ALL cells of this query type

# Step 2: Add noise to ALL cells using the SAME rho
for cell in [Cell A, Cell B, Cell C]:
    noisy_value = add_noise(cell.value, rho=0.083)
```

**Key Point**: All 3 cells use the **same** budget ρ = 0.083

### Why Total Cost is 0.083, Not 3 × 0.083

**Parallel Composition Theorem**:
- Cell A queries: transactions in Cell A (disjoint dataset)
- Cell B queries: transactions in Cell B (disjoint dataset)
- Cell C queries: transactions in Cell C (disjoint dataset)

Since these datasets are **disjoint** (a transaction can only be in one cell):
- Total privacy cost = **max(0.083, 0.083, 0.083) = 0.083**
- NOT: 0.083 + 0.083 + 0.083 = 0.249

### Why Are Cells Disjoint?

A transaction can only be in **one** cell:
- Transaction at (Province 1, City 1, MCC 5411, Day 1) → Cell A
- Transaction at (Province 2, City 5, MCC 5411, Day 1) → Cell B
- These are **different transactions** in **different cells**

Even if the same card has transactions in both cells, they are **different transactions** contributing to **different cells**.

## What About Cards Appearing in Multiple Cells?

**Example**: Card X has:
- 1 transaction in Cell A (Province 1, City 1, MCC 5411, Day 1)
- 1 transaction in Cell B (Province 2, City 5, MCC 5411, Day 1)

**Question**: Does this violate parallel composition?

**Answer**: No! Because:
- Cell A queries: "How many transactions in Cell A?" → Card X contributes 1
- Cell B queries: "How many transactions in Cell B?" → Card X contributes 1
- These are **different queries on different cells**
- The **datasets** (transactions) are still disjoint:
  - Dataset A = {all transactions in Cell A}
  - Dataset B = {all transactions in Cell B}
  - These sets are disjoint (a transaction can't be in both)

## The Key Insight

**Parallel composition applies to DISJOINT DATASETS, not disjoint individuals (cards).**

- ✅ Disjoint datasets → Parallel composition (cost = max)
- ❌ Same dataset queried multiple times → Sequential composition (cost = sum)

In our case:
- Each cell is a query on a **disjoint set of transactions**
- Even if the same card appears in multiple cells, the **transactions** are in different cells
- Therefore: Parallel composition applies

## Summary

1. **Each cell uses the same budget** (e.g., ρ/3 for transaction_count)
2. **All cells together cost only ρ/3** (not N × ρ/3) because of parallel composition
3. **This works because cells are disjoint** (each transaction is in exactly one cell)
4. **Even if cards appear in multiple cells**, the transactions are in different cells, so parallel composition still applies

## Total Privacy Cost

```
Query 1 (transaction_count):  ρ/3  (1M cells, but parallel composition → cost = ρ/3)
Query 2 (unique_cards):       ρ/3  (1M cells, but parallel composition → cost = ρ/3)
Query 3 (total_amount):       ρ/3  (1M cells, but parallel composition → cost = ρ/3)
────────────────────────────────────────────────────────────────────────────
Total:                        ρ    (sequential composition across queries)
```

