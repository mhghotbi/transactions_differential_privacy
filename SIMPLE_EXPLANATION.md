# Simple Explanation: How Budget Works

## The Question
You have 1,000,000 cells. You add noise to all of them. How much privacy budget does this cost?

## Simple Answer

**The budget is NOT per cell. The budget is PER QUERY TYPE.**

## Step-by-Step Example

### Setup
- Total budget: ρ = 0.25
- 3 query types: transaction_count, unique_cards, total_amount
- Budget split: each query gets ρ/3 = 0.083

### What Happens

#### Step 1: For transaction_count query
```python
budget_for_transaction_count = 0.083

# Add noise to ALL 1,000,000 cells using this SAME budget
for each of 1,000,000 cells:
    noisy_value = add_noise(original_value, rho=0.083)
```

**Privacy Cost**: 0.083 (NOT 1,000,000 × 0.083)

**Why?** Because all cells are queried with the **same mechanism** using the **same budget**. This is like asking 1,000,000 different questions, but they're all part of the **same query** (transaction_count).

#### Step 2: For unique_cards query
```python
budget_for_unique_cards = 0.083

# Add noise to ALL 1,000,000 cells using this SAME budget
for each of 1,000,000 cells:
    noisy_value = add_noise(original_value, rho=0.083)
```

**Privacy Cost**: 0.083

#### Step 3: For total_amount query
```python
budget_for_total_amount = 0.083

# Add noise to ALL 1,000,000 cells using this SAME budget
for each of 1,000,000 cells:
    noisy_value = add_noise(original_value, rho=0.083)
```

**Privacy Cost**: 0.083

### Total Privacy Cost

```
transaction_count:  0.083
unique_cards:       0.083
total_amount:       0.083
─────────────────────────
Total:              0.25  ✓
```

## The Key Point

**You're not running 1,000,000 separate DP mechanisms.**
**You're running 1 mechanism that adds noise to 1,000,000 values.**

Think of it like this:
- ❌ NOT: "I'll add noise to Cell 1 with budget 0.083, then Cell 2 with budget 0.083, ..." (this would cost 1,000,000 × 0.083)
- ✅ YES: "I'll add noise to all cells at once using budget 0.083" (this costs 0.083)

## Analogy

Imagine you have a box with 1,000,000 coins. You want to flip all of them with the same randomness.

- ❌ NOT: Flip coin 1 (costs 1 unit of randomness), flip coin 2 (costs 1 unit), ... = 1,000,000 units
- ✅ YES: Flip all coins at once using 1 unit of randomness = 1 unit total

The randomness is shared across all coins.

## In Code

Looking at line 371-377 in `engine/topdown.py`:

```python
# ONE call to add_noise() for ALL cells at once
noisy_values = add_discrete_gaussian_noise(
    data_values,  # This is ALL cell values (1,000,000 values)
    rho=rho,      # This is ONE budget value (0.083)
    ...
)
```

This is **one mechanism** adding noise to **many values**, not **many mechanisms** each adding noise.

## Why This Works

The privacy guarantee is: "The mechanism M that adds noise to all cells satisfies ρ-zCDP."

Not: "Each cell gets its own mechanism M_i that satisfies ρ_i-zCDP."

## Summary

- **Budget is per query type**, not per cell
- **All cells of the same query type share the same budget**
- **Total cost = sum of query budgets** (0.083 + 0.083 + 0.083 = 0.25)
- **NOT = number of cells × budget** (1,000,000 × 0.083 would be wrong)

