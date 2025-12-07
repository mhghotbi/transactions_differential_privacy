# Formal Privacy Analysis

## 1. Privacy Unit Definition (User-Level DP)

### Definition (Privacy Unit)
A **privacy unit** in this system is defined as a **card (user)** within a monthly release.

**User-Level DP** (vs Event-Level DP):
- **Event-Level**: Each transaction is protected independently (sensitivity = 1)
- **User-Level**: The ENTIRE card's transaction history is protected (sensitivity = √D × K)

We implement **User-Level Differential Privacy**, which means:
- Adding or removing ALL transactions from one card changes the output by at most the sensitivity bound
- This is STRONGER privacy than event-level (protects entire user behavior)
- Requires MORE noise but provides MEANINGFUL protection

### Why User-Level (not Event-Level)?
| Unit | What's Protected | Sensitivity | Privacy Meaning |
|------|------------------|-------------|-----------------|
| Event | Single transaction | 1 | Hiding one purchase |
| User | All card's transactions | √D × K | Hiding card's existence |

**Key Insight**: Event-level DP in transaction data is nearly meaningless because:
1. An adversary can link transactions by card number
2. Protecting one transaction doesn't hide the user's pattern
3. User-level provides protection against membership inference

### Definition (D_max)
**D_max** = Maximum number of distinct cells any single card appears in.

A card appears in a cell (city, MCC, day) if it has at least one transaction there.

**Example**: If card C has transactions in:
- Tehran, Grocery, Monday
- Tehran, Grocery, Tuesday  
- Isfahan, Restaurant, Monday

Then card C appears in D=3 distinct cells.

---

## 2. Sensitivity Analysis

### 2.1 Notation
| Symbol | Definition |
|--------|------------|
| D | Database of transactions |
| D' | Neighboring database (differs by one card's transactions) |
| f(D) | Query output vector (one value per cell) |
| Δ₂ | L2 (global) sensitivity |
| K | Per-cell contribution bound (max transactions per card per cell) |
| D_max | Maximum DISTINCT cells a card appears in |
| W | Winsorization cap (per-cell per-card for amounts) |
| n | Total number of cells in output |

### 2.2 Neighboring Database Definition (User-Level)
Two databases D and D' are **neighbors** if they differ in ALL transactions of exactly ONE card.

Formally: 
- D' = D ∪ {all transactions of card c} (adding a card)
- D' = D \ {all transactions of card c} (removing a card)

**Critical Difference from Census 2020**:
- Census: Person lives in ONE block → changes ONE cell
- Transaction: Card transacts in MULTIPLE cells → changes D_max cells

### 2.3 User-Level Sensitivity Formula

**Theorem (User-Level L2 Sensitivity)**:
For a query f where one card can:
- Appear in at most D_max distinct cells
- Contribute at most C to each cell

The L2 sensitivity is:
```
Δ₂(f) = √(D_max) × C
```

**Proof**:
```
||f(D) - f(D')||₂ 
= √(Σᵢ (fᵢ(D) - fᵢ(D'))²)
= √(Σᵢ∈affected_cells Cᵢ²)
≤ √(D_max × C²)        [at most D_max cells, each changes by at most C]
= √(D_max) × C
```

### 2.3 Query Sensitivities

#### Transaction Count
```
Query: f_count(D) = count of transactions in each (city, mcc, day) cell

Sensitivity Analysis:
- One card can appear in at most M cells
- In each cell, contribution is bounded to K transactions
- Change when removing card: at most K in each of M cells

L2 Sensitivity: Δ₂(f_count) = √(M × K²) = √M × K
```

**Proof**:
```
||f_count(D) - f_count(D')||₂ 
= √(Σᵢ (countᵢ(D) - countᵢ(D'))²)
≤ √(M × K²)        [at most M cells affected, each by at most K]
= √M × K
```

#### Unique Cards
```
Query: f_cards(D) = count of unique cards in each cell

Sensitivity Analysis:
- One card is counted at most once per cell
- Can appear in at most M cells
- Change when removing card: at most 1 in each of M cells

L2 Sensitivity: Δ₂(f_cards) = √M × 1 = √M
```

#### Unique Acceptors
```
Query: f_acceptors(D) = count of unique acceptors in each cell

Sensitivity Analysis (Conservative):
- Worst case: card is the ONLY one using M different acceptors
- Removing card removes 1 acceptor count from M cells

L2 Sensitivity: Δ₂(f_acceptors) ≤ √M
```

**Note**: This is conservative. In practice, acceptors typically serve many cards, so the true sensitivity is often lower.

#### Total Amount
```
Query: f_amount(D) = sum of (winsorized) amounts in each cell

Sensitivity Analysis:
- Each transaction amount is capped at W
- Card contributes at most K × W per cell (K transactions × W per transaction)
- Can affect M cells

L2 Sensitivity: Δ₂(f_amount) = √M × K × W
```

### 2.4 Sensitivity Summary Table

| Query | L2 Sensitivity | With K=5, M=100, W=10M |
|-------|---------------|------------------------|
| transaction_count | √M × K | √100 × 5 = 50 |
| unique_cards | √M | √100 = 10 |
| unique_acceptors | √M | √100 = 10 |
| total_amount | √M × K × W | √100 × 5 × 10⁷ = 5×10⁸ |

---

## 3. Mechanism: Discrete Gaussian

### 3.1 Definition
The **Discrete Gaussian Mechanism** adds noise from the discrete Gaussian distribution:

```
M(D) = f(D) + Z,  where Z ~ N_ℤ(0, σ²)
```

The discrete Gaussian distribution over integers with variance σ² is:
```
Pr[Z = z] = exp(-z² / (2σ²)) / Σₖ exp(-k² / (2σ²))
```

### 3.2 Privacy Guarantee (zCDP)

**Theorem (Bun & Steinke 2016)**:
The Discrete Gaussian mechanism with σ² = Δ₂² / (2ρ) satisfies ρ-zCDP.

**Proof Sketch**:
1. Discrete Gaussian satisfies (α, α/(2σ²))-RDP for all α > 1
2. Taking the limit, it satisfies ρ-zCDP with ρ = Δ₂² / (2σ²)
3. Solving for σ²: σ² = Δ₂² / (2ρ)

### 3.3 Conversion to (ε, δ)-DP

**Theorem**: ρ-zCDP implies (ε, δ)-DP for any δ > 0 with:
```
ε = ρ + 2√(ρ × ln(1/δ))
```

**Example** (with ρ = 0.25, δ = 10⁻¹⁰):
```
ε = 0.25 + 2√(0.25 × ln(10¹⁰))
  = 0.25 + 2√(0.25 × 23.03)
  = 0.25 + 2√5.76
  = 0.25 + 4.80
  = 5.05
```

---

## 4. Composition Theorem

### 4.1 Composition Across Queries

**Theorem (zCDP Composition)**:
If mechanisms M₁, M₂, ..., Mₙ satisfy ρ₁, ρ₂, ..., ρₙ-zCDP respectively, then the composition M = (M₁, M₂, ..., Mₙ) satisfies (Σρᵢ)-zCDP.

**Application**: For 4 queries each with budget ρ/4:
```
Total budget = ρ/4 + ρ/4 + ρ/4 + ρ/4 = ρ ✓
```

### 4.2 Composition Across Time (Days)

**Approach**: Parallel composition within a month.

Since we report (city, mcc, **day**) cells:
- Day 1: Cells (city, mcc, day=1) get noise with ρ_day
- Day 2: Cells (city, mcc, day=2) get noise with ρ_day
- ...
- Day 30: Cells (city, mcc, day=30) get noise with ρ_day

**Key Insight**: A card's transactions on different days affect DIFFERENT cells.
By parallel composition: Total privacy cost for the month = ρ_day (not 30 × ρ_day).

### 4.3 Budget Allocation Tree

```
Monthly Budget: ρ = 0.25
│
├── Province-Month Level: 0% (PUBLIC DATA - no noise)
│   └── These totals are published exactly (no privacy cost)
│
└── Cell Level: 100% = 0.25
    └── Per query: 0.25/3 ≈ 0.083
```

**Total composition**:
- Province-month: 0 (public invariants)
- Cell-level queries: 3 × 0.083 = 0.25
- **Total**: 0.25 ✓

**Key Point**: Since province-month totals are public data (already published), they consume no privacy budget. The entire budget is allocated to cell-level measurements.

---

## 5. Post-Processing Guarantee

### 5.1 Theorem (Post-Processing Immunity)
If M satisfies ρ-zCDP, then for any function g, the mechanism g(M(D)) also satisfies ρ-zCDP.

**Implication**: Our post-processing steps do NOT increase privacy cost:

1. **Non-negativity clamping**: max(0, noisy_value) — Free!
2. **NNLS optimization**: Adjusting to match totals — Free!
3. **Controlled rounding**: Converting to integers — Free!
4. **Confidence intervals**: Computing from σ — Free!
5. **Suppression**: Hiding small cells — Free (and improves privacy)!

### 5.2 Province-Month Invariants (Public Data)

**Important**: Our system uses **province-month totals as public invariants**.

Our invariants:
- Province-month totals: EXACT (no DP cost - these are publicly published data)

**Justification**: Province-month totals are already publicly available (e.g., from aggregate banking reports, government statistics). Releasing them exactly doesn't reveal additional information beyond what is already public.

**Algorithm**:
1. Compute province-month totals from data (these match public statistics)
2. Add DP noise to cell-level (city, mcc, day) measurements with full budget
3. NNLS post-processing ensures cell sums match exact province-month invariants
4. Controlled rounding produces integer outputs

**Privacy Guarantee**: The cell-level measurements satisfy ρ-zCDP. The province-month invariants are treated as public information (no privacy cost).

---

## 6. Final Privacy Statement

### 6.1 Monthly Privacy Guarantee

For each monthly release, the system provides:

| Metric | Value |
|--------|-------|
| zCDP parameter (ρ) | 0.25 |
| (ε, δ)-DP with δ=10⁻¹⁰ | ε ≈ 5.05 |
| (ε, δ)-DP with δ=10⁻⁶ | ε ≈ 4.22 |

### 6.2 Annual Privacy Guarantee (12 Monthly Releases)

If the same card appears in all 12 months:

**Sequential Composition**: Total ρ_annual = 12 × 0.25 = 3.0

Converting to (ε, δ)-DP with δ=10⁻¹⁰:
```
ε_annual = 3.0 + 2√(3.0 × 23.03) = 3.0 + 16.6 = 19.6
```

### 6.3 Formal Statement

**Theorem**: The Transaction DP System satisfies ρ_monthly-zCDP per month, where:
- ρ_monthly = 0.25 (configurable)
- Each card's monthly activity is protected
- Total annual privacy cost is at most 12 × ρ_monthly under sequential composition

---

## 7. Comparison with US Census 2020

| Aspect | Census 2020 | Our System |
|--------|-------------|------------|
| **Privacy Unit** | Person | Card-Month |
| **Mechanism** | Discrete Gaussian | Discrete Gaussian ✓ |
| **Framework** | zCDP | zCDP ✓ |
| **Geographic Hierarchy** | 6 levels | 2 levels |
| **NNLS Post-Processing** | Yes | Yes ✓ |
| **Controlled Rounding** | Yes | Yes ✓ |
| **Invariants** | Pop totals exact | Province-month totals (public) ✓ |
| **Suppression** | Yes | Yes ✓ |
| **Confidence Intervals** | Yes | Yes ✓ |
| **Global Sensitivity** | N/A (one residence) | √M × K ✓ |
| **Total ρ** | ~2.63 (decennial) | 0.25 (monthly) |

### Key Differences:
1. **Multiple appearances**: Census has 1 person → 1 block. We have 1 card → M cells.
2. **Release frequency**: Census is once/decade. We release monthly.
3. **Sensitivity**: Census has L2 = √(#queries). We have L2 = √M × K per query.

---

## 8. Verification Checklist

### 8.1 Implementation Verification
- [ ] Discrete Gaussian uses exact rational arithmetic
- [ ] RNG uses `secrets` module (cryptographic)
- [ ] Sensitivity computed correctly with M and K
- [ ] Budget allocation sums to total_rho
- [ ] NNLS preserves non-negativity
- [ ] Rounding preserves sum constraints
- [ ] Suppression applied after noise (on noisy values)

### 8.2 Statistical Verification (from test suite)
- [ ] Noise mean ≈ 0 (z-test)
- [ ] Noise variance = σ² (within 10%)
- [ ] Noise distribution passes chi-squared test
- [ ] Membership inference accuracy ≈ 50%

### 8.3 Audit Trail
Each release should include:
1. Privacy parameters used (ρ, δ, ε)
2. Sensitivity values (M, K, Δ₂)
3. σ values for each query
4. Suppression threshold and count
5. Date/time of release

---

## References

1. Bun, M., & Steinke, T. (2016). Concentrated differential privacy: Simplifications, extensions, and lower bounds. *TCC 2016*.

2. Canonne, C. L., Kamath, G., & Steinke, T. (2020). The discrete gaussian for differential privacy. *NeurIPS 2020*.

3. Abowd, J. M., et al. (2022). The 2020 Census Disclosure Avoidance System TopDown Algorithm. *Harvard Data Science Review*.

4. Dwork, C., & Roth, A. (2014). The algorithmic foundations of differential privacy. *Foundations and Trends in Theoretical Computer Science*.

5. Census Bureau. (2021). Disclosure Avoidance for the 2020 Census: An Introduction. *census.gov*.

