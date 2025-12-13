# Architecture Decisions

## Decision Log

This document records key architectural and design decisions for the Transaction SDC System.

---

## ADR-001: SDC vs Formal DP

**Date**: Current
**Status**: Accepted
**Context**: Need to choose between formal Differential Privacy (DP) and Statistical Disclosure Control (SDC) for secure enclave deployment.

**Decision**: Use SDC (utility-first) approach instead of formal DP.

**Rationale**:
- Secure enclave provides primary protection through physical isolation
- SDC provides secondary protection layer with utility-first focus
- Better utility preservation for statistical analysis
- Context-aware plausibility bounds more appropriate for transaction data
- Province invariants (public data) can be preserved exactly

**Consequences**:
- No formal privacy budget tracking (no ε, δ guarantees)
- Focus shifts to plausibility and utility preservation
- DP modules kept for potential future use but not active
- Validation focuses on plausibility checks rather than privacy proofs

---

## ADR-002: Multiplicative Jitter Mechanism

**Date**: Current
**Status**: Accepted
**Context**: Need to choose noise mechanism for SDC.

**Decision**: Use multiplicative jitter `M(c) = c × (1 + η)` where `η ~ N(0, σ²)` and `σ = noise_level × c`.

**Rationale**:
- Preserves ratios naturally (avg_amount, tx_per_card)
- Relative noise scales with value magnitude
- More intuitive than additive noise for counts
- Aligns with SDC best practices

**Consequences**:
- Noise magnitude proportional to value
- Small counts get small noise, large counts get large noise
- Ratios preserved automatically (before clamping)

---

## ADR-003: Context-Aware Plausibility Bounds

**Date**: Current
**Status**: Accepted
**Context**: Need to determine how to set plausibility bounds for noise.

**Decision**: Compute bounds per (MCC, City, Weekday) stratum from data.

**Rationale**:
- Different contexts have different plausible ranges
- Small city + Restaurant MCC + Weekday ≠ Large city + Gas MCC + Weekend
- Data-driven bounds more accurate than global bounds
- Improves utility by allowing appropriate noise per context

**Consequences**:
- Requires computing bounds during preprocessing
- More complex than global bounds
- Better utility preservation

---

## ADR-004: Province Invariants (Exact Totals)

**Date**: Current
**Status**: Accepted
**Context**: Province totals are public data - should they be preserved exactly?

**Decision**: Preserve province totals exactly (no noise at province level).

**Rationale**:
- Province totals are public data (known from other sources)
- Maintaining exact totals improves consistency
- City-level values adjusted to sum to province totals
- Aligns with secure enclave context (public data known)

**Consequences**:
- Province-level aggregates receive no noise
- City-level values must sum to exact province totals
- Requires post-processing adjustment step

---

## ADR-005: Bounded Contribution Method

**Date**: Current
**Status**: Accepted
**Context**: Need to choose method for computing K (max transactions per card per cell).

**Decision**: Use transaction-weighted percentile method (recommended).

**Rationale**:
- Minimizes data loss (keeps most transactions)
- More robust than simple percentile
- Accounts for transaction volume, not just card count
- Better utility preservation

**Alternatives Considered**:
- IQR method: More robust but may lose more data
- Simple percentile: Simpler but less optimal
- Fixed value: Too rigid, doesn't adapt to data

**Consequences**:
- Requires computing transaction-weighted distribution
- More complex than fixed value
- Better utility than alternatives

---

## ADR-006: Per-MCC Winsorization

**Date**: Current
**Context**: Need to handle outliers in transaction amounts.

**Decision**: Winsorize amounts per MCC using percentile caps (default: 99th percentile).

**Rationale**:
- Different MCCs have different amount distributions
- Restaurant transactions ≠ Gas station transactions
- Per-MCC caps more appropriate than global cap
- Reduces outlier influence on statistics

**Consequences**:
- Requires computing caps per MCC during preprocessing
- More complex than global cap
- Better utility preservation

---

## ADR-007: Spark for Distributed Processing

**Date**: Current
**Status**: Accepted
**Context**: Need to process billions of transactions efficiently.

**Decision**: Use Apache Spark for all large-scale operations.

**Rationale**:
- Scales horizontally across cluster
- Handles billions of rows efficiently
- Industry standard for big data processing
- Supports Parquet format natively

**Consequences**:
- Requires Java runtime
- More complex than single-machine processing
- Enables production-scale deployment

---

## ADR-008: Parquet Format for I/O

**Date**: Current
**Status**: Accepted
**Context**: Need efficient storage format for large datasets.

**Decision**: Use Parquet format for all large datasets (not CSV).

**Rationale**:
- Columnar storage (efficient for analytics)
- Compression reduces storage costs
- Spark native support
- Industry standard

**Consequences**:
- Cannot use simple text editors to inspect data
- Requires Parquet-compatible tools
- Better performance than CSV

---

## ADR-009: Legacy DP Code Retention

**Date**: Current
**Status**: Accepted
**Context**: System shifted from DP to SDC - what to do with DP code?

**Decision**: Keep DP modules (budget.py, primitives.py, sensitivity.py) but mark as inactive.

**Rationale**:
- May need formal DP guarantees in future
- Code is tested and documented
- Easy to re-enable if needed
- No harm in keeping it

**Consequences**:
- Codebase includes unused code
- May confuse new developers
- Can be re-enabled if needed

---

## ADR-010: Configuration Management

**Date**: Current
**Status**: Accepted
**Context**: Need centralized configuration management.

**Decision**: Use dataclasses with INI file loading (`core/config.py`).

**Rationale**:
- Type-safe configuration access
- Validation built-in
- Easy to extend
- Clear separation of concerns

**Consequences**:
- Requires dataclass definitions
- Validation logic must be maintained
- More structured than dict-based config

---

## Future Decisions (To Be Made)

- **ADR-011**: Should we add formal DP as optional mode?
- **ADR-012**: Should we support multiple noise mechanisms?
- **ADR-013**: Should we add confidence intervals for SDC output?

