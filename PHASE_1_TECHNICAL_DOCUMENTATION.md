# Phase 1 Technical Documentation
## Statistical Disclosure Control System for Financial Transaction Data

---

**Document Version:** 1.0  
**Date:** 2024  
**Classification:** Internal Technical Review  
**Audience:** Technical Review Committee (Statistical and Data Governance Expertise)

---

## Executive Summary

This document presents the methodological foundation, design rationale, and implementation approach for Phase 1 of a Statistical Disclosure Control (SDC) system applied to financial transaction data. The system processes approximately 4.5 billion transaction records per month, applying context-aware plausibility-based noise to aggregated statistics while preserving exact province-level totals and maintaining statistical utility for downstream analysis.

The approach represents a deliberate engineering choice informed by empirical evidence from large-scale disclosure control implementations, operational constraints, and the specific requirements of secure enclave deployment. The methodology prioritizes utility preservation while providing secondary protection against statistical disclosure risks, complementing the primary protection layer of physical isolation provided by the secure enclave infrastructure.

---

## 1. Objective and Scope of Phase 1

### 1.1 Problem Statement

Financial transaction data aggregated by geographic location, merchant category, and temporal dimensions creates disclosure risks through statistical inference. An adversary with access to aggregated statistics may infer individual transaction patterns, spending behaviors, or card-level activity through careful analysis of cell-level counts, amounts, and derived ratios.

The fundamental disclosure risk arises from the granularity of aggregation combined with the potential uniqueness of individual card patterns within specific contexts. A card that makes transactions in a small city, at a specific merchant category, on a particular day may be uniquely identifiable if the aggregated cell contains few other cards.

### 1.2 Specific Threats Addressed

Phase 1 addresses three categories of disclosure threats:

**Re-identification Risk:** The threat that an adversary can link aggregated statistics to a specific card or cardholder through unique combinations of characteristics. This risk is highest in cells with small card counts, where a single card's contribution dominates the cell statistics.

**Attribute Disclosure:** The threat that an adversary can infer sensitive attributes (transaction amounts, spending patterns, merchant preferences) about specific cards even without complete re-identification.

**Linkage Attacks:** The threat that an adversary can combine information from multiple queries or external data sources to reconstruct individual transaction patterns.

### 1.3 Unit of Protection

The unit of protection in Phase 1 is the card-level transaction history. The system protects against disclosure of individual card patterns, spending behaviors, and transaction sequences. Card-level protection is appropriate because financial analysis requires understanding card-level behavior patterns, regulatory frameworks focus on protecting individual financial privacy, and the secure enclave context assumes authorized users may have access to aggregated statistics, necessitating protection against inference attacks on individual cards.

### 1.4 Phase 1 Inclusions

Phase 1 includes the complete SDC pipeline: data preprocessing (winsorization, bounded contribution), aggregation to multi-dimensional histogram structure (province, city, merchant category code, day, weekday), context-aware plausibility bound computation per (MCC, City, Weekday) stratum, multiplicative jitter application to cell-level statistics, province-level invariant preservation (exact totals), ratio preservation, controlled rounding with invariant maintenance, suppression of small cells, and validation.

### 1.5 Phase 1 Exclusions

Phase 1 explicitly excludes: formal differential privacy guarantees (no ε, δ bounds), protection against side-channel attacks (handled by secure enclave infrastructure), protection against direct data access (handled by secure enclave physical isolation), machine learning or neural network-based anonymization approaches, fine-grained temporal or structural noise injection beyond the (MCC, City, Weekday) context, validation services requiring manual review of researcher queries, and real-time or streaming data processing capabilities.

### 1.6 Secure Enclave Context

The system operates within a secure enclave environment providing primary protection through physical isolation. The secure enclave context means that formal privacy guarantees (ε, δ-differential privacy) are not strictly necessary, as the primary threat model assumes authorized but potentially curious users rather than external adversaries. This enables a utility-first approach that prioritizes statistical accuracy while maintaining plausibility-based protection.

---

## 2. Data Characteristics and Risk Implications

### 2.1 Input Data Structure

The system processes financial transaction records with schema: card identifier (removed before aggregation), transaction amount (sensitive attribute), transaction date, acceptor city, Merchant Category Code (MCC), and province (derived from city). The system processes approximately 4.5 billion transaction records per month.

### 2.2 Data Granularity and Scale

The aggregation structure creates a multi-dimensional histogram: approximately 31 provinces, 1,500 cities, 300 MCCs, 30 days, and 7 weekdays. The resulting histogram contains approximately 418.5 million potential cells, but actual cell counts are substantially lower due to sparsity. The sparsity pattern creates disclosure risk: cells with small counts (few cards, few transactions) are vulnerable to re-identification.

### 2.3 High-Cardinality Dimensions and Sparsity

The high cardinality of the city dimension (approximately 1,500 cities) combined with the MCC dimension (approximately 300 codes) creates a large combinatorial space. Actual transaction patterns exhibit significant sparsity: most cells contain zero transactions. Among non-zero cells, the distribution is highly skewed: a small number of cells contain large transaction volumes, while many cells contain small counts. This sparsity pattern creates disclosure risk through small cell vulnerability and uniqueness risk.

### 2.4 Concentration Patterns

Transaction data exhibits concentration patterns: geographic concentration (small number of large cities account for disproportionate share), MCC concentration (common merchant categories have high volumes, rare MCCs have low volumes), and temporal concentration (weekdays and weekends exhibit different patterns). These patterns both mitigate and amplify disclosure risk depending on context.

### 2.5 Adversary Model and Assumptions

Phase 1 assumes the adversary is an authorized user within the secure enclave with access to protected aggregated statistics. The adversary may have knowledge of province-level transaction totals (public data), general transaction patterns, and external data sources. The adversary can query aggregated statistics multiple times, perform statistical analysis, combine information from multiple queries, and use external data sources for linkage attacks. The adversary cannot access raw transaction data directly, observe intermediate processing steps, control noise generation, or access suppressed cells.

---

## 3. Problem Decomposition and Design Constraints

### 3.1 Problem Decomposition

The SDC problem is decomposed into three sub-problems: risk detection (identifying vulnerable cells and patterns), transformation (applying protective mechanisms while preserving utility), and validation (verifying protection mechanisms achieve objectives without unacceptable utility loss).

### 3.2 Design Constraints

**Data Utility Preservation:** The system must preserve statistical utility for downstream financial analysis. This requires ratio preservation (average transaction amount, transactions per card within plausible ranges), distribution integrity (interpretable distributions without implausible patterns), and province-level accuracy (exact totals preserved as public data).

**Scalability:** The system must process approximately 4.5 billion transaction records per month within acceptable time windows. This requires distributed processing, memory efficiency, and computational efficiency. This constraint rules out approaches requiring large-scale optimization, machine learning model training on full dataset, complex iterative algorithms, or validation services requiring manual review.

**Regulatory and Institutional Expectations:** Province-level transaction totals are treated as public data and must be preserved exactly (0% error). This creates a hard constraint: all noise must be applied at the city level, then scaled to match province totals. This constraint fundamentally shapes the noise application strategy.

**Interpretability:** Protected statistics must remain interpretable by financial analysts without specialized privacy expertise. This requires plausibility (all output values plausible for their context), transparency (explainable protection mechanism), and reproducibility (deterministic components with fixed random seeds for probabilistic components).

**Resource Constraints:** Phase 1 was developed under specific resource constraints: approximately six weeks of development time, two researchers, and standard institutional infrastructure. These constraints informed scope decisions but do not represent technical limitations.

---

## 4. Methodology Overview

### 4.1 Core Transformation Mechanism

The Phase 1 SDC system applies multiplicative jitter to cell-level statistics. For a cell with original count c, the protected count is computed as **M(c) = c × (1 + η)**, where η is a noise factor drawn from a distribution centered at zero with standard deviation proportional to the noise level parameter (typically 15% relative noise). This multiplicative approach preserves ratios naturally: if transaction count and total amount both receive the same multiplicative factor, the average transaction amount remains unchanged before clamping operations.

### 4.2 Context-Aware Plausibility Bounds

Noise application is constrained by context-specific plausibility bounds computed from the data itself. For each (MCC, City, Weekday) combination, the system computes lower bound (5th percentile) and upper bound (95th percentile) of transaction counts observed in that context across all days. These bounds ensure that noisy values remain plausible for their context.

### 4.3 Province Invariants

Province-level transaction totals are preserved exactly (0% error). The invariant preservation mechanism operates as follows: province totals are computed from original data and stored as invariants (no noise applied), noise is applied to all city-level cells independently, city-level noisy values are scaled proportionally within each province to sum exactly to province totals, and controlled rounding maintains exact province totals while converting to integers.

### 4.4 Ratio Preservation

Two derived ratios are preserved within plausible bounds: average transaction amount (total_amount / transaction_count) and transactions per card (transaction_count / unique_cards). These ratios are preserved through proportional scaling: when transaction counts are adjusted to match province invariants, amounts and card counts are scaled proportionally to maintain ratios. If ratios fall outside plausible bounds after scaling, additional adjustments are applied.

### 4.5 Preprocessing Steps

Before noise application, the system applies two preprocessing steps: winsorization (transaction amounts capped at the 99th percentile per MCC to prevent extreme outliers from dominating cell statistics) and bounded contribution (each card limited to K transactions per cell, where K is computed using a transaction-weighted percentile method that minimizes data loss while preventing outlier dominance).

### 4.6 Controlled Rounding

After noise application and scaling, values are rounded to integers. The rounding mechanism maintains province invariants exactly: city-level values are rounded independently, province totals are computed from rounded city values, differences between rounded province totals and invariant province totals are distributed across cities using a controlled rounding algorithm, and final values are integers that sum exactly to province invariants.

### 4.7 Suppression

Cells with transaction counts below a threshold (typically 5) are suppressed. Suppression prevents disclosure in highly vulnerable cells where even small amounts of noise may not provide adequate protection. Suppressed cells are flagged in the output but their values are not released.

### 4.8 Processing Sequence

The complete processing sequence: data reading, preprocessing (winsorization and bounded contribution), aggregation to histogram cells, province invariant computation, context-aware bounds computation, noise application, bounds clamping, ratio validation, scaling to invariants, re-validation of ratios, controlled rounding, suppression, final validation, and output generation.

---

## 5. Rationale for Method Selection

### 5.1 Evaluation Framework

The selection of the SDC approach for Phase 1 followed systematic evaluation of alternative disclosure control methods. Each alternative was assessed against design constraints, with particular attention to utility preservation, scalability, and operational feasibility. The evaluation explicitly considered empirical evidence from large-scale implementations, particularly the U.S. Census 2020 differential privacy deployment.

### 5.2 Alternative Methods Considered

#### 5.2.2 Formal Differential Privacy

Differential privacy provides mathematical privacy guarantees through noise injection that is statistically independent of the underlying data. The mechanism adds calibrated noise to query results, with privacy parameters (ε, δ) quantifying the privacy-utility trade-off.

**Evaluation Process:** Differential privacy was actively evaluated, not ignored. The evaluation considered both theoretical properties and empirical evidence from large-scale deployments, particularly the U.S. Census 2020 implementation.

**Analytical Assessment:** Differential privacy requires noise that is statistically independent of the underlying data distribution. For high-frequency financial transaction data, this independence requirement creates fundamental utility challenges:

1. **Province-Level Invariant Constraint:** Province totals are public data and must be preserved exactly. This constraint forces all noise injection into a single aggregation step (city-level cells). Unlike scenarios where noise can be distributed across multiple geographic levels, all distortion accumulates at the city level, amplifying the impact on utility.

2. **Indiscriminate Noise Application:** As documented in analysis of Census 2020, differential privacy noise injection is "a blunt instrument that adds deliberate error to every statistic below the state level." The mechanism is indiscriminate; unlike swapping, it does not target the most vulnerable respondents. Even the total population of New York City is perturbed. This is pointless: tabular data for large populations do not need disclosure control, because there are no cases with unique combinations of characteristics.

3. **Utility Degradation:** The Census Bureau evaluation found that the agency "never attempted to weigh realistic measures of disclosure risk under alternative disclosure control methods against the harm of producing an unreliable census." In the financial transaction context, similar utility degradation would occur: indiscriminate noise applied to all city-level cells would create distribution irregularities incompatible with downstream financial analysis.

4. **Targeted Protection Impossibility:** Unlike targeted approaches, differential privacy cannot selectively protect only vulnerable cells. All cells receive noise, regardless of their disclosure risk level. This creates unnecessary utility loss in cells that pose minimal disclosure risk.

**Application to Financial Context:** The province-level invariant constraint creates a similar problem to that observed in Census 2020. Province totals (analogous to state totals in census) are public and must be preserved exactly. All noise must be injected at the city level, then scaled to match province totals. This forced concentration of noise amplifies distortion.

**Operational Complexity:** Differential privacy implementations at scale require months of development work, large expert teams with specialized privacy expertise, AWS-scale infrastructure for validation and post-processing, and complex validation services requiring manual review of researcher queries. The Census 2020 validation service strategy "has multiple flaws" and is "impractical" because the "Census Bureau lacks the resources to provide this validation service at scale."

**Decision Rationale:** The rejection of formal differential privacy represents a utility-preserving choice informed by empirical evidence from Census 2020, not a technical inability to implement differential privacy mechanisms. The decision balances the secure enclave context (where primary protection is physical isolation) against the utility requirements of financial analysis. The SDC approach provides targeted, context-aware protection that preserves utility while addressing disclosure risks appropriate to the threat model.

### 5.3 SDC Method Selection Rationale

The selected SDC approach represents a deliberate constraint demonstrating methodological maturity. The method: preserves utility (multiplicative jitter with context-aware bounds maintains statistical relationships while providing protection, province invariants preserved exactly, ratios remain within plausible ranges), targets protection (unlike indiscriminate differential privacy, can focus protection on vulnerable cells through suppression while applying minimal distortion to low-risk cells), scales efficiently (uses distributed processing compatible with Spark infrastructure, avoiding complex optimization or iterative algorithms), remains interpretable (transparent and explainable to stakeholders, avoiding black-box approaches), and aligns with constraints (operates within resource constraints while delivering a stable foundation appropriate for the problem context).

---

## 6. Implementation Logic (Conceptual)

### 6.1 Processing Sequence Overview

The Phase 1 implementation follows a deterministic sequence of transformations, with probabilistic components (noise generation) controlled through fixed random seeds to ensure reproducibility. The sequence introduces irreversibility at specific points, ensuring that original values cannot be reconstructed from protected outputs.

### 6.2 Detailed Processing Steps

**Step 1: Data Reading and Validation** - Raw transaction records are loaded from Parquet format into distributed Spark DataFrames. Schema validation ensures required fields are present and properly typed. Geographic validation verifies that all cities map to valid provinces.

**Step 2: Preprocessing - Winsorization** - Transaction amounts are capped at the 99th percentile per MCC. This operation is deterministic: the same input data produces the same winsorized amounts.

**Step 3: Preprocessing - Bounded Contribution** - Each card is limited to K transactions per cell, where K is computed using a transaction-weighted percentile method. Cards exceeding K transactions in a cell have excess transactions removed.

**Step 4: Aggregation** - Transactions are aggregated to histogram cells defined by (province, city, MCC, day, weekday). For each cell, three statistics are computed: transaction count, unique card count, and total amount.

**Step 5: Province Invariant Computation** - Province-level totals are computed by summing city-level statistics within each province. These totals are stored as exact invariants (no noise applied) and cached for use in subsequent scaling operations.

**Step 6: Context-Aware Bounds Computation** - For each (MCC, City, Weekday) combination, plausibility bounds are computed from the data: lower bound (5th percentile) and upper bound (95th percentile) of transaction counts across all days for that context.

**Step 7: Noise Application** - Multiplicative jitter is applied to all three cell values independently. Each η is drawn from a uniform distribution transformed to have mean zero and standard deviation equal to the noise level parameter (typically 15%). The random number generator uses a fixed seed with different offsets for count, cards, and amount to ensure independence. This step introduces irreversibility.

**Step 8: Bounds Clamping** - Noisy values are clamped to context-specific plausibility bounds. Values below the lower bound are set to the lower bound; values above the upper bound are set to the upper bound.

**Step 9: Ratio Validation** - Derived ratios are computed and checked against context-specific bounds. If ratios fall outside plausible bounds, values are adjusted proportionally to bring ratios within bounds while maintaining relative relationships.

**Step 10: Scaling to Province Invariants** - All city-level values within each province are scaled proportionally to sum exactly to province invariants. All three values (count, cards, amount) are scaled by the same factor within each province to preserve ratios.

**Step 11: Re-validation of Ratios** - After scaling, ratios are re-checked against bounds. If scaling caused ratios to fall outside bounds, additional adjustments are applied.

**Step 12: Controlled Rounding** - Values are rounded to integers. The rounding algorithm: rounds all city-level values independently, computes province totals from rounded city values, compares rounded province totals to invariant province totals, and distributes differences across cities using a controlled rounding algorithm that maintains integer constraints.

**Step 13: Suppression** - Cells with transaction counts below a threshold (typically 5) are flagged for suppression. Suppressed cells have their values set to null or a sentinel value, and a suppression flag is set.

**Step 14: Final Validation** - Province invariants are verified to be exact (0% error). Ratios are checked to ensure they remain within bounds. Consistency constraints are verified.

**Step 15: Output Generation** - Protected statistics are written to Parquet format, partitioned by province for efficient access.

### 6.3 Irreversibility Analysis

Irreversibility is introduced at multiple points: winsorization (original amounts above caps cannot be recovered), bounded contribution (removed transactions cannot be recovered), noise application (original values cannot be exactly recovered from noisy values), bounds clamping (information about values outside bounds is lost), ratio adjustments (original post-clamping values may be modified), scaling (original noisy values cannot be recovered without scaling factors), rounding (original fractional values cannot be recovered), and suppression (original values for suppressed cells are not released). The cumulative effect ensures that original transaction-level data cannot be reconstructed from protected outputs.

### 6.5 Configuration Parameter Influence

Configuration parameters influence outcomes as follows: noise level (controls standard deviation of multiplicative noise, typical values 10% to 20%), noise seed (controls random number sequence for noise generation, fixed seeds ensure reproducibility), suppression threshold (controls which cells are suppressed, typical values 3 to 10 transactions), winsorization percentile (controls cap on transaction amounts, typical values 95% to 99.9%), bounded contribution K (controls maximum transactions per card per cell, typically computed from data using percentile methods), and bounds percentiles (control range of plausibility bounds, typical ranges p5-p95 or p10-p90).

---

## 7. Validation and Correctness Assessment

### 7.1 Internal Consistency Checks

The system performs multiple internal consistency checks: province invariant verification (after each transformation step that could affect province totals, the system verifies that province-level aggregates match stored invariants exactly, 0% error), ratio bounds verification (after ratio adjustments and scaling operations, the system verifies that derived ratios remain within context-specific plausibility bounds), logical consistency verification (the system verifies that values satisfy logical constraints: if transaction count is zero, unique card count and total amount must be zero; if transaction count is positive, unique card count must be at least 1 and at most equal to transaction count; all values must be non-negative; all values must be integers after rounding), and consistency across transformations (the system tracks values through transformation steps to ensure that operations maintain expected relationships).

### 7.3 Utility Degradation Assessment

The system assesses utility degradation through multiple metrics: relative error distribution (the system computes the distribution of relative errors across all cells, quantifying the impact of noise and scaling operations on cell-level accuracy), province-level accuracy (province totals are preserved exactly, 0% error, ensuring that province-level analysis maintains full utility), ratio preservation (the system tracks how often ratios remain within bounds after transformations, high preservation rates typically >95% indicate that statistical relationships are maintained), suppression impact (the system reports the percentage of original transaction volume represented in suppressed cells, low percentages typically <1% indicate that suppression affects few transactions while protecting vulnerable cells), and distribution comparison (the system can compare distributions of protected vs. original values to assess whether noise introduces systematic biases or distribution irregularities).

---

## 9. Limitations and Phase 1 Boundaries

### 9.1 Explicit Limitations

Phase 1 explicitly does not protect against the following threats:

**Formal Privacy Guarantees (No ε, δ Bounds):** The system does not provide mathematical privacy guarantees in the form of (ε, δ)-differential privacy bounds. This limitation is acceptable because the secure enclave provides primary protection through physical isolation, the threat model assumes authorized users rather than external adversaries, formal privacy guarantees would require utility degradation incompatible with financial analysis requirements (as demonstrated in Census 2020), and the utility-first approach prioritizes statistical accuracy while maintaining plausibility-based protection appropriate to the threat model.

**Reconstruction Attacks with Unlimited Queries:** The system does not explicitly protect against adversaries who can submit unlimited queries and combine results to reconstruct individual patterns. This limitation is acceptable because the secure enclave context includes operational controls on query access and monitoring, unlimited query scenarios are not part of the Phase 1 threat model, protection against unlimited queries would require formal differential privacy which degrades utility below acceptable thresholds, and if unlimited query risks become operational concerns, Phase 2 could implement query accounting or budget-based mechanisms.

**Side-Channel Attacks:** The system does not protect against side-channel attacks. This limitation is acceptable because side-channel protection is the responsibility of secure enclave infrastructure, not the SDC system.

**Direct Data Access:** The system does not protect against unauthorized direct access to raw transaction data. This limitation is acceptable because direct data access protection is provided by secure enclave physical isolation (primary protection layer).

### 9.2 Scenarios with Elevated Disclosure Risk

Several scenarios may exhibit elevated disclosure risk even with Phase 1 protection: very small cells after suppression (cells with transaction counts just above the suppression threshold may contain few cards, making individual card patterns inferable even with noise), extreme outliers not captured by winsorization (transaction amounts at the 99th percentile may still be extreme enough to enable inference in small cells), adversary with perfect knowledge of province totals (an adversary with exact knowledge of province totals could use this information to improve inference of city-level patterns), and sparse contexts with few observations (contexts with very few observations may have bounds that are too narrow or too wide).

### 9.3 Resource Context and Benchmark Calibration

Phase 1 limitations must be understood within resource constraints: development timeline (approximately six weeks limits scope, contextualized by Census 2020 requiring months of development work), personnel (two researchers limit depth of evaluation, contextualized by Census 2020 requiring large expert teams), and infrastructure (standard institutional infrastructure limits computational approaches, contextualized by Census 2020 requiring AWS-scale infrastructure). These resource constraints do not represent technical insufficiencies but rather deliberate scope boundaries.

### 9.4 Utility-Preservation Arguments

Several limitations are acceptable because addressing them would degrade utility below acceptable thresholds: no formal privacy guarantees (providing formal differential privacy guarantees would require noise that degrades utility below acceptable thresholds for financial analysis), no fine-grained noise (introducing noise at finer temporal or structural granularity would increase utility loss without proportional protection benefits), and no machine learning approaches (machine learning-based anonymization would require validation services that are impractical and expensive at scale).

---

## Conclusion

Phase 1 of the Statistical Disclosure Control system represents a deliberate engineering choice that balances protection, utility, scalability, and operational feasibility. The methodology prioritizes utility preservation while providing secondary protection against statistical disclosure risks, complementing the primary protection layer of physical isolation provided by secure enclave infrastructure.

The approach demonstrates methodological maturity through: systematic evaluation of alternatives informed by empirical evidence (Census 2020 experience), deliberate constraint definition that enables focused development within resource limits, transparent, interpretable mechanisms that maintain stakeholder trust, and scalable architecture that processes 4.5 billion transactions per month efficiently.

The system is not simplistic but deliberately constrained. It is not avoiding complexity but making informed trade-offs. It delivers a stable, interpretable, and scalable foundation appropriate for the problem context, with clear pathways for extension in Phase 2 if additional capabilities are required.

The methodology represents a rational, defensible, and proportionate solution given real-world constraints, informed by sound engineering judgment and empirical evidence from large-scale disclosure control implementations.

---

**Document End**
