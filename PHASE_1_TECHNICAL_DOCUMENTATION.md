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

The fundamental disclosure risk arises from the granularity of aggregation combined with the potential uniqueness of individual card patterns within specific contexts. A card that makes transactions in a small city, at a specific merchant category, on a particular day may be uniquely identifiable if the aggregated cell contains few other cards. Even when cells contain multiple cards, statistical analysis of multiple queries may enable reconstruction of individual patterns.

### 1.2 Specific Threats Addressed

Phase 1 addresses three categories of disclosure threats:

**Re-identification Risk:** The threat that an adversary can link aggregated statistics to a specific card or cardholder through unique combinations of characteristics. This risk is highest in cells with small card counts, where a single card's contribution dominates the cell statistics.

**Attribute Disclosure:** The threat that an adversary can infer sensitive attributes (transaction amounts, spending patterns, merchant preferences) about specific cards even without complete re-identification. This occurs when cell-level statistics reveal information about individual contributions through statistical analysis.

**Linkage Attacks:** The threat that an adversary can combine information from multiple queries or external data sources to reconstruct individual transaction patterns. This risk increases when the same underlying data is queried multiple times with different aggregations, potentially enabling exact reconstruction through linear algebra techniques.

### 1.3 Unit of Protection

The unit of protection in Phase 1 is the card-level transaction history. The system protects against disclosure of individual card patterns, spending behaviors, and transaction sequences. This user-level protection approach recognizes that a single card may contribute to multiple cells across different geographic locations, merchant categories, and time periods.

The card-level unit of protection is appropriate because:
- Financial analysis typically requires understanding card-level behavior patterns
- Regulatory frameworks focus on protecting individual financial privacy
- The secure enclave context assumes authorized users may have access to aggregated statistics, necessitating protection against inference attacks on individual cards
- Card-level protection aligns with the bounded contribution mechanism (K transactions per card per cell), which limits individual card impact on cell statistics

### 1.4 Phase 1 Inclusions

Phase 1 includes the complete SDC pipeline from raw transaction input through protected statistical output:

- Data preprocessing including winsorization of transaction amounts and bounded contribution enforcement
- Aggregation to multi-dimensional histogram structure (province, city, merchant category code, day, weekday)
- Context-aware plausibility bound computation per (MCC, City, Weekday) stratum
- Multiplicative jitter application to cell-level statistics (transaction count, unique cards, total amount)
- Province-level invariant preservation (exact totals maintained)
- Ratio preservation (average transaction amount, transactions per card)
- Controlled rounding with invariant maintenance
- Suppression of small cells below threshold
- Validation and correctness assessment

### 1.5 Phase 1 Exclusions

Phase 1 explicitly excludes:

- Formal differential privacy guarantees (no ε, δ bounds)
- Protection against side-channel attacks (handled by secure enclave infrastructure)
- Protection against direct data access (handled by secure enclave physical isolation)
- Machine learning or neural network-based anonymization approaches
- Fine-grained temporal or structural noise injection beyond the (MCC, City, Weekday) context
- Validation services requiring manual review of researcher queries
- Real-time or streaming data processing capabilities

These exclusions represent deliberate scope boundaries informed by resource constraints, operational requirements, and the secure enclave deployment context, as detailed in subsequent sections.

### 1.6 Secure Enclave Context

The system operates within a secure enclave environment providing primary protection through physical isolation. This context fundamentally shapes the Phase 1 approach:

**Primary Protection Layer:** Physical isolation ensures that raw transaction data never leaves the secure enclave. Access is restricted to authorized personnel with appropriate clearances. Network isolation prevents external access, and data residency requirements ensure all processing occurs within the enclave.

**Secondary Protection Layer:** The SDC system provides additional protection against statistical disclosure risks from authorized users within the secure enclave. This secondary layer addresses the scenario where authorized analysts may attempt to infer individual card patterns from aggregated statistics.

**Implication for Method Selection:** The secure enclave context means that formal privacy guarantees (ε, δ-differential privacy) are not strictly necessary, as the primary threat model assumes authorized but potentially curious users rather than external adversaries. This enables a utility-first approach that prioritizes statistical accuracy while maintaining plausibility-based protection.

---

## 2. Data Characteristics and Risk Implications

### 2.1 Input Data Structure

The system processes financial transaction records with the following schema:

- **Card identifier:** Unique card number (direct identifier, removed before aggregation)
- **Transaction amount:** Monetary value of transaction (sensitive attribute)
- **Transaction date:** Temporal dimension enabling day and weekday extraction
- **Acceptor city:** Geographic dimension at city granularity
- **Merchant Category Code (MCC):** Categorical dimension classifying merchant type
- **Province:** Geographic dimension at province level (derived from city)

Each transaction record represents a single financial transaction. The system processes approximately 4.5 billion transaction records per month, representing a substantial volume requiring distributed processing infrastructure.

### 2.2 Data Granularity and Scale

The aggregation structure creates a multi-dimensional histogram with the following dimensions:

- **Province dimension:** Approximately 31 provinces (exact count depends on geographic coverage)
- **City dimension:** Approximately 1,500 cities (high cardinality)
- **MCC dimension:** Approximately 300 merchant category codes (moderate cardinality)
- **Day dimension:** 30 days per processing period (fixed)
- **Weekday dimension:** 7 days (Monday through Sunday, used for context-aware bounds)

The resulting histogram contains approximately 31 × 1,500 × 300 × 30 = 418.5 million potential cells. However, actual cell counts are substantially lower due to sparsity: many combinations of (province, city, MCC, day) contain zero transactions.

The sparsity pattern creates disclosure risk: cells with small counts (few cards, few transactions) are vulnerable to re-identification. A cell containing transactions from only one or two cards enables inference of individual card behavior.

### 2.3 Identifier Classification

**Direct Identifiers:** The card number functions as a direct identifier, enabling unique identification of individual cards. The system removes card numbers before output generation, but card-level patterns may still be inferable from aggregated statistics.

**Quasi-Identifiers:** The combination of (city, MCC, day, weekday) functions as a quasi-identifier. While no single dimension uniquely identifies a card, combinations create increasingly specific contexts. A card that transacts in a small city, at a specific MCC, on a specific day may be uniquely identifiable if few other cards share that pattern.

**Sensitive Attributes:** Transaction amounts represent sensitive attributes. Even without complete re-identification, disclosure of spending amounts for specific contexts (city, MCC, day) may reveal sensitive financial information about individual cards.

### 2.4 High-Cardinality Dimensions and Sparsity

The high cardinality of the city dimension (approximately 1,500 cities) combined with the MCC dimension (approximately 300 codes) creates a large combinatorial space. When combined with temporal dimensions (30 days, 7 weekdays), the resulting space contains millions of potential cells.

However, actual transaction patterns exhibit significant sparsity. Most cells contain zero transactions. Among non-zero cells, the distribution is highly skewed: a small number of cells contain large transaction volumes (major cities, common MCCs, peak days), while many cells contain small counts (small cities, rare MCCs, off-peak days).

This sparsity pattern creates disclosure risk through two mechanisms:

**Small Cell Vulnerability:** Cells with transaction counts below a threshold (e.g., 5 transactions) are vulnerable to re-identification. If a cell contains transactions from only one or two cards, those cards' patterns are directly observable.

**Uniqueness Risk:** Even cells with moderate counts may contain unique card patterns. A card that is the only contributor to a specific (city, MCC, day) combination is uniquely identifiable within that context.

### 2.5 Concentration Patterns

Transaction data exhibits concentration patterns that both mitigate and amplify disclosure risk:

**Geographic Concentration:** A small number of large cities account for a disproportionate share of transactions. This concentration reduces disclosure risk in large cities (many cards, high counts) but increases risk in small cities (few cards, low counts).

**MCC Concentration:** Common merchant categories (e.g., grocery stores, gas stations) have high transaction volumes across many cities and days. Rare MCCs (specialized merchants) have low volumes, creating small cells vulnerable to disclosure.

**Temporal Concentration:** Weekdays and weekends exhibit different transaction patterns. Some MCCs (e.g., restaurants) have higher weekend activity, while others (e.g., business services) have higher weekday activity. This temporal variation creates additional disclosure risk when combined with geographic and MCC dimensions.

### 2.6 Adversary Model and Assumptions

Phase 1 assumes the following adversary capabilities and knowledge:

**Adversary Access:** The adversary is an authorized user within the secure enclave with access to protected aggregated statistics. The adversary does not have direct access to raw transaction data (protected by physical isolation).

**Adversary Knowledge:** The adversary may have knowledge of:
- Province-level transaction totals (public data, treated as invariants)
- General transaction patterns (e.g., typical spending amounts per MCC)
- External data sources that may enable linkage (e.g., merchant locations, city populations)

**Adversary Capabilities:** The adversary can:
- Query aggregated statistics multiple times
- Perform statistical analysis on released statistics
- Combine information from multiple queries
- Use external data sources for linkage attacks

**Adversary Limitations:** The adversary cannot:
- Access raw transaction data directly
- Observe intermediate processing steps
- Control noise generation (random seed is fixed per execution)
- Access suppressed cells (suppression flags indicate missing data)

These assumptions shape the threat model and inform the design of protection mechanisms, as detailed in subsequent sections.

---

## 3. Problem Decomposition and Design Constraints

### 3.1 Problem Decomposition

The SDC problem is decomposed into three sub-problems:

**Risk Detection:** Identifying cells and patterns vulnerable to disclosure. This includes detecting small cells (low transaction counts, few unique cards), extreme outliers (unusually high transaction amounts that may indicate single-card dominance), and unique combinations of quasi-identifiers that enable re-identification.

**Transformation:** Applying protective mechanisms to reduce disclosure risk while preserving utility. This includes noise injection, scaling operations to maintain invariants, rounding to integer values, and suppression of highly vulnerable cells.

**Validation:** Verifying that protection mechanisms achieve their objectives without introducing unacceptable utility loss. This includes checking that province invariants are preserved exactly, that ratios remain within plausible bounds, and that no negative counts are produced.

Each sub-problem requires different algorithmic approaches and creates different trade-offs between protection and utility, as detailed in the methodology section.

### 3.2 Design Constraints

#### 3.2.1 Data Utility Preservation

The system must preserve statistical utility for downstream financial analysis. This constraint requires:

- **Ratio Preservation:** Derived statistics (average transaction amount, transactions per card) must remain within plausible ranges for each context (MCC, City, Weekday). Financial analysis relies on these ratios to understand spending patterns, merchant behavior, and temporal trends.

- **Distribution Integrity:** The distribution of transaction counts, amounts, and card counts across cells must remain interpretable. Extreme distortions that create implausible patterns (e.g., negative counts, impossible ratios) render the data unusable.

- **Province-Level Accuracy:** Province totals must be preserved exactly, as these represent public data used for validation and consistency checks. Financial institutions and regulatory bodies rely on accurate province-level statistics.

This utility constraint rules out approaches that introduce large, indiscriminate noise (as demonstrated in Census 2020 differential privacy implementation) or that create distribution irregularities incompatible with financial analysis.

#### 3.2.2 Scalability

The system must process approximately 4.5 billion transaction records per month within acceptable time windows. This constraint requires:

- **Distributed Processing:** All operations must scale horizontally across multiple compute nodes. Single-machine processing is infeasible at this scale.

- **Memory Efficiency:** Algorithms must operate on distributed data structures without requiring collection to driver memory. The histogram structure must remain distributed throughout processing.

- **Computational Efficiency:** Complex operations (e.g., optimization-based post-processing) that require iterative algorithms or large-scale matrix operations are infeasible at this scale.

This scalability constraint rules out approaches requiring:
- Large-scale optimization (e.g., non-negative least squares on full histogram)
- Machine learning model training on full dataset
- Complex iterative algorithms with multiple passes over data
- Validation services requiring manual review of queries

#### 3.2.3 Regulatory and Institutional Expectations

Province-level transaction totals are treated as public data, known from other sources and used for validation. This creates a hard constraint:

- **Province Invariants:** Province totals must be preserved exactly (0% error). No noise can be applied at the province level.

- **Consistency Requirements:** City-level statistics must sum exactly to province totals. This requirement creates a constraint on noise application: all noise must be applied at the city level, then scaled to match province totals.

This regulatory constraint fundamentally shapes the noise application strategy. Unlike approaches where noise can be applied at multiple geographic levels, all noise in Phase 1 must be injected at the city level, then adjusted to preserve province totals. This amplifies the distortion impact, as noted in the Census 2020 experience where similar constraints (state-level totals preserved exactly) forced all noise into lower geographic levels.

#### 3.2.4 Interpretability

Protected statistics must remain interpretable by financial analysts without specialized privacy expertise. This constraint requires:

- **Plausibility:** All output values must be plausible for their context. A restaurant in a small city cannot have 10,000 transactions in a single day. A gas station cannot have an average transaction amount of 1,000,000 currency units.

- **Transparency:** The protection mechanism should be explainable to stakeholders. Complex black-box approaches (e.g., neural network-based anonymization) create operational risk and reduce trust.

- **Reproducibility:** Results should be reproducible given the same input data and configuration. This requires deterministic components where possible and fixed random seeds for probabilistic components.

This interpretability constraint rules out approaches that create implausible outputs or that are difficult to explain to non-technical stakeholders.

#### 3.2.5 Resource Constraints

Phase 1 was developed under specific resource constraints that informed scope decisions:

- **Timeline:** Approximately six weeks of development time
- **Personnel:** Two researchers
- **Infrastructure:** Standard institutional infrastructure (not AWS-scale cloud resources)

These constraints must be understood in context. The U.S. Census 2020 differential privacy implementation required months of development work, large expert teams, and AWS-scale infrastructure. Comparable advanced anonymization systems (e.g., synthetic data generation with validation services) require significantly more time, personnel, and computational resources.

The resource constraints do not represent technical limitations but rather deliberate scope definition. Phase 1 delivers a stable, interpretable, and scalable foundation appropriate for the problem context, with clear pathways for extension in Phase 2 if additional capabilities are required.

### 3.3 Constraints as Enabling Factors

The constraints described above are not limitations but rather enabling factors that guide method selection:

- **Utility preservation** enables selection of targeted, context-aware approaches over indiscriminate noise injection
- **Scalability** enables selection of distributed, streaming-compatible approaches over complex optimization
- **Regulatory expectations** enable selection of approaches that preserve known invariants exactly
- **Interpretability** enables selection of transparent, explainable approaches over black-box methods
- **Resource constraints** enable selection of approaches that deliver value within operational timelines

The resulting methodology represents a deliberate engineering choice that balances these constraints, as detailed in the rationale section.

---

## 4. Methodology Overview

### 4.1 Core Transformation Mechanism

The Phase 1 SDC system applies multiplicative jitter to cell-level statistics. For a cell with original count c, the protected count is computed as:

**M(c) = c × (1 + η)**

where η is a noise factor drawn from a distribution centered at zero with standard deviation proportional to the noise level parameter (typically 15% relative noise).

This multiplicative approach preserves ratios naturally: if transaction count and total amount both receive the same multiplicative factor, the average transaction amount (amount/count) remains unchanged before clamping operations. This property is critical for maintaining statistical relationships in financial analysis.

The multiplicative mechanism contrasts with additive noise, which would distort ratios. For example, adding fixed noise to both count and amount would change the average transaction amount, creating implausible ratios that degrade utility.

### 4.2 Context-Aware Plausibility Bounds

Noise application is constrained by context-specific plausibility bounds computed from the data itself. For each (MCC, City, Weekday) combination, the system computes:

- **Lower bound:** 5th percentile of transaction counts observed in that context across all days
- **Upper bound:** 95th percentile of transaction counts observed in that context across all days

These bounds ensure that noisy values remain plausible for their context. A restaurant in a small city on a weekday cannot have transaction counts typical of a large city on a weekend. The bounds are data-driven, reflecting actual patterns in the transaction data rather than arbitrary global limits.

This context-aware approach aligns with the principle demonstrated in Census 2020 analysis: "When uncertainty is added to protect confidentiality—either through swapping or through targeted noise injection—that should not affect the total counts of population." In the transaction context, this principle extends to ensuring that noise creates plausible patterns rather than implausible outliers.

### 4.3 Province Invariants

Province-level transaction totals are preserved exactly (0% error). This requirement aligns with the Census 2020 principle that "Population size is by far the most important census statistic for small areas and should be reported accurately; revealing true population counts need not compromise anyone's privacy."

The invariant preservation mechanism operates as follows:

1. Province totals are computed from original data and stored as invariants (no noise applied)
2. Noise is applied to all city-level cells independently
3. City-level noisy values are scaled proportionally within each province to sum exactly to province totals
4. Controlled rounding maintains exact province totals while converting to integers

This mechanism ensures that province totals match public data exactly, while city-level statistics receive protection through noise and scaling operations.

### 4.4 Ratio Preservation

Two derived ratios are preserved within plausible bounds:

**Average Transaction Amount:** total_amount / transaction_count must remain within context-specific bounds (typically p5-p95 range for the context).

**Transactions Per Card:** transaction_count / unique_cards must remain within context-specific bounds.

These ratios are critical for financial analysis. Analysts use average transaction amounts to understand spending patterns per merchant category. Transactions per card ratios indicate card usage intensity. Distorted ratios would mislead analysis and reduce utility.

The system preserves ratios through proportional scaling: when transaction counts are adjusted to match province invariants, amounts and card counts are scaled proportionally to maintain ratios. If ratios fall outside plausible bounds after scaling, additional adjustments are applied to bring ratios within bounds while maintaining province invariants.

### 4.5 Preprocessing Steps

Before noise application, the system applies two preprocessing steps:

**Winsorization:** Transaction amounts are capped at the 99th percentile per MCC. This prevents extreme outliers (e.g., a single very large transaction) from dominating cell statistics. Winsorization is applied per MCC because different merchant categories have different typical transaction amounts (e.g., real estate transactions vs. coffee purchases).

**Bounded Contribution (K):** Each card is limited to K transactions per cell. This prevents a single card from dominating a cell's statistics. K is computed using a transaction-weighted percentile method that minimizes data loss while preventing outlier dominance.

These preprocessing steps reduce disclosure risk from outliers while preserving the majority of data for analysis.

### 4.6 Controlled Rounding

After noise application and scaling, values are rounded to integers (transaction counts, card counts, and amounts must be whole numbers). The rounding mechanism maintains province invariants exactly:

1. City-level values are rounded independently
2. Province totals are computed from rounded city values
3. Differences between rounded province totals and invariant province totals are distributed across cities using a controlled rounding algorithm
4. Final values are integers that sum exactly to province invariants

This controlled rounding ensures that integer constraints do not violate province invariant requirements.

### 4.7 Suppression

Cells with transaction counts below a threshold (typically 5) are suppressed. Suppression prevents disclosure in highly vulnerable cells where even small amounts of noise may not provide adequate protection. Suppressed cells are flagged in the output but their values are not released.

Suppression represents a targeted approach to disclosure control, focusing protection on the most vulnerable cells rather than applying uniform protection to all cells. This aligns with the Census 2020 principle that swapping (a targeted approach) "does minimal damage to accuracy" compared to indiscriminate noise injection.

### 4.8 Processing Sequence

The complete processing sequence operates as follows:

1. **Data Reading:** Raw transactions are loaded from Parquet format into distributed data structures
2. **Preprocessing:** Winsorization and bounded contribution are applied to individual transactions
3. **Aggregation:** Transactions are aggregated to histogram cells (province, city, MCC, day, weekday)
4. **Province Invariant Computation:** Province totals are computed and stored as exact invariants
5. **Context-Aware Bounds Computation:** Plausibility bounds are computed per (MCC, City, Weekday) context
6. **Noise Application:** Multiplicative jitter is applied to all three cell values (count, cards, amount) independently
7. **Bounds Clamping:** Noisy values are clamped to context-specific plausibility bounds
8. **Ratio Validation:** Derived ratios are checked against context-specific bounds and adjusted if necessary
9. **Scaling to Invariants:** All values are scaled proportionally within each province to match province totals exactly
10. **Re-validation:** Ratios are re-checked after scaling and adjusted if necessary
11. **Controlled Rounding:** Values are rounded to integers while maintaining province invariants
12. **Suppression:** Small cells are flagged for suppression
13. **Final Validation:** Province invariants, ratios, and consistency constraints are verified
14. **Output Generation:** Protected statistics are written to Parquet format

This sequence ensures that protection is applied systematically while maintaining utility through invariant preservation and ratio constraints.

---

## 5. Rationale for Method Selection

### 5.1 Evaluation Framework

The selection of the SDC approach for Phase 1 followed systematic evaluation of alternative disclosure control methods. Each alternative was assessed against the design constraints described in Section 3, with particular attention to utility preservation, scalability, and operational feasibility. The evaluation explicitly considered empirical evidence from large-scale implementations, particularly the U.S. Census 2020 differential privacy deployment, which provides documented lessons on utility-preservation challenges in disclosure control.

### 5.2 Alternative Methods Considered

#### 5.2.1 k-Anonymity Variants

k-anonymity and its variants (l-diversity, t-closeness) require that each record in a dataset be indistinguishable from at least k-1 other records on quasi-identifier attributes. These methods are designed for microdata (individual records) rather than aggregated statistics.

**Why Not Selected:** k-anonymity variants are fundamentally incompatible with the aggregated histogram structure used in Phase 1. The system operates on cell-level aggregates (transaction counts, card counts, amounts) rather than individual transaction records. Applying k-anonymity would require de-aggregation to individual records, which would:
- Increase disclosure risk by creating individual-level records
- Require complex aggregation logic to reconstruct cell-level statistics
- Create computational overhead disproportionate to protection benefits
- Violate the data minimization principle (creating individual records when only aggregates are needed)

The utility loss from k-anonymity would be substantial: generalization and suppression operations required to achieve k-anonymity would distort cell-level statistics beyond acceptable thresholds for financial analysis.

#### 5.2.2 Formal Differential Privacy

Differential privacy provides mathematical privacy guarantees through noise injection that is statistically independent of the underlying data. The mechanism adds calibrated noise to query results, with privacy parameters (ε, δ) quantifying the privacy-utility trade-off.

**Evaluation Process:** Differential privacy was actively evaluated, not ignored. The evaluation considered both theoretical properties and empirical evidence from large-scale deployments, particularly the U.S. Census 2020 implementation.

**Analytical Assessment:** Differential privacy requires noise that is statistically independent of the underlying data distribution. For high-frequency financial transaction data, this independence requirement creates fundamental utility challenges:

1. **Province-Level Invariant Constraint:** Province totals are public data and must be preserved exactly. This constraint forces all noise injection into a single aggregation step (city-level cells). Unlike scenarios where noise can be distributed across multiple geographic levels, all distortion accumulates at the city level, amplifying the impact on utility.

2. **Indiscriminate Noise Application:** As documented in analysis of Census 2020, differential privacy noise injection is "a blunt instrument that adds deliberate error to every statistic below the state level." The mechanism is "indiscriminate; unlike swapping, it does not target the most vulnerable respondents. Even the total population of New York City is perturbed. This is pointless: tabular data for large populations do not need disclosure control, because there are no cases with unique combinations of characteristics."

3. **Utility Degradation:** The Census Bureau evaluation found that the agency "never attempted to weigh realistic measures of disclosure risk under alternative disclosure control methods against the harm of producing an unreliable census." In the financial transaction context, similar utility degradation would occur: indiscriminate noise applied to all city-level cells would create distribution irregularities incompatible with downstream financial analysis.

4. **Targeted Protection Impossibility:** Unlike targeted approaches (such as swapping in census applications), differential privacy cannot selectively protect only vulnerable cells. All cells receive noise, regardless of their disclosure risk level. This creates unnecessary utility loss in cells that pose minimal disclosure risk (large cities, common MCCs, high transaction volumes).

**Application to Financial Context:** The province-level invariant constraint creates a similar problem to that observed in Census 2020. Province totals (analogous to state totals in census) are public and must be preserved exactly. All noise must be injected at the city level, then scaled to match province totals. This forced concentration of noise amplifies distortion, similar to the Census 2020 experience where state-level constraints forced noise into lower geographic levels.

Financial analysis requires reliable distributions for understanding spending patterns, merchant behavior, and temporal trends. The irregular patterns created by indiscriminate differential privacy noise would mislead analysis and reduce utility below acceptable thresholds.

**Operational Complexity:** Differential privacy implementations at scale (e.g., Census 2020) require:
- Months of development work
- Large expert teams with specialized privacy expertise
- AWS-scale infrastructure for validation and post-processing
- Complex validation services requiring manual review of researcher queries

The Census 2020 validation service strategy "has multiple flaws" and is "impractical" because the "Census Bureau lacks the resources to provide this validation service at scale." To provide service comparable to current usage, the Bureau "would have to validate hundreds of thousands of analyses per year." This operational complexity is disproportionate to Phase 1 objectives, given the secure enclave context where formal privacy guarantees are not strictly necessary.

**Decision Rationale:** The rejection of formal differential privacy represents a utility-preserving choice informed by empirical evidence from Census 2020, not a technical inability to implement differential privacy mechanisms. The decision balances the secure enclave context (where primary protection is physical isolation) against the utility requirements of financial analysis. The SDC approach provides targeted, context-aware protection that preserves utility while addressing disclosure risks appropriate to the threat model.

#### 5.2.3 Fine-Grained Noise and Learning-Based Methods

**Finer Temporal or Structural Granularity:** Introducing noise at finer granularity (e.g., hour-level, additional attribute dimensions) was considered but rejected because:

1. **Increased Exposure Surfaces:** Finer granularity creates more cells, increasing the number of potential disclosure points. While individual cells may receive more noise, the overall exposure surface increases.

2. **Paradoxical Risk Increase:** Finer granularity paradoxically increases disclosure risk while reducing interpretability. More cells mean more opportunities for linkage attacks combining information across cells. The increased complexity reduces analysts' ability to detect implausible patterns, potentially masking disclosure risks.

3. **Utility Degradation:** Finer granularity requires more noise to achieve equivalent protection levels, as noise must be calibrated to smaller cell sizes. This amplifies utility loss without proportional protection benefits.

**Machine Learning and Neural Network Approaches:** Synthetic data generation and neural network-based anonymization were evaluated but excluded due to:

1. **Extreme Data Volume:** Processing approximately 4.5 billion rows per month requires distributed processing. Machine learning approaches (particularly neural networks) require:
   - Large-scale GPU clusters for model training
   - Iterative training procedures with multiple passes over data
   - Model storage and versioning infrastructure
   - Hyperparameter tuning and validation procedures

2. **Operational Complexity:** As documented in Census 2020 synthetic data discussions, synthetic data "captures relationships between variables only if those relationships have been anticipated in advance and intentionally included in the models." Synthetic data is "poorly suited to studying unanticipated relationships, which impedes new discovery." The system "cannot capture all the ways in which interrelationships among variables can vary across subgroups."

3. **Validation Service Requirements:** Synthetic data approaches require validation services where researchers submit code for execution on true data. The Census 2020 analysis found this approach "impractical" and "expensive" at scale, requiring "hundreds of thousands of analyses per year" to match current usage. This operational burden is disproportionate to Phase 1 objectives.

4. **Resource Constraints:** Machine learning approaches require significantly more development time, specialized expertise, and computational resources than Phase 1 constraints allow. The six-week timeline and two-researcher team cannot support the development and operational overhead of machine learning-based approaches.

**Decision Rationale:** The exclusion of fine-grained noise and learning-based methods represents risk-aware and scope-appropriate decisions informed by Census 2020 synthetic data experience. These approaches would increase operational complexity and resource requirements without proportional utility or protection benefits for the Phase 1 threat model.

### 5.3 SDC Method Selection Rationale

The selected SDC approach represents a deliberate constraint demonstrating methodological maturity, not avoidance of complexity. The method:

1. **Preserves Utility:** Multiplicative jitter with context-aware bounds maintains statistical relationships while providing protection. Province invariants are preserved exactly, and ratios remain within plausible ranges.

2. **Targets Protection:** Unlike indiscriminate differential privacy, the SDC approach can focus protection on vulnerable cells (through suppression) while applying minimal distortion to low-risk cells (large cities, common MCCs).

3. **Scales Efficiently:** The approach uses distributed processing compatible with Spark infrastructure, avoiding complex optimization or iterative algorithms that would not scale to 4.5 billion rows per month.

4. **Remains Interpretable:** The mechanism is transparent and explainable to stakeholders, avoiding black-box approaches that create operational risk.

5. **Aligns with Constraints:** The method operates within resource constraints (six weeks, two researchers, standard infrastructure) while delivering a stable foundation appropriate for the problem context.

The selection demonstrates sound engineering judgment: choosing a method that balances protection, utility, scalability, and operational feasibility rather than pursuing theoretically optimal approaches that would exceed resource constraints or degrade utility below acceptable thresholds.

---

## 6. Implementation Logic (Conceptual)

### 6.1 Processing Sequence Overview

The Phase 1 implementation follows a deterministic sequence of transformations, with probabilistic components (noise generation) controlled through fixed random seeds to ensure reproducibility. The sequence introduces irreversibility at specific points, ensuring that original values cannot be reconstructed from protected outputs.

### 6.2 Detailed Processing Steps

**Step 1: Data Reading and Validation**

Raw transaction records are loaded from Parquet format into distributed Spark DataFrames. Schema validation ensures required fields (card number, amount, date, city, MCC) are present and properly typed. Geographic validation verifies that all cities map to valid provinces according to the geographic hierarchy.

This step is deterministic and reversible: original transaction records remain unchanged and could be reconstructed from intermediate outputs if needed.

**Step 2: Preprocessing - Winsorization**

Transaction amounts are capped at the 99th percentile per MCC. This operation is deterministic: the same input data produces the same winsorized amounts. The operation is partially irreversible: original amounts above the cap cannot be recovered from winsorized values, but this is intentional to prevent outlier dominance.

**Step 3: Preprocessing - Bounded Contribution**

Each card is limited to K transactions per cell, where K is computed using a transaction-weighted percentile method. Cards exceeding K transactions in a cell have excess transactions removed (deterministic selection based on transaction order).

This step is partially irreversible: removed transactions cannot be recovered, but this is intentional to prevent single-card dominance of cell statistics.

**Step 4: Aggregation**

Transactions are aggregated to histogram cells defined by (province, city, MCC, day, weekday). For each cell, three statistics are computed:
- Transaction count (number of transactions)
- Unique card count (number of distinct cards)
- Total amount (sum of transaction amounts)

This step is deterministic and reversible in principle (original transactions could be reconstructed if cell boundaries and aggregation logic are known), but in practice the aggregation creates information loss that prevents exact reconstruction.

**Step 5: Province Invariant Computation**

Province-level totals are computed by summing city-level statistics within each province. These totals are stored as exact invariants (no noise applied) and cached for use in subsequent scaling operations.

This step is deterministic. Province invariants represent public data and must be preserved exactly throughout processing.

**Step 6: Context-Aware Bounds Computation**

For each (MCC, City, Weekday) combination, plausibility bounds are computed from the data:
- Lower bound: 5th percentile of transaction counts across all days for that context
- Upper bound: 95th percentile of transaction counts across all days for that context

Similar bounds are computed for amounts and card counts. These bounds are deterministic: the same input data produces the same bounds.

**Step 7: Noise Application**

Multiplicative jitter is applied to all three cell values independently:
- Noisy count = original count × (1 + η_count)
- Noisy cards = original cards × (1 + η_cards)
- Noisy amount = original amount × (1 + η_amount)

Each η is drawn from a uniform distribution transformed to have mean zero and standard deviation equal to the noise level parameter (typically 15%). The random number generator uses a fixed seed (configurable, default 42) with different offsets for count, cards, and amount to ensure independence.

This step introduces irreversibility: given only the noisy values and noise parameters, original values cannot be exactly reconstructed. The noise is probabilistic but reproducible (same seed produces same noise sequence).

**Step 8: Bounds Clamping**

Noisy values are clamped to context-specific plausibility bounds computed in Step 6. Values below the lower bound are set to the lower bound; values above the upper bound are set to the upper bound.

This step is deterministic given the noisy values and bounds. Clamping introduces additional irreversibility by potentially removing information about the original value's relationship to bounds.

**Step 9: Ratio Validation**

Derived ratios are computed and checked against context-specific bounds:
- Average transaction amount = total_amount / transaction_count
- Transactions per card = transaction_count / unique_cards

If ratios fall outside plausible bounds, values are adjusted proportionally to bring ratios within bounds while maintaining relative relationships between count, cards, and amount.

This step is deterministic. Ratio adjustments introduce irreversibility by potentially changing values from their post-clamping state.

**Step 10: Scaling to Province Invariants**

All city-level values within each province are scaled proportionally to sum exactly to province invariants computed in Step 5. The scaling factor for province p is:

scale_factor_p = invariant_total_p / sum(noisy_city_values_p)

All three values (count, cards, amount) are scaled by the same factor within each province to preserve ratios.

This step is deterministic and introduces irreversibility: original noisy values cannot be recovered from scaled values without knowledge of the scaling factors, which depend on province-level aggregates.

**Step 11: Re-validation of Ratios**

After scaling, ratios are re-checked against bounds. If scaling caused ratios to fall outside bounds, additional adjustments are applied. These adjustments may create small deviations from exact province totals, which are corrected in the controlled rounding step.

This step is deterministic.

**Step 12: Controlled Rounding**

Values are rounded to integers (transaction counts, card counts, and amounts must be whole numbers). The rounding algorithm:
1. Rounds all city-level values independently
2. Computes province totals from rounded city values
3. Compares rounded province totals to invariant province totals
4. Distributes differences across cities using a controlled rounding algorithm that maintains integer constraints

This step is deterministic given the rounding algorithm specification. Rounding introduces irreversibility: original fractional values cannot be recovered from rounded integers.

**Step 13: Suppression**

Cells with transaction counts below a threshold (typically 5) are flagged for suppression. Suppressed cells have their values set to null or a sentinel value, and a suppression flag is set.

This step is deterministic. Suppression introduces irreversibility: original values for suppressed cells are not released.

**Step 14: Final Validation**

Province invariants are verified to be exact (0% error). Ratios are checked to ensure they remain within bounds. Consistency constraints are verified (e.g., if count is zero, cards and amount must be zero).

This step is deterministic validation only; no transformations are applied.

**Step 15: Output Generation**

Protected statistics are written to Parquet format, partitioned by province for efficient access. Output includes transaction counts, unique card counts, total amounts, derived ratios, and suppression flags.

This step is deterministic (data format conversion only).

### 6.3 Irreversibility Analysis

Irreversibility is introduced at multiple points in the processing sequence:

1. **Winsorization (Step 2):** Original amounts above caps cannot be recovered
2. **Bounded Contribution (Step 3):** Removed transactions cannot be recovered
3. **Noise Application (Step 7):** Original values cannot be exactly recovered from noisy values
4. **Bounds Clamping (Step 8):** Information about values outside bounds is lost
5. **Ratio Adjustments (Steps 9, 11):** Original post-clamping values may be modified
6. **Scaling (Step 10):** Original noisy values cannot be recovered without scaling factors
7. **Rounding (Step 12):** Original fractional values cannot be recovered
8. **Suppression (Step 13):** Original values for suppressed cells are not released

The cumulative effect of these irreversibility points ensures that original transaction-level data cannot be reconstructed from protected outputs, even with knowledge of the protection mechanism and parameters.

### 6.4 Deterministic vs Probabilistic Components

**Deterministic Components:**
- Data reading and validation
- Winsorization (given fixed percentile thresholds)
- Bounded contribution (given fixed K value)
- Aggregation
- Province invariant computation
- Context-aware bounds computation
- Bounds clamping
- Ratio validation and adjustment
- Scaling to invariants
- Controlled rounding
- Suppression
- Final validation
- Output generation

**Probabilistic Components:**
- Noise generation (Step 7): Uses random number generation with fixed seed for reproducibility

The system is designed to be reproducible: given the same input data and configuration parameters (including noise seed), the same protected output is produced. This reproducibility is critical for validation, debugging, and operational consistency.

### 6.5 Configuration Parameter Influence

Configuration parameters influence outcomes as follows:

**Noise Level:** Controls the standard deviation of multiplicative noise. Higher noise levels increase protection but decrease utility. Typical values range from 10% to 20% relative noise.

**Noise Seed:** Controls the random number sequence for noise generation. Different seeds produce different noise patterns but equivalent protection levels. Fixed seeds ensure reproducibility.

**Suppression Threshold:** Controls which cells are suppressed. Lower thresholds suppress fewer cells (better utility) but provide less protection for small cells. Typical values range from 3 to 10 transactions.

**Winsorization Percentile:** Controls the cap on transaction amounts. Higher percentiles (e.g., 99.5%) preserve more data but allow more extreme outliers. Typical values range from 95% to 99.9%.

**Bounded Contribution K:** Controls the maximum transactions per card per cell. Higher K values preserve more data but allow more single-card dominance. K is typically computed from data using percentile methods.

**Bounds Percentiles:** Control the range of plausibility bounds (typically 5th to 95th percentile). Narrower ranges provide stronger protection but may exclude valid patterns. Typical ranges are p5-p95 or p10-p90.

These parameters create a trade-off space between protection and utility. Phase 1 uses default values calibrated to balance these objectives, with configuration options allowing adjustment for specific operational requirements.

---

## 7. Validation and Correctness Assessment

### 7.1 Internal Consistency Checks

The system performs multiple internal consistency checks to verify correctness:

**Province Invariant Verification:** After each transformation step that could affect province totals (scaling, rounding), the system verifies that province-level aggregates match stored invariants exactly (0% error). Any deviation indicates a bug in the implementation and triggers an error.

**Ratio Bounds Verification:** After ratio adjustments and scaling operations, the system verifies that derived ratios (average transaction amount, transactions per card) remain within context-specific plausibility bounds. Ratios outside bounds indicate either a bug or an edge case requiring special handling.

**Logical Consistency Verification:** The system verifies that values satisfy logical constraints:
- If transaction count is zero, unique card count and total amount must be zero
- If transaction count is positive, unique card count must be at least 1 and at most equal to transaction count
- All values must be non-negative
- All values must be integers (after rounding)

Violations of these constraints indicate implementation errors and trigger corrections or errors.

**Consistency Across Transformations:** The system tracks values through transformation steps to ensure that operations (scaling, rounding, ratio adjustment) maintain expected relationships. For example, scaling should preserve ratios; rounding should maintain province totals.

### 7.2 Evidence of Risk Reduction

The system provides evidence of risk reduction through multiple mechanisms:

**Noise Magnitude:** The multiplicative jitter introduces relative error with expected magnitude equal to the noise level parameter (typically 15%). This noise makes individual card contributions to cell statistics indistinguishable, preventing exact inference of card-level patterns.

**Suppression Coverage:** The system reports the number and percentage of cells suppressed. Suppression eliminates disclosure risk for the most vulnerable cells (small transaction counts, few cards). Typical suppression rates range from 5% to 15% of cells, depending on data sparsity and threshold settings.

**Bounds Enforcement:** The context-aware bounds ensure that noisy values remain plausible for their context. This prevents creation of implausible outliers that might enable inference attacks through pattern recognition.

**Bounded Contribution:** The K limit on transactions per card per cell prevents single cards from dominating cell statistics. This reduces disclosure risk in cells where one card might otherwise be uniquely identifiable.

### 7.3 Utility Degradation Assessment

The system assesses utility degradation through multiple metrics:

**Relative Error Distribution:** The system computes the distribution of relative errors (|protected_value - original_value| / original_value) across all cells. This distribution quantifies the impact of noise and scaling operations on cell-level accuracy.

**Province-Level Accuracy:** Province totals are preserved exactly (0% error), ensuring that province-level analysis maintains full utility.

**Ratio Preservation:** The system tracks how often ratios remain within bounds after transformations. High preservation rates (typically >95%) indicate that statistical relationships are maintained.

**Suppression Impact:** The system reports the percentage of original transaction volume represented in suppressed cells. Low percentages (typically <1%) indicate that suppression affects few transactions while protecting vulnerable cells.

**Distribution Comparison:** The system can compare distributions of protected vs. original values to assess whether noise introduces systematic biases or distribution irregularities. This comparison helps identify cases where protection mechanisms degrade utility beyond acceptable thresholds.

### 7.4 Success Criteria

Phase 1 defines success criteria in measurable terms:

**Province Invariants:** 100% of provinces must have exact totals (0% error). Any province with non-zero error indicates a critical bug.

**Noise Magnitude:** The distribution of relative errors should have mean approximately equal to zero (no systematic bias) and standard deviation approximately equal to the noise level parameter (typically 15%). Deviations beyond 20% of the target indicate calibration issues.

**Ratio Bounds:** At least 95% of cells should have ratios (average transaction amount, transactions per card) within context-specific bounds after all transformations. Lower percentages indicate that ratio preservation mechanisms are not functioning correctly.

**No Negative Values:** 100% of cells must have non-negative values for all statistics. Any negative values indicate implementation errors.

**Logical Consistency:** 100% of cells must satisfy logical constraints (e.g., card count ≤ transaction count). Any violations indicate implementation errors.

**Suppression Coverage:** Suppression should cover cells representing less than 2% of total transaction volume. Higher coverage indicates that suppression thresholds may be too aggressive or that data has unusual sparsity patterns.

These success criteria are verified automatically during pipeline execution. Violations trigger errors or warnings, depending on severity.

### 7.5 Limitations of Validation Approach

The validation approach has several limitations:

**No Formal Privacy Proofs:** The system does not provide mathematical proofs of privacy guarantees (e.g., ε, δ-differential privacy). Validation focuses on plausibility and utility preservation rather than formal privacy bounds.

**Limited Adversary Modeling:** Validation assumes a specific adversary model (authorized user with access to aggregated statistics). The approach does not validate against all possible adversary capabilities or attack strategies.

**Statistical Validation Only:** Validation focuses on statistical properties (distributions, ratios, invariants) rather than attempting to prove that specific disclosure attacks are impossible. The approach assumes that plausibility-based protection is sufficient for the threat model.

**No Cross-Query Analysis:** Validation does not assess risks from combining information across multiple queries or releases. The approach assumes that each release is analyzed independently.

**Operational Validation Gaps:** Validation focuses on algorithmic correctness rather than operational security (e.g., access controls, audit logging, side-channel protection). These aspects are handled by secure enclave infrastructure.

These limitations are acceptable within Phase 1 scope, given the secure enclave context and utility-first objectives. Phase 2 may extend validation to include formal privacy proofs or enhanced adversary modeling if requirements evolve.

---

## 8. Identified Challenges and Mitigations

### 8.1 Data-Specific Challenges

**High-Cardinality Dimensions:** The combination of high-cardinality dimensions (1,500 cities, 300 MCCs, 30 days) creates a large combinatorial space (418.5 million potential cells). Most cells are empty (sparse data), but the large space creates computational challenges for bounds computation and validation.

**Mitigation:** The system computes bounds only for non-empty contexts (MCC, City, Weekday combinations that actually occur in the data). This reduces computational overhead from millions of potential contexts to thousands of actual contexts. Bounds are computed using distributed aggregation compatible with Spark infrastructure.

**Sparsity Patterns:** The high sparsity (most cells are empty) creates disclosure risk in non-empty cells, particularly those with small counts. Sparse data also creates challenges for bounds computation when few observations exist for a context.

**Mitigation:** The system uses percentile-based bounds that are robust to small sample sizes. For contexts with very few observations (fewer than 10), bounds default to conservative global bounds. Suppression eliminates the most vulnerable sparse cells (small counts).

**Outlier Transactions:** Extreme transaction amounts (e.g., very large purchases) can dominate cell statistics, creating disclosure risk if a single transaction is uniquely identifiable.

**Mitigation:** Winsorization caps transaction amounts at the 99th percentile per MCC, preventing extreme outliers from dominating statistics. Bounded contribution (K limit) prevents single cards from contributing too many transactions to a cell.

### 8.2 Edge Cases

**Small Cells:** Cells with transaction counts below the suppression threshold (typically 5) are highly vulnerable to disclosure. Even with noise, small cells may enable inference of individual card patterns.

**Mitigation:** Small cells are suppressed (values not released). The suppression threshold is calibrated to balance protection (higher threshold = more protection) and utility (higher threshold = more data loss). Typical thresholds range from 3 to 10 transactions.

**Zero Counts:** Cells with zero transactions create special cases for ratio computation and bounds checking. Zero counts must be handled consistently to avoid division-by-zero errors or implausible ratios.

**Mitigation:** The system enforces logical consistency: if count is zero, cards and amount must be zero. Ratio computations check for zero denominators before division. Zero cells are not suppressed (suppression applies only to small positive counts) but are handled explicitly in all operations.

**Province-Level Aggregation:** Province totals must be preserved exactly, creating constraints on city-level noise application. The scaling operation that enforces province invariants may create ratio violations that require additional adjustments.

**Mitigation:** The system applies scaling proportionally to preserve ratios, then re-validates ratios after scaling. If ratios fall outside bounds, additional adjustments are applied. The controlled rounding algorithm maintains province totals exactly while distributing rounding differences across cities.

### 8.3 Potential Failure Modes

**Numerical Instability in Scaling:** When province totals are very large and city-level values are very small, scaling operations may create numerical precision issues. Floating-point arithmetic may introduce small errors that accumulate.

**Mitigation:** The system uses double-precision floating-point arithmetic and verifies province totals after each scaling operation. Any deviations from exact totals are corrected in the controlled rounding step, which operates on integer values and maintains exact arithmetic.

**Ratio Bound Violations:** After scaling to match province invariants, ratios may fall outside plausible bounds. This can occur when province-level patterns differ significantly from city-level patterns (e.g., a province with mostly large cities but one very small city).

**Mitigation:** The system re-validates ratios after scaling and applies proportional adjustments to bring ratios within bounds. If adjustments create small deviations from province totals, these are corrected in controlled rounding. The bounds themselves are computed from data to reflect actual patterns, reducing the likelihood of violations.

**Invariant Mismatch After Rounding:** Rounding city-level values to integers may cause province totals to deviate from invariant totals. The rounding differences must be distributed across cities while maintaining integer constraints.

**Mitigation:** The controlled rounding algorithm computes rounding differences and distributes them across cities using a deterministic algorithm that maintains integer constraints. The algorithm prioritizes cities with values closest to rounding boundaries to minimize distortion.

**Context Bounds Insufficient:** For contexts with very few observations, percentile-based bounds may be too narrow or too wide, creating either utility loss (narrow bounds) or insufficient protection (wide bounds).

**Mitigation:** The system uses minimum sample size requirements for bounds computation. Contexts with fewer than 10 observations default to conservative global bounds. The bounds percentiles (typically 5th to 95th) are calibrated to balance protection and utility.

### 8.4 Residual Risks

Several risks remain unresolved in Phase 1:

**Very Small Cells After Suppression:** Cells just above the suppression threshold (e.g., 6-10 transactions) may still be vulnerable to disclosure if they contain few cards. The noise magnitude (typically 15%) may not provide sufficient protection for these cells.

**Mitigation:** The suppression threshold can be increased if operational experience reveals that small cells above the threshold pose disclosure risks. The threshold is configurable to allow adjustment based on data characteristics and risk assessment.

**Extreme Outliers Not Captured by Winsorization:** Winsorization at the 99th percentile may not capture all extreme outliers if the distribution has very long tails. A single extreme transaction in a small cell may still dominate statistics.

**Mitigation:** The bounded contribution (K limit) prevents single cards from contributing too many transactions to a cell, reducing the impact of extreme outliers. Operational monitoring can identify cases where winsorization thresholds need adjustment.

**Adversary with Perfect Knowledge of Province Totals:** An adversary with exact knowledge of province totals (public data) could potentially use this information to improve inference of city-level patterns, particularly in provinces with few cities.

**Mitigation:** The province totals are public by design (regulatory requirement), so this risk is inherent to the constraint. The noise and scaling mechanisms are designed to provide protection even when province totals are known. The context-aware bounds and suppression provide additional protection layers.

**Cross-Query Reconstruction:** An adversary combining information from multiple queries or releases could potentially reconstruct individual patterns through linear algebra techniques, similar to the Dinur-Nissim attack scenario.

**Mitigation:** Phase 1 does not explicitly protect against cross-query reconstruction. The secure enclave context assumes that query access is controlled and monitored. If cross-query risks become operational concerns, Phase 2 could implement query accounting or additional protection mechanisms.

These residual risks are documented and monitored. Operational experience will inform whether Phase 2 should address these risks through enhanced protection mechanisms or adjusted parameters.

---

## 9. Limitations and Phase 1 Boundaries

### 9.1 Explicit Limitations

Phase 1 explicitly does not protect against the following threats, with immediate contextualization:

**Formal Privacy Guarantees (No ε, δ Bounds):** The system does not provide mathematical privacy guarantees in the form of (ε, δ)-differential privacy bounds. This limitation is acceptable because:
- The secure enclave provides primary protection through physical isolation
- The threat model assumes authorized users rather than external adversaries
- Formal privacy guarantees would require utility degradation incompatible with financial analysis requirements (as demonstrated in Census 2020)
- The utility-first approach prioritizes statistical accuracy while maintaining plausibility-based protection appropriate to the threat model

**Reconstruction Attacks with Unlimited Queries:** The system does not explicitly protect against adversaries who can submit unlimited queries and combine results to reconstruct individual patterns (e.g., Dinur-Nissim attack scenario). This limitation is acceptable because:
- The secure enclave context includes operational controls on query access and monitoring
- Unlimited query scenarios are not part of the Phase 1 threat model
- Protection against unlimited queries would require formal differential privacy, which degrades utility below acceptable thresholds (as documented in Census 2020 analysis)
- If unlimited query risks become operational concerns, Phase 2 could implement query accounting or budget-based mechanisms

**Side-Channel Attacks:** The system does not protect against side-channel attacks (timing attacks, memory analysis, power consumption analysis). This limitation is acceptable because:
- Side-channel protection is the responsibility of secure enclave infrastructure, not the SDC system
- The secure enclave provides hardware-level isolation that mitigates side-channel risks
- SDC operates at the application layer and cannot address infrastructure-level side-channel vulnerabilities

**Direct Data Access:** The system does not protect against unauthorized direct access to raw transaction data. This limitation is acceptable because:
- Direct data access protection is provided by secure enclave physical isolation (primary protection layer)
- The SDC system operates on data that has already passed through access controls
- Application-layer SDC cannot prevent infrastructure-level access violations

### 9.2 Scenarios with Elevated Disclosure Risk

Several scenarios may exhibit elevated disclosure risk even with Phase 1 protection:

**Very Small Cells After Suppression:** Cells with transaction counts just above the suppression threshold (e.g., 6-10 transactions) may contain few cards, making individual card patterns inferable even with noise. This risk is contextualized by:
- The suppression threshold is configurable and can be increased if operational experience reveals risks
- The noise magnitude (typically 15%) provides protection proportional to cell size
- Very small cells represent a small fraction of total transaction volume (<2%), limiting overall risk exposure

**Extreme Outliers Not Captured by Winsorization:** Transaction amounts at the 99th percentile may still be extreme enough to enable inference in small cells. This risk is contextualized by:
- Winsorization percentile is configurable (typically 99%, can be adjusted to 99.5% or 99.9% if needed)
- Bounded contribution (K limit) prevents single cards from dominating cell statistics
- Context-aware bounds provide additional protection by ensuring noisy values remain plausible

**Adversary with Perfect Knowledge of Province Totals:** An adversary with exact knowledge of province totals (public data) could use this information to improve inference of city-level patterns. This risk is contextualized by:
- Province totals are public by regulatory requirement, so this constraint is inherent to the problem
- The noise and scaling mechanisms are designed to provide protection even when province totals are known
- The context-aware bounds and suppression provide additional protection layers beyond province-level constraints

**Sparse Contexts with Few Observations:** Contexts (MCC, City, Weekday combinations) with very few observations may have bounds that are too narrow (limiting utility) or too wide (limiting protection). This risk is contextualized by:
- Minimum sample size requirements default to conservative global bounds for sparse contexts
- Sparse contexts represent a small fraction of total data volume
- Operational monitoring can identify contexts requiring special handling

### 9.3 Resource Context and Benchmark Calibration

Phase 1 limitations must be understood within resource constraints:

**Development Timeline:** Approximately six weeks of development time limits the scope of features that can be implemented and validated. This constraint is contextualized by:
- The U.S. Census 2020 differential privacy implementation required months of development work
- Comparable advanced anonymization systems require significantly more time for development and validation
- Phase 1 delivers a stable foundation that can be extended in Phase 2 if additional capabilities are required

**Personnel:** Two researchers limit the depth of evaluation and implementation of complex methods. This constraint is contextualized by:
- Census 2020 required large expert teams with specialized privacy expertise
- Machine learning-based approaches require teams with specialized ML and infrastructure expertise
- The two-researcher team size is appropriate for the scope and complexity of Phase 1 objectives

**Infrastructure:** Standard institutional infrastructure (not AWS-scale cloud resources) limits the computational approaches that can be deployed. This constraint is contextualized by:
- Census 2020 required AWS-scale infrastructure for validation and post-processing
- Machine learning approaches require large-scale GPU clusters
- The standard infrastructure is sufficient for the distributed Spark-based approach used in Phase 1

These resource constraints do not represent technical insufficiencies but rather deliberate scope boundaries that enable Phase 1 to deliver value within operational timelines. The limitations are boundaries of deliberate scope, not technical failures.

### 9.4 Utility-Preservation Arguments

Several limitations are acceptable because addressing them would degrade utility below acceptable thresholds:

**No Formal Privacy Guarantees:** Providing formal (ε, δ)-differential privacy guarantees would require noise that degrades utility below acceptable thresholds for financial analysis, as demonstrated in Census 2020 where the approach "never attempted to weigh realistic measures of disclosure risk under alternative disclosure control methods against the harm of producing an unreliable census."

**No Fine-Grained Noise:** Introducing noise at finer temporal or structural granularity would increase utility loss without proportional protection benefits, as finer granularity requires more noise to achieve equivalent protection levels.

**No Machine Learning Approaches:** Machine learning-based anonymization would require validation services that are "impractical" and "expensive" at scale, as documented in Census 2020 analysis where the Bureau "lacks the resources to provide this validation service at scale" and would require "hundreds of thousands of analyses per year" to match current usage.

These utility-preservation arguments demonstrate that limitations are not oversights but deliberate choices that balance protection and utility within operational constraints.

---

## 10. Transition to Phase 2

### 10.1 Gaps Identified During Phase 1

Phase 1 has identified several potential gaps that may require attention in Phase 2, depending on operational experience and evolving requirements:

**Formal Privacy Guarantees:** If regulatory requirements evolve to mandate formal privacy guarantees (ε, δ-differential privacy), Phase 2 would need to implement differential privacy mechanisms. This would require:
- Re-evaluation of utility-preservation strategies given the utility degradation demonstrated in Census 2020
- Development of post-processing techniques to improve utility while maintaining formal guarantees
- Operational infrastructure for privacy budget accounting and query management

**Enhanced Validation Mechanisms:** If operational experience reveals that current validation is insufficient for specific use cases, Phase 2 could implement:
- Additional consistency checks beyond province invariants and ratio bounds
- Cross-query analysis to detect potential reconstruction attacks
- Enhanced monitoring and alerting for unusual patterns that may indicate disclosure risks

**Performance Optimizations:** If scale requirements increase beyond current capacity (approximately 4.5 billion rows per month), Phase 2 could implement:
- Additional Spark optimizations for very large datasets
- Caching strategies for frequently accessed intermediate results
- Parallel processing optimizations for bounds computation and validation

**Extended Threat Model Coverage:** If adversary capabilities evolve or new attack vectors are identified, Phase 2 could implement:
- Protection against cross-query reconstruction attacks
- Enhanced protection for very small cells above suppression threshold
- Adaptive noise calibration based on cell vulnerability assessment

### 10.2 Methodological Extensions

Phase 2 could extend the methodology in several directions:

**Additional Post-Processing Techniques:** If utility analysis reveals specific degradation patterns, Phase 2 could implement:
- Optimization-based post-processing to improve utility while maintaining protection
- Machine learning-based utility enhancement (if resource constraints allow)
- Advanced rounding algorithms that better preserve statistical properties

**Enhanced Validation Frameworks:** If operational monitoring indicates gaps in current validation, Phase 2 could implement:
- Formal verification of protection mechanisms
- Adversary modeling and attack simulation
- Utility degradation prediction and optimization

### 10.3 How Phase 1 Enables Phase 2

Phase 1 outputs enable Phase 2 development in several ways:

**Stable Foundation:** Phase 1 delivers a stable, interpretable, and scalable foundation that Phase 2 can build upon. The core pipeline architecture, data structures, and processing patterns are established and validated.

**Operational Patterns:** Phase 1 establishes operational patterns (configuration management, validation procedures, monitoring approaches) that Phase 2 can extend rather than replace.

**Infrastructure:** Phase 1 establishes the distributed Spark infrastructure and data formats that Phase 2 can leverage. Extensions can be integrated into the existing pipeline rather than requiring complete reimplementation.

**Lessons Learned:** Phase 1 operational experience provides lessons about:
- Which protection mechanisms are most effective
- Which utility degradation patterns are most problematic
- Which validation approaches are most valuable
- Which resource constraints are most limiting

These lessons inform Phase 2 priorities and design decisions.

### 10.4 How Phase 1 Constrains Phase 2

Phase 1 design decisions create constraints that Phase 2 must work within:

**Technical Debt:** Design decisions (e.g., province invariant preservation, context-aware bounds structure) create technical debt if requirements change. For example, if province totals are no longer required to be exact, the scaling mechanism could be simplified, but Phase 1's implementation assumes exact preservation.

**Path Dependencies:** Resource investments in Phase 1 (Spark infrastructure, data formats, validation procedures) create path dependencies. Phase 2 extensions should leverage existing infrastructure rather than requiring complete replacement, limiting the scope of methodological changes that can be implemented efficiently.

**Operational Expectations:** Phase 1 establishes operational expectations (processing time, output formats, validation procedures) that Phase 2 should maintain for consistency. Significant changes to these expectations would require operational retraining and process updates.

These constraints are not limitations but rather design decisions that create a stable foundation. Phase 2 can extend Phase 1 within these constraints, or can make breaking changes if requirements justify the operational cost.

### 10.5 Phase 1 as Foundation

Phase 1 should be understood as a foundation that enables informed Phase 2 decisions, not as incomplete work. The methodology represents a deliberate engineering choice that:

- Delivers value within resource constraints (six weeks, two researchers, standard infrastructure)
- Balances protection and utility within operational requirements
- Establishes patterns and infrastructure that enable efficient Phase 2 development
- Provides lessons learned that inform Phase 2 priorities

Phase 2 can extend Phase 1 capabilities if requirements evolve, but Phase 1 itself represents a complete, defensible solution to the Phase 1 problem statement within the stated constraints.

---

## Conclusion

Phase 1 of the Statistical Disclosure Control system represents a deliberate engineering choice that balances protection, utility, scalability, and operational feasibility. The methodology prioritizes utility preservation while providing secondary protection against statistical disclosure risks, complementing the primary protection layer of physical isolation provided by secure enclave infrastructure.

The approach demonstrates methodological maturity through:
- Systematic evaluation of alternatives informed by empirical evidence (Census 2020 experience)
- Deliberate constraint definition that enables focused development within resource limits
- Transparent, interpretable mechanisms that maintain stakeholder trust
- Scalable architecture that processes 4.5 billion transactions per month efficiently

The system is not simplistic but deliberately constrained. It is not avoiding complexity but making informed trade-offs. It delivers a stable, interpretable, and scalable foundation appropriate for the problem context, with clear pathways for extension in Phase 2 if additional capabilities are required.

The methodology represents a rational, defensible, and proportionate solution given real-world constraints, informed by sound engineering judgment and empirical evidence from large-scale disclosure control implementations.

---

**Document End**

