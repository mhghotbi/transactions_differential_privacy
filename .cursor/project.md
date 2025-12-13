# Project Overview

## Project Name
Transaction Statistical Disclosure Control (SDC) System

## Purpose
A production-ready Statistical Disclosure Control system for financial transaction data designed for secure enclave deployment. The system processes billions of transactions using Apache Spark and applies context-aware plausibility-based noise to protect individual privacy while maximizing statistical utility.

## Goals
1. **Privacy Protection**: Provide utility-first SDC protection as secondary layer (primary: secure enclave physical isolation)
2. **Scalability**: Process billions of transactions efficiently using distributed computing (Spark)
3. **Utility Preservation**: Maintain statistical utility through context-aware noise and ratio preservation
4. **Production Readiness**: Robust error handling, validation, and monitoring capabilities
5. **Compliance**: Align with privacy regulations and secure enclave deployment requirements

## Key Stakeholders
- **Data Governance Team**: Privacy policy and compliance oversight
- **Data Scientists**: End users consuming protected statistics
- **Engineering Team**: System development and maintenance
- **Security Team**: Security review and secure enclave deployment
- **Secure Enclave Operators**: Physical isolation and infrastructure management

## Success Criteria
- ✅ Process 10B+ transactions per month
- ✅ Maintain province invariants exactly (public data)
- ✅ Preserve statistical relationships (ratios, averages)
- ✅ Apply context-aware plausibility bounds per (MCC, City, Weekday)
- ✅ Complete processing within acceptable time windows
- ✅ Pass SDC validation checks (no negative counts, plausible ratios)

## Current Status
- **Version**: 3.0.0
- **Status**: Production-ready (SDC-focused)
- **Primary Use Case**: Monthly transaction statistics release in secure enclave
- **Deployment**: Spark cluster (YARN/Kubernetes) in secure enclave
- **Approach**: Utility-first SDC (not formal DP)

## Architecture Approach

### SDC (Statistical Disclosure Control)
- **Primary Protection**: Secure enclave physical isolation
- **Secondary Protection**: Context-aware plausibility-based noise
- **Noise Mechanism**: Multiplicative jitter `M(c) = c × (1 + η)`
- **Context-Aware**: Bounds computed per (MCC, City, Weekday) stratum
- **Invariants**: Province totals preserved exactly (no noise)

### Key Features
- Multiplicative jitter preserves ratios naturally
- Data-driven plausibility bounds per context
- Province count invariants maintained exactly
- Bounded contribution (K) using transaction-weighted percentile
- Per-MCC winsorization for transaction amounts
- Controlled rounding with ratio preservation

## Related Documentation
- [README.md](../README.md) - Quick start guide
- [CODE_EXPLANATION.md](../CODE_EXPLANATION.md) - Technical deep-dive
- [TUTORIAL.md](../TUTORIAL.md) - Step-by-step usage (if exists)

## Legacy DP Code
- DP modules (budget.py, primitives.py, sensitivity.py) are kept for potential future use
- Currently not active - system uses SDC approach
- Can be re-enabled if formal DP guarantees are needed

