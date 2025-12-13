# Development Checklists

Checklists for common development tasks in the Transaction SDC System.

---

## Pre-Development Checklist

Before starting work on a new feature or bug fix:

- [ ] Understand the SDC approach (utility-first, not formal DP)
- [ ] Review relevant documentation (architecture.md, decisions.md)
- [ ] Check existing code for similar functionality
- [ ] Understand secure enclave context and constraints
- [ ] Identify affected components
- [ ] Plan testing strategy

---

## Code Review Checklist

Before submitting code for review:

### General
- [ ] Code follows PEP 8 style guide
- [ ] Type hints added where appropriate
- [ ] Docstrings added to public functions/classes
- [ ] No hardcoded paths or values
- [ ] Error handling appropriate
- [ ] Logging at appropriate levels

### SDC-Specific
- [ ] Province invariants preserved exactly (if modifying aggregation/noise)
- [ ] Context-aware bounds used (not global bounds)
- [ ] Ratios preserved (avg_amount, tx_per_card)
- [ ] Multiplicative jitter used (not additive)
- [ ] Bounded contribution (K) applied correctly
- [ ] No raw transaction data in logs

### Spark-Specific
- [ ] Spark DataFrames used (not Pandas for large datasets)
- [ ] No unnecessary data collection to driver
- [ ] Broadcast variables used for small lookup tables
- [ ] Appropriate partition count configured
- [ ] Existing Spark session reused (not created new)

### Configuration
- [ ] Configuration validated before use
- [ ] File existence checked before reading
- [ ] Paths configurable (not hardcoded)

### Testing
- [ ] Unit tests added/updated
- [ ] SDC validation tests added/updated (if modifying SDC mechanisms)
- [ ] Tests pass locally
- [ ] Edge cases covered

---

## SDC Mechanism Checklist

When modifying SDC mechanisms (noise, bounds, invariants):

- [ ] Province invariants preserved exactly
- [ ] Context-aware bounds computed per (MCC, City, Weekday)
- [ ] Multiplicative jitter preserves ratios
- [ ] Noise clamped to plausibility bounds
- [ ] Bounded contribution (K) applied correctly
- [ ] Ratios validated after noise (avg_amount, tx_per_card)
- [ ] No negative counts produced
- [ ] Suppression thresholds applied correctly
- [ ] Seed produces reproducible results

---

## Pipeline Execution Checklist

Before running the pipeline:

- [ ] Configuration file exists and is valid
- [ ] Input data exists and is accessible
- [ ] Output directory exists or can be created
- [ ] Spark session configured correctly
- [ ] Required files exist (city_province.csv, etc.)
- [ ] Sufficient disk space for output
- [ ] Sufficient memory for processing

After running the pipeline:

- [ ] Pipeline completed successfully
- [ ] Output files created
- [ ] Output schema matches expected
- [ ] Province invariants verified (exact totals)
- [ ] No negative counts in output
- [ ] Ratios plausible (avg_amount, tx_per_card)
- [ ] Suppression flags set correctly
- [ ] Logs reviewed for errors/warnings

---

## Testing Checklist

Before running tests:

- [ ] Test data available
- [ ] Test configuration valid
- [ ] Dependencies installed
- [ ] Spark available (for integration tests)

After running tests:

- [ ] All tests pass
- [ ] Test coverage acceptable (>80% for unit tests)
- [ ] SDC validation tests pass
- [ ] Performance tests pass (if applicable)
- [ ] No test warnings or errors

---

## Documentation Checklist

When updating documentation:

- [ ] README.md updated (if user-facing changes)
- [ ] CODE_EXPLANATION.md updated (if technical changes)
- [ ] Architecture.md updated (if architecture changes)
- [ ] Decisions.md updated (if new decisions made)
- [ ] Glossary.md updated (if new terms added)
- [ ] Code comments/docstrings updated
- [ ] Examples updated (if API changes)

---

## Deployment Checklist

Before deploying to secure enclave:

### Security
- [ ] No raw transaction data in logs
- [ ] Input data validated
- [ ] Output validated before release
- [ ] Suppression thresholds configured
- [ ] Access controls configured
- [ ] Audit logging enabled
- [ ] Dependencies scanned for vulnerabilities

### Configuration
- [ ] Configuration file validated
- [ ] Paths configured correctly
- [ ] Noise levels appropriate
- [ ] Suppression thresholds appropriate
- [ ] Bounded contribution method configured

### Infrastructure
- [ ] Secure enclave accessible
- [ ] Spark cluster configured
- [ ] Sufficient resources available
- [ ] Monitoring configured
- [ ] Backup procedures in place

### Testing
- [ ] Tests pass in secure enclave environment
- [ ] Integration tests pass
- [ ] SDC validation tests pass
- [ ] Performance tests pass

### Documentation
- [ ] Deployment procedures documented
- [ ] Configuration documented
- [ ] Incident response plan documented
- [ ] Security procedures documented

---

## Bug Fix Checklist

When fixing a bug:

- [ ] Bug reproduced locally
- [ ] Root cause identified
- [ ] Fix implemented
- [ ] Tests added/updated to prevent regression
- [ ] Fix verified locally
- [ ] Code reviewed
- [ ] Documentation updated (if needed)

---

## Feature Addition Checklist

When adding a new feature:

- [ ] Feature requirements understood
- [ ] Design reviewed (architecture.md, decisions.md)
- [ ] Implementation plan created
- [ ] Code implemented
- [ ] Tests added
- [ ] Documentation updated
- [ ] Code reviewed
- [ ] Feature tested end-to-end

---

## Performance Optimization Checklist

When optimizing performance:

- [ ] Performance bottleneck identified
- [ ] Optimization approach planned
- [ ] Optimization implemented
- [ ] Performance improvement verified
- [ ] No functionality regressions
- [ ] Tests still pass
- [ ] Documentation updated (if needed)

---

## Configuration Change Checklist

When changing configuration:

- [ ] Configuration change documented
- [ ] Default values updated (if applicable)
- [ ] Validation rules updated (if needed)
- [ ] Configuration file updated
- [ ] Tests updated (if needed)
- [ ] Documentation updated

---

## Code Cleanup Checklist

When cleaning up code:

- [ ] Dead code removed
- [ ] Unused imports removed
- [ ] Code formatted (black, autopep8, etc.)
- [ ] Comments updated/removed
- [ ] Documentation updated
- [ ] Tests still pass

---

## Emergency Fix Checklist

For urgent production fixes:

- [ ] Issue identified and understood
- [ ] Fix implemented and tested locally
- [ ] Fix reviewed (at least one reviewer)
- [ ] Fix deployed to secure enclave
- [ ] Fix verified in production
- [ ] Post-mortem scheduled
- [ ] Proper fix planned (if temporary fix)

---

## Monthly Maintenance Checklist

Monthly maintenance tasks:

- [ ] Dependencies updated (if needed)
- [ ] Security patches applied
- [ ] Performance metrics reviewed
- [ ] Error logs reviewed
- [ ] Documentation reviewed
- [ ] Tests reviewed and updated
- [ ] Configuration reviewed

---

## Quick Reference

### Most Common Checks
1. Province invariants preserved exactly
2. Context-aware bounds used (not global)
3. Ratios preserved (avg_amount, tx_per_card)
4. No raw transaction data in logs
5. Spark DataFrames used (not Pandas for large datasets)
6. Configuration validated before use
7. Tests pass

### Before Every Commit
- [ ] Code follows style guide
- [ ] Tests pass
- [ ] No raw data in logs
- [ ] Province invariants preserved (if modifying aggregation/noise)

### Before Every PR
- [ ] All checklists completed
- [ ] Code reviewed
- [ ] Documentation updated
- [ ] Tests pass
- [ ] SDC validation passes

---

## Customizing Checklists

These checklists are starting points. Customize for your team:
- Add team-specific checks
- Remove irrelevant checks
- Add project-specific checks
- Update as project evolves

