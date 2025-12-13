#!/usr/bin/env python3
"""
Unit Tests - No Spark Required
==============================
Tests core functionality without needing Spark or Java.

Run with:
    cd census_dp
    python tests/test_no_spark.py
"""

import sys
import os

# Add parent to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))


def test_config():
    """Test configuration loading and validation."""
    print("Testing Config...")
    
    from core.config import Config, PrivacyConfig, DataConfig
    from fractions import Fraction
    
    # Test default config
    config = Config()
    assert config.privacy.total_rho == Fraction(1, 4), f"Expected 1/4, got {config.privacy.total_rho}"
    assert config.privacy.delta == 1e-10
    assert sum(config.privacy.geographic_split.values()) == 1.0
    assert sum(config.privacy.query_split.values()) == 1.0
    
    # Test validation
    config.data.input_path = "test.csv"
    config.data.output_path = "output/"
    config.validate()
    
    print("  ✓ Config OK")
    return True


def test_budget():
    """Test privacy budget calculations."""
    print("Testing Budget...")
    
    from core.budget import Budget, BudgetAllocator
    from fractions import Fraction
    
    budget = Budget(
        total_rho=Fraction(1, 1),
        delta=1e-10,
        geographic_split={"province": 0.2, "city": 0.8},
        query_split={
            "transaction_count": 0.25,
            "unique_cards": 0.25,
            "unique_acceptors": 0.25,
            "total_amount": 0.25
        }
    )
    
    # Test geo level budget
    province_budget = budget.get_geo_level_budget("province")
    city_budget = budget.get_geo_level_budget("city")
    assert float(province_budget) == 0.2
    assert float(city_budget) == 0.8
    
    # Test query budget
    query_budget = budget.get_query_budget("transaction_count", "city")
    expected = 1.0 * 0.8 * 0.25  # total * geo * query
    assert abs(float(query_budget) - expected) < 1e-6
    
    # Test sigma computation
    sigma = budget.compute_sigma_for_query("transaction_count", "city")
    assert sigma > 0
    assert sigma < float('inf')
    
    # Test epsilon conversion
    epsilon = budget.total_epsilon
    assert epsilon > 0
    
    print(f"  ✓ Budget OK (epsilon={epsilon:.4f})")
    return True


def test_primitives():
    """Test DP primitives (Discrete Gaussian)."""
    print("Testing Primitives...")
    
    from core.primitives import (
        discrete_gaussian_vector,
        compute_discrete_gaussian_variance,
        DiscreteGaussianMechanism,
        floorsqrt,
        bernoulli_exp_scalar,
        get_rng
    )
    from fractions import Fraction
    import numpy as np
    
    # Test floorsqrt
    assert floorsqrt(4, 1) == 2
    assert floorsqrt(5, 1) == 2
    assert floorsqrt(9, 1) == 3
    assert floorsqrt(1, 4) == 0  # sqrt(0.25) = 0.5, floor = 0
    
    # Test Bernoulli exp
    rng = get_rng()
    # Should return 0 or 1
    result = bernoulli_exp_scalar((1, 2), rng)
    assert result in [0, 1]
    
    # Test discrete gaussian sampling
    samples = discrete_gaussian_vector(Fraction(10, 1), size=100)
    assert len(samples) == 100
    assert all(isinstance(s, int) for s in samples)
    
    # Test variance computation
    variance = compute_discrete_gaussian_variance(Fraction(10, 1))
    assert abs(variance - 10.0) < 0.5  # Should be close to sigma^2
    
    # Test mechanism
    true_answer = np.array([100, 200, 300])
    mechanism = DiscreteGaussianMechanism(
        true_answer=true_answer,
        sigma_sq=Fraction(10, 1)
    )
    assert mechanism.protected_answer.shape == true_answer.shape
    assert mechanism.variance > 0
    
    print("  ✓ Primitives OK")
    return True


def test_geography():
    """Test geography loading."""
    print("Testing Geography...")
    
    from schema.geography import Geography, Province, City
    
    # Check if city_province.csv exists
    csv_path = "data/city_province.csv"
    if not os.path.exists(csv_path):
        print(f"  ⚠ Skipped (city_province.csv not found)")
        return True
    
    geo = Geography.from_csv(csv_path)
    
    assert geo.num_provinces > 0
    assert geo.num_cities > 0
    assert len(geo.provinces) == geo.num_provinces
    assert len(geo.cities) == geo.num_cities
    
    # Test city to province mapping
    cities = geo.city_names
    if cities:
        city = cities[0]
        province_code = geo.get_province_for_city(city)
        assert province_code is not None
    
    print(f"  ✓ Geography OK ({geo.num_provinces} provinces, {geo.num_cities} cities)")
    return True


def test_histogram():
    """Test histogram structure."""
    print("Testing Histogram...")
    
    from schema.histogram import TransactionHistogram
    import numpy as np
    
    # Create small histogram
    hist = TransactionHistogram(
        province_dim=3,
        city_dim=10,
        mcc_dim=5,
        day_dim=30
    )
    
    assert hist.shape == (3, 10, 5, 30)
    assert hist.total_cells == 3 * 10 * 5 * 30
    assert hist.non_zero_cells == 0
    
    # Set some values
    hist.set_value(0, 0, 0, 0, 'transaction_count', 100)
    hist.set_value(0, 0, 0, 0, 'unique_cards', 50)
    hist.set_value(0, 0, 0, 0, 'unique_acceptors', 10)
    hist.set_value(0, 0, 0, 0, 'total_amount', 1000000)
    
    assert hist.non_zero_cells == 1
    assert hist.get_value(0, 0, 0, 0, 'transaction_count') == 100
    
    # Test aggregation
    province_totals = hist.aggregate_to_province('transaction_count')
    assert province_totals.shape == (3,)
    assert province_totals[0] == 100
    
    # Test copy
    hist2 = hist.copy()
    assert hist2.get_value(0, 0, 0, 0, 'transaction_count') == 100
    
    # Test to_records
    records = hist.to_records()
    assert len(records) == 1
    assert records[0]['transaction_count'] == 100
    
    print("  ✓ Histogram OK")
    return True


# test_queries() removed - queries module deleted as unused
    return True


def test_sample_data_generator():
    """Test sample data generation (small scale)."""
    print("Testing Sample Data Generator...")
    
    import tempfile
    import csv
    
    csv_path = "data/city_province.csv"
    if not os.path.exists(csv_path):
        print(f"  ⚠ Skipped (city_province.csv not found)")
        return True
    
    from examples.generate_sample_data import (
        load_cities_from_csv,
        generate_card_number,
        generate_acceptor_id,
        generate_transaction_id,
        generate_amount,
        generate_sample_data
    )
    
    # Test helpers
    card = generate_card_number()
    assert len(card) == 16
    assert card.isdigit()
    
    acceptor = generate_acceptor_id()
    assert acceptor.startswith("ACC")
    
    txn_id = generate_transaction_id()
    assert txn_id.startswith("TXN")
    
    amount = generate_amount()
    assert amount > 0
    
    # Test city loading
    cities = load_cities_from_csv(csv_path)
    assert len(cities) > 0
    
    # Generate small sample
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        temp_path = f.name
    
    try:
        generate_sample_data(
            num_records=100,
            output_path=temp_path,
            city_province_path=csv_path,
            num_days=7,
            num_cards=20,
            num_acceptors=10,
            seed=42
        )
        
        # Verify output
        with open(temp_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        
        assert len(rows) == 100
        assert 'transaction_id' in rows[0]
        assert 'amount' in rows[0]
        assert 'acceptor_city' in rows[0]
        
    finally:
        os.unlink(temp_path)
    
    print("  ✓ Sample Data Generator OK")
    return True


def test_nnls_postprocessor():
    """Test NNLS post-processor for consistency enforcement."""
    print("Testing NNLS Post-Processor...")
    
    from core.postprocessing import NNLSPostProcessor
    import numpy as np
    
    processor = NNLSPostProcessor(method='simple')
    
    # Test 1: Basic adjustment
    noisy = np.array([100.5, 200.3, 150.2])
    target = 450  # Slightly different from sum
    
    adjusted = processor.solve(noisy, target)
    
    assert abs(adjusted.sum() - target) < 1e-6, f"Sum mismatch: {adjusted.sum()} vs {target}"
    assert all(adjusted >= 0), "Non-negativity violated"
    
    # Test 2: Edge case - all zeros
    zeros = np.array([0.0, 0.0, 0.0])
    adjusted = processor.solve(zeros, 100)
    assert abs(adjusted.sum() - 100) < 1e-6
    
    # Test 3: Hierarchical constraints
    from core.postprocessing import ConsistencyConstraint
    
    noisy = np.array([50.0, 30.0, 20.0, 80.0, 70.0, 50.0])  # 6 cities
    constraints = [
        ConsistencyConstraint([0, 1, 2], 100, "Province A"),  # Cities 0-2 sum to 100
        ConsistencyConstraint([3, 4, 5], 200, "Province B"),  # Cities 3-5 sum to 200
    ]
    
    adjusted = processor.solve_hierarchical(noisy, constraints)
    
    assert abs(adjusted[:3].sum() - 100) < 1e-6, "Province A constraint violated"
    assert abs(adjusted[3:].sum() - 200) < 1e-6, "Province B constraint violated"
    
    print("  ✓ NNLS Post-Processor OK")
    return True


def test_controlled_rounder():
    """Test Census-style controlled rounder."""
    print("Testing Controlled Rounder...")
    
    from core.rounder import CensusControlledRounder, DeterministicRounder
    import numpy as np
    
    # Test randomized rounder
    rounder = CensusControlledRounder(seed=42)
    
    # Test 1: Basic rounding
    fractional = np.array([10.3, 20.7, 15.5, 8.5])
    target = 55  # sum of rounded should be 55
    
    result = rounder.round(fractional, target)
    
    assert result.values.sum() == target, f"Sum mismatch: {result.values.sum()} vs {target}"
    assert all(result.values >= 0), "Non-negativity violated"
    assert all(result.values == result.values.astype(int)), "Not integers"
    
    # Test 2: Bounds check (floor <= result <= ceil)
    floors = np.floor(fractional).astype(int)
    ceils = np.ceil(fractional).astype(int)
    
    for i in range(len(fractional)):
        assert floors[i] <= result.values[i] <= ceils[i], \
            f"Bounds violated at {i}: {floors[i]} <= {result.values[i]} <= {ceils[i]}"
    
    # Test 3: Deterministic rounder
    det_rounder = DeterministicRounder()
    result2 = det_rounder.round(fractional, target)
    
    assert result2.values.sum() == target
    
    # Test 4: Multiple runs should be statistically unbiased
    rounder_stats = CensusControlledRounder(seed=None)  # Random
    fractional_test = np.array([5.3, 4.7])
    
    sums = []
    for _ in range(100):
        r = rounder_stats.round(fractional_test, 10)
        sums.append(r.values[0])
    
    # First value should round up ~30% of the time (since frac part is 0.3)
    round_up_rate = sum(1 for s in sums if s == 6) / 100
    assert 0.1 < round_up_rate < 0.5, f"Unexpected round-up rate: {round_up_rate}"
    
    print("  ✓ Controlled Rounder OK")
    return True


def test_invariant_manager():
    """Test invariant manager for Census-style invariants."""
    print("Testing Invariant Manager...")
    
    from core.invariants import (
        InvariantManager, InvariantLevel, TemporalLevel
    )
    import numpy as np
    
    # Test 1: Default invariants
    manager = InvariantManager()
    
    # National monthly should be invariant
    assert manager.is_invariant(InvariantLevel.NATIONAL, TemporalLevel.MONTHLY)
    assert manager.is_invariant(InvariantLevel.PROVINCE, TemporalLevel.MONTHLY)
    
    # City and daily should get noise
    assert not manager.is_invariant(InvariantLevel.CITY, TemporalLevel.DAILY)
    
    # Test 2: should_add_noise
    assert not manager.should_add_noise('national', 'monthly')
    assert not manager.should_add_noise('province', 'monthly')
    assert manager.should_add_noise('city', 'daily')
    assert manager.should_add_noise('province', 'daily')  # Daily province gets noise
    
    # Test 3: Compute invariants from numpy
    data = {
        'transaction_count': np.array([
            [[10, 20, 30], [15, 25, 35]],  # Province 0: 2 cities, 3 days
            [[40, 50, 60], [45, 55, 65]],  # Province 1: 2 cities, 3 days
        ])
    }
    province_mapping = {'Province_A': [0], 'Province_B': [1]}
    
    # Manual calculation:
    # Province A: (10+20+30) + (15+25+35) = 60 + 75 = 135
    # Province B: (40+50+60) + (45+55+65) = 150 + 165 = 315
    # National: 135 + 315 = 450
    
    invariants = manager.compute_invariants_from_numpy(data, province_mapping)
    
    assert invariants.national_monthly.get('transaction_count') == 450
    
    # Test 4: Summary
    summary = manager.summary()
    assert 'national' in summary.lower()
    assert 'province' in summary.lower()
    
    print("  ✓ Invariant Manager OK")
    return True


def test_invariant_enforcer():
    """Test invariant enforcer for consistency."""
    print("Testing Invariant Enforcer...")
    
    from core.invariants import InvariantManager, InvariantEnforcer
    import numpy as np
    
    manager = InvariantManager()
    enforcer = InvariantEnforcer(manager)
    
    # Test temporal consistency
    daily_values = np.array([100.5, 120.3, 110.2, 95.0])  # 4 days
    monthly_total = 425.0
    
    adjusted = enforcer.enforce_temporal(daily_values, monthly_total, round_to_int=False)
    
    assert abs(adjusted.sum() - monthly_total) < 1e-6, "Monthly sum not enforced"
    assert all(adjusted >= 0), "Non-negativity violated"
    
    # Test with rounding
    adjusted_int = enforcer.enforce_temporal(daily_values, monthly_total, round_to_int=True)
    
    assert adjusted_int.sum() == int(monthly_total), "Integer sum not enforced"
    assert all(adjusted_int == adjusted_int.astype(int)), "Not integers"
    
    # Test geographic consistency
    city_values = np.array([50.3, 30.7, 20.0])
    province_total = 100.0
    
    adjusted_geo = enforcer.enforce_geographic(city_values, province_total, round_to_int=False)
    
    assert abs(adjusted_geo.sum() - province_total) < 1e-6, "Province sum not enforced"
    
    print("  ✓ Invariant Enforcer OK")
    return True


def test_bounded_contribution():
    """Test bounded contribution calculator."""
    print("Testing Bounded Contribution...")
    
    from core.bounded_contribution import BoundedContributionCalculator
    import numpy as np
    
    # Test 1: IQR method
    calculator = BoundedContributionCalculator(method='iqr', iqr_multiplier=1.5)
    
    # Simulate contributions: most cards have 1-3 tx, few outliers have 50+
    contributions = np.array([1, 1, 1, 1, 2, 2, 2, 3, 3, 3, 5, 5, 10, 50, 100])
    
    k = calculator.compute_k_from_numpy(contributions)
    
    # Q1=1, Q3=5, IQR=4, upper=5+1.5*4=11 → K=11
    assert k > 0, "K should be positive"
    assert k < 50, "K should exclude extreme outliers"
    
    # Test 2: Fixed method
    calculator_fixed = BoundedContributionCalculator(method='fixed', fixed_k=5)
    k_fixed = calculator_fixed.compute_k_from_numpy(contributions)
    
    assert k_fixed == 5, f"Fixed K should be 5, got {k_fixed}"
    
    # Test 3: Percentile method
    calculator_pct = BoundedContributionCalculator(method='percentile', percentile=95.0)
    k_pct = calculator_pct.compute_k_from_numpy(contributions)
    
    assert k_pct > 0, "Percentile K should be positive"
    
    # Test 4: Stats are captured
    assert 'q1' in calculator.stats
    assert 'q3' in calculator.stats
    assert 'iqr' in calculator.stats
    
    print("  ✓ Bounded Contribution OK")
    return True


def test_config_bounded_contribution():
    """Test config loading for bounded contribution settings."""
    print("Testing Config Bounded Contribution...")
    
    from core.config import PrivacyConfig
    
    config = PrivacyConfig()
    
    # Test default values
    assert config.contribution_bound_method == 'iqr'
    assert config.contribution_bound_iqr_multiplier == 1.5
    assert config.contribution_bound_fixed == 5
    assert config.contribution_bound_percentile == 99.0
    assert config.computed_contribution_bound is None
    
    # Test validation
    config.contribution_bound_method = 'iqr'
    config.validate()  # Should pass
    
    config.contribution_bound_method = 'percentile'
    config.validate()  # Should pass
    
    config.contribution_bound_method = 'fixed'
    config.validate()  # Should pass
    
    # Test invalid method
    try:
        config.contribution_bound_method = 'invalid'
        config.validate()
        assert False, "Should have raised ValueError"
    except ValueError:
        pass  # Expected
    
    print("  ✓ Config Bounded Contribution OK")
    return True


def test_confidence_calculator():
    """Test confidence interval calculations."""
    print("Testing Confidence Calculator...")
    
    from core.confidence import (
        ConfidenceCalculator,
        VarianceEstimator,
        sigma_to_confidence_summary,
        Z_SCORES
    )
    import math
    
    # Test 1: Basic z-scores
    calc = ConfidenceCalculator(default_confidence_level=0.90)
    
    assert abs(calc.get_z_score(0.90) - 1.645) < 0.01, "Z-score for 90% incorrect"
    assert abs(calc.get_z_score(0.95) - 1.960) < 0.01, "Z-score for 95% incorrect"
    assert abs(calc.get_z_score(0.99) - 2.576) < 0.01, "Z-score for 99% incorrect"
    
    # Test 2: Margin of error
    sigma = 10.0
    moe_90 = calc.compute_margin_of_error(sigma, 0.90)
    expected_moe = 1.645 * sigma
    assert abs(moe_90 - expected_moe) < 0.1, f"MOE incorrect: {moe_90} vs {expected_moe}"
    
    # Test 3: Confidence interval
    value = 100.0
    ci_lower, ci_upper = calc.compute_confidence_interval(value, sigma, 0.90)
    
    assert ci_lower < value, "Lower bound should be below value"
    assert ci_upper > value, "Upper bound should be above value"
    assert abs((ci_upper - ci_lower) - 2 * moe_90) < 0.1, "CI width incorrect"
    
    # Test 4: Relative MOE
    rel_moe = calc.compute_relative_moe(value, sigma, 0.90)
    expected_rel = moe_90 / value
    assert abs(rel_moe - expected_rel) < 0.01, "Relative MOE incorrect"
    
    # Test 5: Relative MOE with zero value
    rel_moe_zero = calc.compute_relative_moe(0, sigma, 0.90)
    assert rel_moe_zero == float('inf'), "Zero value should give infinite relative MOE"
    
    # Test 6: Variance estimator
    var_est = VarianceEstimator()
    variance = var_est.compute_dp_variance(sigma)
    assert abs(variance - sigma**2) < 0.01, "Variance calculation incorrect"
    
    # Test 7: Summary function
    summary = sigma_to_confidence_summary(sigma, value)
    assert 'sigma' in summary
    assert 'moe_90' in summary
    assert 'variance' in summary
    assert summary['sigma'] == sigma
    
    print("  ✓ Confidence Calculator OK")
    return True


def test_suppression_manager():
    """Test suppression functionality (without Spark)."""
    print("Testing Suppression Manager (logic only)...")
    
    # Just test that the classes can be imported and configured
    from core.suppression import SuppressionManager, ComplementarySuppression
    
    # Test 1: Basic initialization
    mgr = SuppressionManager(threshold=10, method='flag')
    assert mgr.threshold == 10
    assert mgr.method == 'flag'
    
    # Test 2: Invalid threshold
    try:
        mgr_invalid = SuppressionManager(threshold=-1)
        assert False, "Should have raised ValueError"
    except ValueError:
        pass  # Expected
    
    # Test 3: Invalid method
    try:
        mgr_invalid = SuppressionManager(method='invalid')
        assert False, "Should have raised ValueError"
    except ValueError:
        pass  # Expected
    
    # Test 4: Different methods
    mgr_flag = SuppressionManager(threshold=10, method='flag')
    mgr_null = SuppressionManager(threshold=10, method='null')
    mgr_value = SuppressionManager(threshold=10, method='value', sentinel_value=-999)
    
    assert mgr_flag.method == 'flag'
    assert mgr_null.method == 'null'
    assert mgr_value.method == 'value'
    assert mgr_value.sentinel_value == -999
    
    # Test 5: Zero threshold (no suppression)
    mgr_zero = SuppressionManager(threshold=0)
    assert mgr_zero.threshold == 0
    
    print("  ✓ Suppression Manager OK")
    return True


def test_global_sensitivity():
    """Test global sensitivity calculations (without Spark)."""
    print("Testing Global Sensitivity (logic only)...")
    
    import math
    
    # Test sensitivity calculation formulas
    # L2 sensitivity = sqrt(M) * K
    # Where M = max cells per individual, K = contribution bound
    
    M = 100  # Max cells a card appears in
    K = 5    # Contribution bound per cell
    W = 1000000  # Winsorize cap
    
    # Transaction count: sqrt(M) * K
    tx_sens = math.sqrt(M) * K
    expected_tx = 50.0
    assert abs(tx_sens - expected_tx) < 0.01, f"Transaction sensitivity: {tx_sens} vs {expected_tx}"
    
    # Unique cards: sqrt(M) * 1
    cards_sens = math.sqrt(M) * 1
    expected_cards = 10.0
    assert abs(cards_sens - expected_cards) < 0.01, f"Cards sensitivity: {cards_sens} vs {expected_cards}"
    
    # Total amount: sqrt(M) * K * W
    amount_sens = math.sqrt(M) * K * W
    expected_amount = 10.0 * 5 * 1000000  # 50,000,000
    assert abs(amount_sens - expected_amount) < 1, f"Amount sensitivity: {amount_sens} vs {expected_amount}"
    
    # Test with local sensitivity (M=1)
    M_local = 1
    tx_sens_local = math.sqrt(M_local) * K
    assert tx_sens_local == K, "Local sensitivity should equal K"
    
    print("  ✓ Global Sensitivity OK")
    return True


def test_config_new_options():
    """Test new config options for Census 2020 compliance."""
    print("Testing Config New Options...")
    
    from core.config import PrivacyConfig
    from fractions import Fraction
    
    config = PrivacyConfig()
    
    # Test suppression defaults
    assert config.suppression_threshold == 10
    assert config.suppression_method == 'flag'
    assert config.suppression_sentinel == -1
    
    # Test confidence interval defaults
    assert config.confidence_levels == [0.90]
    assert config.include_relative_moe == True
    
    # Test sensitivity defaults
    assert config.sensitivity_method == 'global'
    assert config.fixed_max_cells_per_card == 100
    
    # Test default rho (should be 0.25 = 1/4)
    assert config.total_rho == Fraction(1, 4), f"Default rho should be 1/4, got {config.total_rho}"
    
    # Test validation with new options
    config.validate()  # Should pass with defaults
    
    # Test invalid suppression threshold
    config.suppression_threshold = -5
    try:
        config.validate()
        assert False, "Should have raised ValueError for negative threshold"
    except ValueError:
        pass  # Expected
    
    config.suppression_threshold = 10  # Reset
    
    # Test invalid suppression method
    config.suppression_method = 'invalid_method'
    try:
        config.validate()
        assert False, "Should have raised ValueError for invalid method"
    except ValueError:
        pass  # Expected
    
    config.suppression_method = 'flag'  # Reset
    
    # Test invalid sensitivity method
    config.sensitivity_method = 'invalid_sens'
    try:
        config.validate()
        assert False, "Should have raised ValueError for invalid sensitivity method"
    except ValueError:
        pass  # Expected
    
    config.sensitivity_method = 'global'  # Reset
    
    # Test invalid confidence level
    config.confidence_levels = [1.5]  # Invalid (>1)
    try:
        config.validate()
        assert False, "Should have raised ValueError for invalid confidence level"
    except ValueError:
        pass  # Expected
    
    print("  ✓ Config New Options OK")
    return True


def run_all_tests():
    """Run all tests."""
    print("=" * 60)
    print("Transaction DP System - Unit Tests (No Spark)")
    print("=" * 60)
    print()
    
    tests = [
        test_config,
        test_budget,
        test_primitives,
        test_geography,
        test_histogram,
        # test_queries removed - queries module deleted as unused
        test_sample_data_generator,
        test_nnls_postprocessor,
        test_controlled_rounder,
        test_invariant_manager,
        test_invariant_enforcer,
        test_bounded_contribution,
        test_config_bounded_contribution,
        # New Census 2020 compliance tests
        test_confidence_calculator,
        test_suppression_manager,
        test_global_sensitivity,
        test_config_new_options,
    ]
    
    passed = 0
    failed = 0
    
    for test_func in tests:
        try:
            if test_func():
                passed += 1
        except Exception as e:
            failed += 1
            print(f"  ✗ FAILED: {e}")
            import traceback
            traceback.print_exc()
    
    print()
    print("=" * 60)
    print(f"Results: {passed} passed, {failed} failed")
    print("=" * 60)
    
    return failed == 0


if __name__ == '__main__':
    success = run_all_tests()
    sys.exit(0 if success else 1)

