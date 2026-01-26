"""Standalone tests for billing tier lookup fix (SENTRY-3B06).

This file can be run directly without installing the full sentry_relay package.
"""

import sys
import os

# Add the py directory to the path to import billing modules directly
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'py'))

# Import the billing modules directly
from sentry_relay.billing.exceptions import TierNotFound
from sentry_relay.billing.models import (
    BillingMetricHistory,
    PlanDefinition,
    PlanTier,
    get_plan_definition,
)
from sentry_relay.billing.serializers import (
    BillingHistorySerializer,
    migrate_historical_tiers,
)


def test_get_tier_exact_match():
    """Test exact tier lookup with matching quantity."""
    plan = PlanDefinition(
        name="test_plan",
        tiers=[
            PlanTier(quantity=10000, unit_price=0.001),
            PlanTier(quantity=100000, unit_price=0.0005),
        ]
    )
    
    tier = plan.get_tier(10000, strict=False)
    assert tier is not None
    assert tier.quantity == 10000
    assert tier.unit_price == 0.001
    print("✓ test_get_tier_exact_match passed")


def test_get_tier_no_match_non_strict():
    """Test tier lookup with no match in non-strict mode."""
    plan = PlanDefinition(
        name="test_plan",
        tiers=[
            PlanTier(quantity=100000, unit_price=0.0005),
        ]
    )
    
    tier = plan.get_tier(10000, strict=False)
    assert tier is None
    print("✓ test_get_tier_no_match_non_strict passed")


def test_get_tier_no_match_strict_raises():
    """Test tier lookup with no match in strict mode raises TierNotFound."""
    plan = PlanDefinition(
        name="test_plan",
        tiers=[
            PlanTier(quantity=100000, unit_price=0.0005),
        ]
    )
    
    try:
        plan.get_tier(10000, strict=True)
        assert False, "Should have raised TierNotFound"
    except TierNotFound as e:
        assert e.quantity == 10000
        assert "test_plan" in str(e)
    print("✓ test_get_tier_no_match_strict_raises passed")


def test_get_closest_tier():
    """Test closest tier matching for historical quantities."""
    plan = PlanDefinition(
        name="test_plan",
        tiers=[
            PlanTier(quantity=10000, unit_price=0.001),
            PlanTier(quantity=100000, unit_price=0.0005),
            PlanTier(quantity=1000000, unit_price=0.0003),
        ]
    )
    
    # Test quantity between tiers
    tier = plan.get_closest_tier(50000)
    assert tier is not None
    assert tier.quantity == 10000  # Closest tier <= 50000
    
    # Test quantity larger than all tiers
    tier = plan.get_closest_tier(5000000)
    assert tier is not None
    assert tier.quantity == 1000000
    
    # Test quantity smaller than all tiers
    tier = plan.get_closest_tier(5000)
    assert tier is None
    print("✓ test_get_closest_tier passed")


def test_calculate_cost_with_historical_price():
    """Test cost calculation using stored historical tier price."""
    history = BillingMetricHistory(
        id=1,
        organization_id="org-123",
        plan_name="am1_sponsored",
        metric_type="transactions",
        reserved_quantity=10000,  # Old tier that doesn't exist anymore
        actual_usage=8000,
        period_start="2024-01-01",
        period_end="2024-01-31",
        historical_tier_price=0.0008,  # Historical price
    )
    
    plan = PlanDefinition(
        name="am1_sponsored",
        tiers=[
            PlanTier(quantity=10000000, unit_price=0.00026),  # Only current tier
        ]
    )
    
    cost = history.calculate_cost(plan)
    assert cost == 8000 * 0.0008  # Uses historical price
    print("✓ test_calculate_cost_with_historical_price passed")


def test_calculate_cost_with_exact_tier_match():
    """Test cost calculation with exact tier match."""
    history = BillingMetricHistory(
        id=2,
        organization_id="org-456",
        plan_name="am1_sponsored",
        metric_type="transactions",
        reserved_quantity=10000000,
        actual_usage=5000000,
        period_start="2024-02-01",
        period_end="2024-02-29",
    )
    
    plan = PlanDefinition(
        name="am1_sponsored",
        tiers=[
            PlanTier(quantity=10000000, unit_price=0.00026),
        ]
    )
    
    cost = history.calculate_cost(plan)
    assert cost == 5000000 * 0.00026
    print("✓ test_calculate_cost_with_exact_tier_match passed")


def test_serialize_with_exact_tier_match():
    """Test serialization when tier exists in current plan."""
    history = BillingMetricHistory(
        id=1,
        organization_id="org-123",
        plan_name="am1_sponsored_attachments",
        metric_type="attachments",
        reserved_quantity=1000000000,
        actual_usage=500000000,
        period_start="2024-01-01",
        period_end="2024-01-31",
    )
    
    serializer = BillingHistorySerializer()
    data = serializer.serialize(history)
    
    assert data["id"] == 1
    assert data["reserved_quantity"] == 1000000000
    assert data["tier"]["tier_match"] == "exact"
    assert data["tier"]["is_historical"] is False
    assert data["tier"]["quantity"] == 1000000000
    print("✓ test_serialize_with_exact_tier_match passed")


def test_serialize_with_historical_tier_data():
    """Test serialization using stored historical tier data.
    
    This is the main fix for SENTRY-3B06 - when historical tier data is stored,
    it should be used instead of attempting lookup in current plan.
    """
    history = BillingMetricHistory(
        id=2,
        organization_id="org-456",
        plan_name="am1_sponsored",
        metric_type="transactions",
        reserved_quantity=10000,  # Old tier not in current plan
        actual_usage=8000,
        period_start="2023-06-01",
        period_end="2023-06-30",
        historical_tier_price=0.0008,
        historical_tier_name="10K transactions",
    )
    
    serializer = BillingHistorySerializer()
    data = serializer.serialize(history)
    
    assert data["tier"]["tier_match"] == "historical_data"
    assert data["tier"]["is_historical"] is True
    assert data["tier"]["unit_price"] == 0.0008
    assert data["tier"]["name"] == "10K transactions"
    assert data["cost"] == 8000 * 0.0008
    print("✓ test_serialize_with_historical_tier_data passed")


def test_sentry_3b06_scenario():
    """Test the exact issue: 1GB attachments tier lookup.
    
    Scenario:
    1. Customer has sponsored plan subscription with am1_sponsored_attachments
    2. Historical data shows reserved_quantity=1000000000 (1GB)
    3. Current plan still has this tier
    4. Should serialize without error
    """
    history = BillingMetricHistory(
        id=100,
        organization_id="org-sponsored-123",
        plan_name="am1_sponsored_attachments",
        metric_type="attachments",
        reserved_quantity=1000000000,  # 1GB
        actual_usage=800000000,  # 800MB used
        period_start="2023-12-01",
        period_end="2023-12-31",
    )
    
    serializer = BillingHistorySerializer()
    data = serializer.serialize(history)
    
    # Should serialize successfully
    assert data["id"] == 100
    assert data["reserved_quantity"] == 1000000000
    assert data["tier"]["quantity"] == 1000000000
    assert data["tier"]["tier_match"] == "exact"
    assert data["cost"] > 0
    print("✓ test_sentry_3b06_scenario passed")


def test_sentry_3b06_historical_tier_scenario():
    """Test scenario where old tier no longer exists in plan.
    
    Scenario:
    1. Customer had subscription with 10K transactions tier
    2. Plan was updated to only support 10M transactions tier
    3. Historical data still references 10K tier
    4. Should serialize gracefully using historical data
    """
    # Simulate old tier data stored in database
    history = BillingMetricHistory(
        id=101,
        organization_id="org-old-tier-456",
        plan_name="am1_sponsored",
        metric_type="transactions",
        reserved_quantity=10000,  # 10K - no longer exists in current plan
        actual_usage=9000,
        period_start="2023-01-01",
        period_end="2023-01-31",
        historical_tier_price=0.00080,  # Old price stored
        historical_tier_name="10K transactions (legacy)",
    )
    
    serializer = BillingHistorySerializer()
    data = serializer.serialize(history)
    
    # Should serialize successfully using historical data
    assert data["id"] == 101
    assert data["tier"]["tier_match"] == "historical_data"
    assert data["tier"]["is_historical"] is True
    assert data["tier"]["unit_price"] == 0.00080
    assert data["cost"] == 9000 * 0.00080
    print("✓ test_sentry_3b06_historical_tier_scenario passed")


def run_all_tests():
    """Run all tests."""
    print("\n" + "=" * 70)
    print("Running billing tier lookup tests (SENTRY-3B06)")
    print("=" * 70 + "\n")
    
    tests = [
        test_get_tier_exact_match,
        test_get_tier_no_match_non_strict,
        test_get_tier_no_match_strict_raises,
        test_get_closest_tier,
        test_calculate_cost_with_historical_price,
        test_calculate_cost_with_exact_tier_match,
        test_serialize_with_exact_tier_match,
        test_serialize_with_historical_tier_data,
        test_sentry_3b06_scenario,
        test_sentry_3b06_historical_tier_scenario,
    ]
    
    passed = 0
    failed = 0
    
    for test in tests:
        try:
            test()
            passed += 1
        except AssertionError as e:
            print(f"✗ {test.__name__} failed: {e}")
            failed += 1
        except Exception as e:
            print(f"✗ {test.__name__} error: {e}")
            failed += 1
    
    print("\n" + "=" * 70)
    print(f"Test Results: {passed} passed, {failed} failed")
    print("=" * 70 + "\n")
    
    return failed == 0


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
