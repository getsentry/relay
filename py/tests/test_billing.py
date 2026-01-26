"""Tests for billing tier lookup fix.

Tests for SENTRY-3B06: Graceful handling of historical billing data
with tier quantities that no longer exist in current plan definitions.
"""

import pytest
from sentry_relay.billing import (
    BillingMetricHistory,
    PlanDefinition,
    PlanTier,
    TierNotFound,
    BillingHistorySerializer,
)
from sentry_relay.billing.serializers import migrate_historical_tiers
from sentry_relay.billing.models import get_plan_definition


class TestPlanTierLookup:
    """Test tier lookup functionality."""
    
    def test_get_tier_exact_match(self):
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
    
    def test_get_tier_no_match_non_strict(self):
        """Test tier lookup with no match in non-strict mode."""
        plan = PlanDefinition(
            name="test_plan",
            tiers=[
                PlanTier(quantity=100000, unit_price=0.0005),
            ]
        )
        
        tier = plan.get_tier(10000, strict=False)
        assert tier is None
    
    def test_get_tier_no_match_strict_raises(self):
        """Test tier lookup with no match in strict mode raises TierNotFound."""
        plan = PlanDefinition(
            name="test_plan",
            tiers=[
                PlanTier(quantity=100000, unit_price=0.0005),
            ]
        )
        
        with pytest.raises(TierNotFound) as exc_info:
            plan.get_tier(10000, strict=True)
        
        assert exc_info.value.quantity == 10000
        assert "test_plan" in str(exc_info.value)
    
    def test_get_closest_tier(self):
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


class TestBillingHistoryCalculateCost:
    """Test cost calculation for billing history."""
    
    def test_calculate_cost_with_historical_price(self):
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
    
    def test_calculate_cost_with_exact_tier_match(self):
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
    
    def test_calculate_cost_with_closest_tier_fallback(self):
        """Test cost calculation using closest tier fallback."""
        history = BillingMetricHistory(
            id=3,
            organization_id="org-789",
            plan_name="test_plan",
            metric_type="transactions",
            reserved_quantity=50000,  # Between tiers
            actual_usage=40000,
            period_start="2024-03-01",
            period_end="2024-03-31",
        )
        
        plan = PlanDefinition(
            name="test_plan",
            tiers=[
                PlanTier(quantity=10000, unit_price=0.001),
                PlanTier(quantity=100000, unit_price=0.0005),
            ]
        )
        
        cost = history.calculate_cost(plan)
        assert cost == 40000 * 0.001  # Uses closest tier (10000)


class TestBillingHistorySerializer:
    """Test billing history serialization with graceful tier handling."""
    
    def test_serialize_with_exact_tier_match(self):
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
    
    def test_serialize_with_historical_tier_data(self):
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
    
    def test_serialize_with_closest_tier_fallback(self):
        """Test serialization with closest tier fallback (non-strict mode)."""
        # Create a plan with updated tiers
        from sentry_relay.billing.models import PLAN_DEFINITIONS
        PLAN_DEFINITIONS["test_fallback_plan"] = PlanDefinition(
            name="test_fallback_plan",
            metric_type="transactions",
            tiers=[
                PlanTier(quantity=10000000, unit_price=0.00026),
            ]
        )
        
        history = BillingMetricHistory(
            id=3,
            organization_id="org-789",
            plan_name="test_fallback_plan",
            metric_type="transactions",
            reserved_quantity=10000,  # Old tier smaller than current tier
            actual_usage=8000,
            period_start="2023-01-01",
            period_end="2023-01-31",
        )
        
        serializer = BillingHistorySerializer(use_strict_tier_lookup=False)
        data = serializer.serialize(history)
        
        # Should use legacy fallback since no tier <= 10000
        assert data["tier"]["tier_match"] == "legacy_fallback"
        assert data["tier"]["is_historical"] is True
        
        # Clean up
        del PLAN_DEFINITIONS["test_fallback_plan"]
    
    def test_serialize_strict_mode_raises_on_missing_tier(self):
        """Test that strict mode raises TierNotFound for missing tiers."""
        from sentry_relay.billing.models import PLAN_DEFINITIONS
        PLAN_DEFINITIONS["test_strict_plan"] = PlanDefinition(
            name="test_strict_plan",
            metric_type="transactions",
            tiers=[
                PlanTier(quantity=10000000, unit_price=0.00026),
            ]
        )
        
        history = BillingMetricHistory(
            id=4,
            organization_id="org-999",
            plan_name="test_strict_plan",
            metric_type="transactions",
            reserved_quantity=10000,  # Doesn't exist in plan
            actual_usage=8000,
            period_start="2023-01-01",
            period_end="2023-01-31",
        )
        
        serializer = BillingHistorySerializer(use_strict_tier_lookup=True)
        
        with pytest.raises(TierNotFound) as exc_info:
            serializer.serialize(history)
        
        assert exc_info.value.quantity == 10000
        
        # Clean up
        del PLAN_DEFINITIONS["test_strict_plan"]
    
    def test_serialize_many(self):
        """Test serializing multiple billing history records."""
        histories = [
            BillingMetricHistory(
                id=i,
                organization_id=f"org-{i}",
                plan_name="am1_sponsored_attachments",
                metric_type="attachments",
                reserved_quantity=1000000000,
                actual_usage=500000000 * i,
                period_start="2024-01-01",
                period_end="2024-01-31",
            )
            for i in range(1, 4)
        ]
        
        serializer = BillingHistorySerializer()
        data_list = serializer.serialize_many(histories)
        
        assert len(data_list) == 3
        assert all(d["tier"]["tier_match"] == "exact" for d in data_list)


class TestMigrateHistoricalTiers:
    """Test migration function for historical tier data."""
    
    def test_migrate_with_exact_tier(self):
        """Test migrating record with exact tier match."""
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
        
        migrated = migrate_historical_tiers(history)
        
        assert migrated.historical_tier_price is not None
        assert migrated.historical_tier_price == 0.00000015
        assert migrated.historical_tier_name == "1GB attachments"
    
    def test_migrate_already_has_historical_data(self):
        """Test migrating record that already has historical data."""
        history = BillingMetricHistory(
            id=2,
            organization_id="org-456",
            plan_name="am1_sponsored",
            metric_type="transactions",
            reserved_quantity=10000,
            actual_usage=8000,
            period_start="2023-06-01",
            period_end="2023-06-30",
            historical_tier_price=0.0008,
            historical_tier_name="10K transactions",
        )
        
        migrated = migrate_historical_tiers(history)
        
        # Should keep existing historical data
        assert migrated.historical_tier_price == 0.0008
        assert migrated.historical_tier_name == "10K transactions"


class TestRealWorldScenario:
    """Test the exact scenario described in SENTRY-3B06."""
    
    def test_sentry_3b06_scenario(self):
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
    
    def test_sentry_3b06_historical_tier_scenario(self):
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
        
        # Should NOT raise TierNotFound
