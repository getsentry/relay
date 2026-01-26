"""Billing history serializer with graceful tier lookup handling.

Fixes SENTRY-3B06: Historical billing data contains reserved quantities for tiers
that no longer exist in the current plan definition, causing tier lookup failures
when serializing billing history.
"""

from typing import Any, Dict, Optional
from .models import BillingMetricHistory, get_plan_definition, PlanTier
from .exceptions import TierNotFound


class BillingHistorySerializer:
    """Serializer for billing history that gracefully handles missing tiers.
    
    This serializer addresses the issue where historical billing data references
    tier quantities that no longer exist in current plan definitions. Instead of
    raising TierNotFound errors, it:
    
    1. Uses stored historical tier information when available
    2. Falls back to closest tier matching when exact tier doesn't exist
    3. Gracefully handles missing tiers by using default values
    """
    
    def __init__(self, use_strict_tier_lookup: bool = False):
        """Initialize the serializer.
        
        Args:
            use_strict_tier_lookup: If True, raises TierNotFound for missing tiers.
                                   If False (default), handles missing tiers gracefully.
        """
        self.use_strict_tier_lookup = use_strict_tier_lookup
    
    def serialize(self, billing_history: BillingMetricHistory) -> Dict[str, Any]:
        """Serialize billing history record.
        
        Args:
            billing_history: The billing history record to serialize
            
        Returns:
            Dictionary representation of the billing history
            
        Raises:
            TierNotFound: Only if use_strict_tier_lookup=True and tier not found
        """
        plan_def = get_plan_definition(billing_history.plan_name)
        
        # Get tier information (with graceful fallback)
        tier_info = self._get_tier_info(billing_history, plan_def)
        
        # Calculate cost
        cost = billing_history.calculate_cost(plan_def) if plan_def else 0.0
        
        return {
            "id": billing_history.id,
            "organization_id": billing_history.organization_id,
            "plan_name": billing_history.plan_name,
            "metric_type": billing_history.metric_type,
            "reserved_quantity": billing_history.reserved_quantity,
            "actual_usage": billing_history.actual_usage,
            "period_start": billing_history.period_start,
            "period_end": billing_history.period_end,
            "tier": tier_info,
            "cost": cost,
        }
    
    def _get_tier_info(
        self,
        billing_history: BillingMetricHistory,
        plan_def: Optional[Any]
    ) -> Dict[str, Any]:
        """Get tier information with graceful fallback for historical data.
        
        This method implements the fix for SENTRY-3B06 by:
        1. First checking if historical tier data is stored in the billing record
        2. Then attempting exact tier lookup in current plan definition
        3. Falling back to closest tier matching
        4. Returning a "legacy tier" placeholder if all else fails
        
        Args:
            billing_history: The billing history record
            plan_def: Current plan definition (may be None)
            
        Returns:
            Dictionary with tier information
            
        Raises:
            TierNotFound: Only if use_strict_tier_lookup=True and tier not found
        """
        # Strategy 1: Use historical tier data if available
        if billing_history.historical_tier_price is not None:
            return {
                "quantity": billing_history.reserved_quantity,
                "unit_price": billing_history.historical_tier_price,
                "name": billing_history.historical_tier_name or f"{billing_history.reserved_quantity:,} units",
                "is_historical": True,
                "tier_match": "historical_data",
            }
        
        # Strategy 2: Try exact tier lookup in current plan
        if plan_def:
            tier = plan_def.get_tier(billing_history.reserved_quantity, strict=False)
            if tier:
                return {
                    "quantity": tier.quantity,
                    "unit_price": tier.unit_price,
                    "name": tier.name,
                    "is_historical": False,
                    "tier_match": "exact",
                }
            
            # Strategy 3: Fall back to closest tier
            if not self.use_strict_tier_lookup:
                closest_tier = plan_def.get_closest_tier(billing_history.reserved_quantity)
                if closest_tier:
                    return {
                        "quantity": billing_history.reserved_quantity,
                        "unit_price": closest_tier.unit_price,
                        "name": f"{billing_history.reserved_quantity:,} units (legacy tier)",
                        "is_historical": True,
                        "tier_match": "closest_fallback",
                        "current_closest_tier": closest_tier.quantity,
                    }
        
        # Strategy 4: If strict mode, raise error
        if self.use_strict_tier_lookup:
            raise TierNotFound(
                billing_history.reserved_quantity,
                billing_history.plan_name
            )
        
        # Strategy 5: Return legacy tier placeholder
        return {
            "quantity": billing_history.reserved_quantity,
            "unit_price": 0.0,
            "name": f"{billing_history.reserved_quantity:,} units (legacy tier - plan not found)",
            "is_historical": True,
            "tier_match": "legacy_fallback",
        }
    
    def serialize_many(self, billing_histories: list) -> list:
        """Serialize multiple billing history records.
        
        Args:
            billing_histories: List of billing history records
            
        Returns:
            List of serialized records
        """
        return [self.serialize(bh) for bh in billing_histories]


def migrate_historical_tiers(billing_history: BillingMetricHistory) -> BillingMetricHistory:
    """Migrate historical billing records to store tier information.
    
    This function should be run as a data migration to populate historical_tier_price
    and historical_tier_name fields for existing records, preventing future TierNotFound errors.
    
    Args:
        billing_history: The billing history record to migrate
        
    Returns:
        Updated billing history record with historical tier data
    """
    # If already has historical tier data, no need to migrate
    if billing_history.historical_tier_price is not None:
        return billing_history
    
    # Try to get current plan definition
    plan_def = get_plan_definition(billing_history.plan_name)
    if plan_def:
        # Try exact tier lookup
        tier = plan_def.get_tier(billing_history.reserved_quantity, strict=False)
        if tier:
            billing_history.historical_tier_price = tier.unit_price
            billing_history.historical_tier_name = tier.name
        else:
            # Use closest tier
            closest_tier = plan_def.get_closest_tier(billing_history.reserved_quantity)
            if closest_tier:
                billing_history.historical_tier_price = closest_tier.unit_price
                billing_history.historical_tier_name = f"{billing_history.reserved_quantity:,} units (legacy)"
    
    return billing_history
