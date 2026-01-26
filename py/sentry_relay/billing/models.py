"""Billing models for plan tiers and historical billing data.

Fixes SENTRY-3B06: Historical billing data tier lookup failures.
"""

from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class PlanTier:
    """Represents a billing plan tier with quantity and pricing information."""
    
    quantity: int  # Reserved quantity for this tier (e.g., 10000000 for 10M transactions)
    unit_price: float  # Price per unit in this tier
    name: str = ""  # Optional human-readable name (e.g., "10M transactions")
    
    def __post_init__(self):
        if not self.name:
            self.name = f"{self.quantity:,} units"


@dataclass
class PlanDefinition:
    """Represents a billing plan with multiple tiers."""
    
    name: str
    tiers: List[PlanTier]
    metric_type: str = "transactions"  # e.g., "transactions", "attachments"
    
    def get_tier(self, quantity: int, strict: bool = True) -> Optional[PlanTier]:
        """Get the tier for a given quantity.
        
        Args:
            quantity: The reserved quantity to look up
            strict: If True, raises TierNotFound when tier doesn't exist.
                   If False, returns None when tier doesn't exist.
        
        Returns:
            The matching PlanTier or None if not found (when strict=False)
            
        Raises:
            TierNotFound: When strict=True and no matching tier is found
        """
        from .exceptions import TierNotFound
        
        for tier in self.tiers:
            if tier.quantity == quantity:
                return tier
        
        if strict:
            raise TierNotFound(quantity, self.name)
        return None
    
    def get_closest_tier(self, quantity: int) -> Optional[PlanTier]:
        """Get the closest tier for a given quantity.
        
        This is useful for historical data where exact tier may not exist.
        Returns the tier with the closest quantity that is <= the given quantity.
        
        Args:
            quantity: The reserved quantity to look up
            
        Returns:
            The closest matching PlanTier or None if no suitable tier exists
        """
        if not self.tiers:
            return None
        
        # Sort tiers by quantity
        sorted_tiers = sorted(self.tiers, key=lambda t: t.quantity)
        
        # Find the largest tier that is <= quantity
        closest_tier = None
        for tier in sorted_tiers:
            if tier.quantity <= quantity:
                closest_tier = tier
            else:
                break
        
        return closest_tier


@dataclass
class BillingMetricHistory:
    """Historical billing record for a specific metric and time period.
    
    This model stores billing data from past periods, which may reference
    tier quantities that no longer exist in current plan definitions.
    """
    
    id: int
    organization_id: str
    plan_name: str
    metric_type: str
    reserved_quantity: int  # Historical quantity that may not match current tiers
    actual_usage: int
    period_start: str
    period_end: str
    
    # Store historical tier information to avoid lookup failures
    historical_tier_price: Optional[float] = None
    historical_tier_name: Optional[str] = None
    
    def calculate_cost(self, plan_definition: PlanDefinition) -> float:
        """Calculate the cost for this billing period.
        
        Fixes SENTRY-3B06: This method now gracefully handles cases where
        the historical reserved_quantity doesn't match any current tier.
        
        Args:
            plan_definition: Current plan definition (may differ from historical)
            
        Returns:
            The calculated cost, using historical tier price if available
        """
        # If we have historical tier price stored, use it
        if self.historical_tier_price is not None:
            return self.actual_usage * self.historical_tier_price
        
        # Try to find exact tier match in current plan
        tier = plan_definition.get_tier(self.reserved_quantity, strict=False)
        
        # If no exact match, try to find closest tier
        if tier is None:
            tier = plan_definition.get_closest_tier(self.reserved_quantity)
        
        # If still no tier found, return 0 (or could log warning)
        if tier is None:
            return 0.0
        
        return self.actual_usage * tier.unit_price


# Example plan definitions
PLAN_DEFINITIONS = {
    "am1_sponsored": PlanDefinition(
        name="am1_sponsored",
        metric_type="transactions",
        tiers=[
            PlanTier(quantity=10000000, unit_price=0.00026, name="10M transactions"),
        ]
    ),
    "am1_sponsored_attachments": PlanDefinition(
        name="am1_sponsored_attachments",
        metric_type="attachments",
        tiers=[
            PlanTier(quantity=1000000000, unit_price=0.00000015, name="1GB attachments"),
        ]
    ),
}


def get_plan_definition(plan_name: str) -> Optional[PlanDefinition]:
    """Get a plan definition by name."""
    return PLAN_DEFINITIONS.get(plan_name)
