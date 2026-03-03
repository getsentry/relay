"""
Apply subscription changes with proper locking and state management.

This module handles subscription plan changes, upgrades, and downgrades
with protection against concurrent modifications.
"""

import logging
from datetime import datetime
from typing import Optional

from getsentry.models.subscription import Subscription, subscription_lock
from getsentry.billing.staged import SubscriptionUpdates

logger = logging.getLogger(__name__)


@subscription_lock
def apply_plan_change(subscription: Subscription, new_plan: str,
                     prorate: bool = True) -> None:
    """
    Apply a plan change to a subscription.
    
    The @subscription_lock decorator ensures that:
    1. A lock is acquired on the subscription
    2. subscription.refresh_from_db() is called after lock acquisition
    3. All subscription properties (plan, billing_period_end, etc.) are fresh
    
    This means when we build conditions for the conditional UPDATE, they will
    match the current database state, preventing SubscriptionIntegrityError
    from concurrent modifications.
    
    Args:
        subscription: The subscription to modify (will be refreshed by decorator)
        new_plan: The new plan identifier
        prorate: Whether to prorate the plan change
    """
    logger.info(
        f"Applying plan change for subscription {subscription.id}: "
        f"{subscription.plan} -> {new_plan}"
    )
    
    # Build the updates
    updates = SubscriptionUpdates()
    updates.add_field('plan', new_plan)
    
    if prorate:
        # Calculate prorated billing period end
        # In a real implementation, this would use actual date math
        updates.add_field('billing_period_end', _calculate_prorated_end())
    
    # Build conditions using subscription properties
    # These are FRESH values thanks to @subscription_lock calling refresh_from_db()
    conditions = {
        'id': subscription.id,
        'plan': subscription.plan,  # Fresh value after lock acquisition
        'billing_period_end': subscription.billing_period_end,  # Fresh value
        'cancel_at_period_end': subscription.cancel_at_period_end,  # Fresh value
    }
    
    # Apply the update with conditional WHERE clause
    # This will succeed because conditions match current DB state
    updates.apply(subscription, conditions=conditions)
    
    logger.info(
        f"Successfully changed subscription {subscription.id} plan to {new_plan}"
    )


@subscription_lock
def apply_upgrade(subscription: Subscription, target_plan: str) -> None:
    """
    Upgrade a subscription to a higher-tier plan.
    
    The @subscription_lock decorator refreshes subscription state, ensuring
    that the conditional UPDATE uses fresh WHERE conditions.
    
    Args:
        subscription: The subscription to upgrade (refreshed by decorator)
        target_plan: The target plan to upgrade to
    """
    logger.info(
        f"Upgrading subscription {subscription.id} from {subscription.plan} "
        f"to {target_plan}"
    )
    
    updates = SubscriptionUpdates()
    updates.add_field('plan', target_plan)
    
    # Clear any pending cancellation on upgrade
    if subscription.cancel_at_period_end:
        updates.add_field('cancel_at_period_end', False)
    
    # Conditions reflect fresh state from refresh_from_db()
    conditions = {
        'id': subscription.id,
        'plan': subscription.plan,
        'billing_period_end': subscription.billing_period_end,
    }
    
    updates.apply(subscription, conditions=conditions)
    
    logger.info(f"Successfully upgraded subscription {subscription.id}")


@subscription_lock
def apply_downgrade(subscription: Subscription, target_plan: str,
                   immediate: bool = False) -> None:
    """
    Downgrade a subscription to a lower-tier plan.
    
    The @subscription_lock decorator ensures we have fresh subscription data
    before building the conditional UPDATE.
    
    Args:
        subscription: The subscription to downgrade (refreshed by decorator)
        target_plan: The target plan to downgrade to
        immediate: If True, apply immediately; if False, apply at period end
    """
    logger.info(
        f"Downgrading subscription {subscription.id} from {subscription.plan} "
        f"to {target_plan} (immediate={immediate})"
    )
    
    updates = SubscriptionUpdates()
    
    if immediate:
        updates.add_field('plan', target_plan)
    else:
        # Schedule downgrade for end of current period
        updates.add_field('planned_downgrade', target_plan)
    
    # Use fresh subscription state for conditions
    conditions = {
        'id': subscription.id,
        'plan': subscription.plan,  # Refreshed by @subscription_lock
        'billing_period_end': subscription.billing_period_end,  # Refreshed
    }
    
    updates.apply(subscription, conditions=conditions)
    
    logger.info(
        f"Successfully scheduled downgrade for subscription {subscription.id}"
    )


def _calculate_prorated_end() -> str:
    """
    Calculate prorated billing period end.
    
    In a real implementation, this would perform actual date calculations
    based on the current period and the new plan's billing cycle.
    
    Returns:
        ISO format date string for the new billing period end
    """
    # Placeholder implementation
    return datetime.now().isoformat()


# Example of how the fix prevents race conditions:
#
# BEFORE THE FIX:
# ---------------
# Time  Process A                           Process B
# t1    Load subscription (plan='basic')    
# t2    Acquire lock                        
# t3                                        Load subscription (plan='basic')
# t4    UPDATE ... WHERE plan='basic'       Blocked on lock
# t5    Release lock                        
# t6                                        Acquire lock
# t7                                        UPDATE ... WHERE plan='basic'
#                                           ^^^ FAILS because plan is now 'pro'
#                                           Raises SubscriptionIntegrityError
#
# AFTER THE FIX:
# --------------
# Time  Process A                           Process B
# t1    Load subscription (plan='basic')    
# t2    Acquire lock                        
# t3    refresh_from_db() [plan='basic']    Load subscription (plan='basic')
# t4    UPDATE ... WHERE plan='basic' ✓     Blocked on lock
# t5    Release lock                        
# t6                                        Acquire lock
# t7                                        refresh_from_db() [plan='pro']
# t8                                        UPDATE ... WHERE plan='pro' ✓
#                                           ^^^ SUCCEEDS because we refreshed!
