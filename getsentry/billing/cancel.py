"""
Subscription cancellation with proper concurrency control.

This module handles subscription cancellations with protection against
concurrent modifications through the subscription_lock decorator.
"""

import logging
from typing import Optional

from getsentry.models.subscription import Subscription, subscription_lock
from getsentry.billing.staged import SubscriptionUpdates

logger = logging.getLogger(__name__)


@subscription_lock
def cancel_at_period_end(subscription: Subscription, reason: Optional[str] = None) -> None:
    """
    Cancel a subscription at the end of the current billing period.
    
    The @subscription_lock decorator ensures that:
    1. The subscription is locked to prevent concurrent modifications
    2. refresh_from_db() is called to get fresh subscription state
    3. The conditional UPDATE uses current database values
    
    This prevents the race condition where another process modifies the
    subscription between reading its state and executing the UPDATE.
    
    Args:
        subscription: The subscription to cancel (refreshed by decorator)
        reason: Optional cancellation reason for logging/analytics
    """
    logger.info(
        f"Cancelling subscription {subscription.id} at period end. Reason: {reason}"
    )
    
    updates = SubscriptionUpdates()
    updates.add_field('cancel_at_period_end', True)
    
    if reason:
        updates.add_field('cancellation_reason', reason)
    
    # Build conditions using fresh subscription state
    # Thanks to @subscription_lock, these values are guaranteed to be current
    conditions = {
        'id': subscription.id,
        'plan': subscription.plan,  # Fresh from refresh_from_db()
        'billing_period_end': subscription.billing_period_end,  # Fresh
        'cancel_at_period_end': subscription.cancel_at_period_end,  # Fresh
    }
    
    # Apply the update - will succeed because conditions match DB state
    updates.apply(subscription, conditions=conditions)
    
    logger.info(
        f"Successfully scheduled cancellation for subscription {subscription.id} "
        f"at {subscription.billing_period_end}"
    )


@subscription_lock
def cancel_immediately(subscription: Subscription, reason: Optional[str] = None) -> None:
    """
    Cancel a subscription immediately.
    
    The @subscription_lock decorator refreshes the subscription state before
    we build the conditional UPDATE, preventing stale WHERE conditions.
    
    Args:
        subscription: The subscription to cancel (refreshed by decorator)
        reason: Optional cancellation reason
    """
    logger.info(
        f"Cancelling subscription {subscription.id} immediately. Reason: {reason}"
    )
    
    updates = SubscriptionUpdates()
    updates.add_field('status', 'cancelled')
    updates.add_field('cancel_at_period_end', False)
    updates.add_field('cancelled_at', _get_current_timestamp())
    
    if reason:
        updates.add_field('cancellation_reason', reason)
    
    # These conditions are fresh thanks to the decorator's refresh_from_db()
    conditions = {
        'id': subscription.id,
        'plan': subscription.plan,
        'billing_period_end': subscription.billing_period_end,
    }
    
    updates.apply(subscription, conditions=conditions)
    
    logger.info(f"Successfully cancelled subscription {subscription.id} immediately")


@subscription_lock
def reactivate_subscription(subscription: Subscription) -> None:
    """
    Reactivate a subscription that was scheduled for cancellation.
    
    This removes the cancel_at_period_end flag, allowing the subscription
    to continue past the current billing period.
    
    The @subscription_lock decorator ensures we have fresh state, preventing
    issues if another process has modified the subscription concurrently.
    
    Args:
        subscription: The subscription to reactivate (refreshed by decorator)
    """
    if not subscription.cancel_at_period_end:
        logger.warning(
            f"Subscription {subscription.id} is not scheduled for cancellation"
        )
        return
    
    logger.info(f"Reactivating subscription {subscription.id}")
    
    updates = SubscriptionUpdates()
    updates.add_field('cancel_at_period_end', False)
    updates.add_field('cancellation_reason', None)
    
    # Use fresh state for conditions (guaranteed by @subscription_lock)
    conditions = {
        'id': subscription.id,
        'plan': subscription.plan,
        'billing_period_end': subscription.billing_period_end,
        'cancel_at_period_end': True,  # Must be True to reactivate
    }
    
    updates.apply(subscription, conditions=conditions)
    
    logger.info(f"Successfully reactivated subscription {subscription.id}")


def _get_current_timestamp() -> str:
    """
    Get the current timestamp in ISO format.
    
    Returns:
        Current timestamp as ISO format string
    """
    from datetime import datetime
    return datetime.utcnow().isoformat()


# How the fix prevents cancellation race conditions:
#
# SCENARIO: Two requests try to cancel the same subscription
#
# BEFORE THE FIX:
# ---------------
# Request 1: Load subscription (cancel_at_period_end=False)
# Request 2: Load subscription (cancel_at_period_end=False)
# Request 1: Acquire lock
# Request 1: UPDATE ... WHERE cancel_at_period_end=False ✓
# Request 1: Release lock
# Request 2: Acquire lock
# Request 2: UPDATE ... WHERE cancel_at_period_end=False ✗
#            ^^^ FAILS: cancel_at_period_end is now True
#            Raises SubscriptionIntegrityError unnecessarily
#
# AFTER THE FIX:
# --------------
# Request 1: Load subscription (cancel_at_period_end=False)
# Request 2: Load subscription (cancel_at_period_end=False)
# Request 1: Acquire lock
# Request 1: refresh_from_db() -> cancel_at_period_end=False
# Request 1: UPDATE ... WHERE cancel_at_period_end=False ✓
# Request 1: Release lock
# Request 2: Acquire lock
# Request 2: refresh_from_db() -> cancel_at_period_end=True
# Request 2: UPDATE ... WHERE cancel_at_period_end=True ✓
#            ^^^ SUCCEEDS: Conditions match current state!
#
# SCENARIO: Concurrent plan change and cancellation
#
# BEFORE THE FIX:
# ---------------
# Cancel Request: Load subscription (plan='basic', period_end='2024-01-01')
# Change Request: Load subscription (plan='basic', period_end='2024-01-01')
# Cancel Request: Acquire lock
# Cancel Request: UPDATE ... WHERE plan='basic' AND period_end='2024-01-01' ✓
# Cancel Request: Release lock
# Change Request: Acquire lock
# Change Request: UPDATE ... WHERE plan='basic' AND period_end='2024-01-01' ✗
#                 ^^^ FAILS: Values changed by cancel request
#                 Raises SubscriptionIntegrityError
#
# AFTER THE FIX:
# --------------
# Cancel Request: Load subscription (plan='basic', period_end='2024-01-01')
# Change Request: Load subscription (plan='basic', period_end='2024-01-01')
# Cancel Request: Acquire lock
# Cancel Request: refresh_from_db() -> plan='basic', period_end='2024-01-01'
# Cancel Request: UPDATE ... WHERE plan='basic' AND period_end='2024-01-01' ✓
# Cancel Request: Release lock
# Change Request: Acquire lock
# Change Request: refresh_from_db() -> plan='basic', period_end='2024-01-01'
#                                      cancel_at_period_end=True (updated!)
# Change Request: UPDATE with FRESH conditions ✓
#                 ^^^ SUCCEEDS: Used current state from refresh!
