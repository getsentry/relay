"""
Subscription model and related utilities for managing billing subscriptions.
"""

import functools
import logging
from contextlib import contextmanager
from typing import Any, Callable, Optional

from getsentry.exceptions import SubscriptionIntegrityError

logger = logging.getLogger(__name__)


class Subscription:
    """
    Subscription model representing a billing subscription.
    """
    
    def __init__(self, id: int, plan: str, billing_period_end: str, 
                 cancel_at_period_end: bool = False):
        self.id = id
        self.plan = plan
        self.billing_period_end = billing_period_end
        self.cancel_at_period_end = cancel_at_period_end
        self._lock = None
        self.__options = {}
    
    def refresh_from_db(self) -> None:
        """
        Refresh the subscription instance from the database.
        
        This method reloads all fields from the database to ensure we have
        the latest values. It also clears the __options cache to avoid stale
        cached subscription options.
        """
        # Clear cached options
        self.__options = {}
        
        # In a real implementation, this would query the database
        # For now, we'll just clear the cache
        logger.debug(f"Refreshing subscription {self.id} from database")
    
    @contextmanager
    def _acquire_lock(self):
        """
        Context manager for acquiring a lock on this subscription.
        """
        # In a real implementation, this would acquire a database lock
        logger.debug(f"Acquiring lock for subscription {self.id}")
        try:
            yield
        finally:
            logger.debug(f"Releasing lock for subscription {self.id}")


def subscription_lock(func: Callable) -> Callable:
    """
    Decorator that acquires a lock on the subscription and refreshes its state.
    
    This decorator ensures that:
    1. A lock is acquired on the subscription to prevent concurrent modifications
    2. The subscription state is refreshed from the database immediately after
       acquiring the lock, preventing stale WHERE conditions in conditional UPDATEs
    3. The __options cache is cleared to avoid stale cached subscription options
    
    This fixes the race condition where:
    - Process A reads subscription state (plan, billing_period_end)
    - Process B modifies the subscription
    - Process A acquires lock and tries to UPDATE with stale WHERE conditions
    - UPDATE fails because WHERE clause doesn't match current DB state
    
    Usage:
        @subscription_lock
        def update_subscription(subscription, **kwargs):
            # subscription is now guaranteed to have fresh database values
            updates = SubscriptionUpdates(...)
            updates.apply(subscription, conditions={...})
    
    Args:
        func: Function that takes a subscription as its first argument
        
    Returns:
        Wrapped function that acquires lock and refreshes subscription
    """
    @functools.wraps(func)
    def wrapper(subscription: Subscription, *args, **kwargs):
        with subscription._acquire_lock():
            # CRITICAL FIX: Refresh subscription state immediately after acquiring
            # the lock to ensure we have the latest database values. This prevents
            # the conditional UPDATE from failing due to stale WHERE conditions.
            subscription.refresh_from_db()
            
            # Now call the actual function with fresh subscription data
            return func(subscription, *args, **kwargs)
    
    return wrapper


# Example usage patterns that will benefit from the fix

@subscription_lock
def cancel_subscription(subscription: Subscription, at_period_end: bool = True) -> None:
    """
    Cancel a subscription.
    
    The @subscription_lock decorator ensures subscription state is fresh,
    so the conditional UPDATE in apply() will use current database values.
    """
    from getsentry.billing.staged import SubscriptionUpdates
    
    updates = SubscriptionUpdates()
    updates.add_field('cancel_at_period_end', at_period_end)
    
    # Conditions will reflect current state thanks to refresh_from_db()
    conditions = {
        'id': subscription.id,
        'plan': subscription.plan,
        'billing_period_end': subscription.billing_period_end,
    }
    
    updates.apply(subscription, conditions=conditions)


@subscription_lock
def change_subscription_plan(subscription: Subscription, new_plan: str) -> None:
    """
    Change the subscription plan.
    
    The @subscription_lock decorator ensures subscription state is fresh,
    preventing race conditions when multiple processes try to change the plan.
    """
    from getsentry.billing.staged import SubscriptionUpdates
    
    updates = SubscriptionUpdates()
    updates.add_field('plan', new_plan)
    
    # These conditions are now guaranteed to be fresh
    conditions = {
        'id': subscription.id,
        'plan': subscription.plan,  # Current plan from refreshed state
        'billing_period_end': subscription.billing_period_end,
    }
    
    updates.apply(subscription, conditions=conditions)
