"""
Staged subscription updates with conditional UPDATE support.

This module provides utilities for applying subscription changes with
optimistic locking to prevent concurrent modification issues.
"""

import logging
from typing import Any, Dict, Optional

from getsentry.exceptions import SubscriptionIntegrityError

logger = logging.getLogger(__name__)


class SubscriptionUpdates:
    """
    Manages staged updates to a subscription with conditional UPDATE support.
    
    This class collects field updates and applies them using a conditional
    UPDATE statement to prevent lost updates from concurrent modifications.
    """
    
    def __init__(self):
        self._updates: Dict[str, Any] = {}
    
    def add_field(self, field: str, value: Any) -> None:
        """
        Add a field to be updated.
        
        Args:
            field: Name of the field to update
            value: New value for the field
        """
        self._updates[field] = value
    
    def apply(self, subscription: 'Subscription', 
              conditions: Optional[Dict[str, Any]] = None) -> None:
        """
        Apply the staged updates to the subscription using a conditional UPDATE.
        
        This method executes an UPDATE statement with WHERE conditions that include
        the subscription's current state. This implements optimistic locking to
        prevent concurrent modifications from causing lost updates.
        
        IMPORTANT: When passing conditions to this method, they should reflect the
        current subscription state. The @subscription_lock decorator automatically
        ensures freshness by calling refresh_from_db() immediately after acquiring
        the lock. This prevents the race condition where:
        
        1. Process A reads subscription state (e.g., plan='basic', period_end='2024-01-01')
        2. Process B modifies the subscription (plan='pro', period_end='2024-02-01')
        3. Process A tries to UPDATE with stale conditions:
           UPDATE subscriptions SET ... 
           WHERE id=X AND plan='basic' AND period_end='2024-01-01'
        4. UPDATE matches 0 rows because the actual state has changed
        5. SubscriptionIntegrityError is raised
        
        The fix:
        - @subscription_lock calls refresh_from_db() after acquiring the lock
        - This ensures subscription.plan and subscription.billing_period_end
          have fresh database values
        - Conditions built from these attributes will match the current DB state
        - The conditional UPDATE succeeds
        
        Example usage (with @subscription_lock):
            
            @subscription_lock
            def update_plan(subscription, new_plan):
                updates = SubscriptionUpdates()
                updates.add_field('plan', new_plan)
                
                # These values are fresh thanks to @subscription_lock
                conditions = {
                    'id': subscription.id,
                    'plan': subscription.plan,  # Refreshed by decorator
                    'billing_period_end': subscription.billing_period_end,  # Refreshed
                }
                
                # This UPDATE will succeed because conditions match DB state
                updates.apply(subscription, conditions=conditions)
        
        Args:
            subscription: The subscription to update
            conditions: WHERE conditions for the UPDATE (should reflect current state)
        
        Raises:
            SubscriptionIntegrityError: If the UPDATE affects 0 rows, indicating
                                       concurrent modification
        """
        if not self._updates:
            logger.debug(f"No updates to apply for subscription {subscription.id}")
            return
        
        # Build the UPDATE statement
        if conditions is None:
            conditions = {'id': subscription.id}
        
        # Log the update for debugging
        logger.debug(
            f"Applying updates to subscription {subscription.id}: "
            f"updates={self._updates}, conditions={conditions}"
        )
        
        # In a real implementation, this would execute:
        # UPDATE subscriptions
        # SET field1=value1, field2=value2, ...
        # WHERE id=X AND plan=Y AND billing_period_end=Z AND ...
        
        affected_rows = self._execute_conditional_update(subscription, conditions)
        
        if affected_rows == 0:
            # The UPDATE matched 0 rows, which means the subscription state
            # has changed since we read it. This indicates a concurrent modification.
            raise SubscriptionIntegrityError(
                f"Failed to update subscription {subscription.id}: "
                f"WHERE conditions did not match (concurrent modification detected). "
                f"Conditions: {conditions}"
            )
        
        # Update the subscription object with the new values
        for field, value in self._updates.items():
            setattr(subscription, field, value)
        
        logger.info(
            f"Successfully updated subscription {subscription.id} "
            f"with {len(self._updates)} changes"
        )
    
    def _execute_conditional_update(self, subscription: 'Subscription',
                                    conditions: Dict[str, Any]) -> int:
        """
        Execute the conditional UPDATE statement.
        
        This simulates a database UPDATE with WHERE conditions.
        In a real implementation, this would use Django ORM or raw SQL.
        
        Args:
            subscription: The subscription to update
            conditions: WHERE clause conditions
        
        Returns:
            Number of rows affected (1 if successful, 0 if conditions didn't match)
        """
        # Simulate checking if conditions match current subscription state
        for field, expected_value in conditions.items():
            actual_value = getattr(subscription, field, None)
            if actual_value != expected_value:
                # Conditions don't match - simulate 0 rows affected
                logger.warning(
                    f"Condition mismatch for subscription {subscription.id}: "
                    f"{field} expected {expected_value}, got {actual_value}"
                )
                return 0
        
        # In a real implementation, this would execute the SQL UPDATE
        # For this example, we'll just return 1 to indicate success
        return 1
