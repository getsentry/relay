"""
Tests for the subscription lock refresh fix.

These tests verify that the @subscription_lock decorator properly refreshes
subscription state after acquiring the lock, preventing SubscriptionIntegrityError
from stale WHERE conditions in concurrent modification scenarios.
"""

import unittest
from unittest.mock import Mock, patch, call
from getsentry.models.subscription import (
    Subscription,
    subscription_lock,
)
from getsentry.exceptions import SubscriptionIntegrityError
from getsentry.billing.staged import SubscriptionUpdates
from getsentry.billing.apply_subscription_change import (
    apply_plan_change,
    apply_upgrade,
    apply_downgrade,
)
from getsentry.billing.cancel import (
    cancel_at_period_end,
    cancel_immediately,
    reactivate_subscription,
)


class TestSubscriptionLockRefresh(unittest.TestCase):
    """Test that the subscription_lock decorator refreshes state after lock acquisition."""
    
    def test_decorator_calls_refresh_from_db(self):
        """Verify that @subscription_lock calls refresh_from_db() after acquiring lock."""
        subscription = Subscription(
            id=1,
            plan='basic',
            billing_period_end='2024-01-01',
        )
        
        # Mock the refresh_from_db method to track if it's called
        with patch.object(subscription, 'refresh_from_db') as mock_refresh:
            
            @subscription_lock
            def test_function(sub):
                # This should be called after refresh_from_db
                return sub.plan
            
            result = test_function(subscription)
            
            # Verify refresh_from_db was called exactly once
            mock_refresh.assert_called_once()
            self.assertEqual(result, 'basic')
    
    def test_refresh_clears_options_cache(self):
        """Verify that refresh_from_db() clears the __options cache."""
        subscription = Subscription(
            id=1,
            plan='basic',
            billing_period_end='2024-01-01',
        )
        
        # Set some cached options
        subscription._Subscription__options = {'cached': 'value'}
        
        # Call refresh_from_db
        subscription.refresh_from_db()
        
        # Verify cache was cleared
        self.assertEqual(subscription._Subscription__options, {})
    
    def test_concurrent_plan_change_scenario(self):
        """
        Simulate concurrent plan changes to verify the fix prevents errors.
        
        BEFORE FIX: Process A reads state, Process B modifies, Process A fails
        AFTER FIX: Process A refreshes after lock, succeeds with current state
        """
        subscription = Subscription(
            id=1,
            plan='basic',
            billing_period_end='2024-01-01',
        )
        
        call_count = [0]
        
        def mock_refresh():
            """Simulate state change from another process on first call."""
            call_count[0] += 1
            if call_count[0] == 1:
                # First call: simulate another process changed the plan
                subscription.plan = 'pro'
                subscription.billing_period_end = '2024-02-01'
            # Clear cache as the real method would
            subscription._Subscription__options = {}
        
        with patch.object(subscription, 'refresh_from_db', side_effect=mock_refresh):
            @subscription_lock
            def update_plan(sub, new_plan):
                # After lock and refresh, we should see the updated state
                updates = SubscriptionUpdates()
                updates.add_field('plan', new_plan)
                
                conditions = {
                    'id': sub.id,
                    'plan': sub.plan,  # This will be 'pro' after refresh
                    'billing_period_end': sub.billing_period_end,  # '2024-02-01'
                }
                
                # This should succeed because we refreshed
                updates.apply(sub, conditions=conditions)
            
            # This should not raise SubscriptionIntegrityError
            update_plan(subscription, 'enterprise')
            
            # Verify refresh was called
            self.assertEqual(call_count[0], 1)
    
    def test_apply_plan_change_uses_fresh_state(self):
        """Verify apply_plan_change uses fresh subscription state."""
        subscription = Subscription(
            id=1,
            plan='basic',
            billing_period_end='2024-01-01',
        )
        
        # Track whether refresh was called before building conditions
        refresh_called = [False]
        original_refresh = subscription.refresh_from_db
        
        def tracking_refresh():
            refresh_called[0] = True
            original_refresh()
        
        with patch.object(subscription, 'refresh_from_db', side_effect=tracking_refresh):
            # This should call refresh_from_db before using subscription properties
            apply_plan_change(subscription, 'pro', prorate=False)
            
            # Verify refresh was called
            self.assertTrue(refresh_called[0])
    
    def test_cancel_at_period_end_uses_fresh_state(self):
        """Verify cancel_at_period_end uses fresh subscription state."""
        subscription = Subscription(
            id=1,
            plan='basic',
            billing_period_end='2024-01-01',
            cancel_at_period_end=False,
        )
        
        refresh_called = [False]
        
        def tracking_refresh():
            refresh_called[0] = True
            subscription._Subscription__options = {}
        
        with patch.object(subscription, 'refresh_from_db', side_effect=tracking_refresh):
            cancel_at_period_end(subscription, reason='user_request')
            
            # Verify refresh was called
            self.assertTrue(refresh_called[0])
            
            # Verify the subscription was updated
            self.assertTrue(subscription.cancel_at_period_end)
    
    def test_reactivate_subscription_uses_fresh_state(self):
        """Verify reactivate_subscription uses fresh subscription state."""
        subscription = Subscription(
            id=1,
            plan='basic',
            billing_period_end='2024-01-01',
            cancel_at_period_end=True,
        )
        
        refresh_called = [False]
        
        def tracking_refresh():
            refresh_called[0] = True
            subscription._Subscription__options = {}
        
        with patch.object(subscription, 'refresh_from_db', side_effect=tracking_refresh):
            reactivate_subscription(subscription)
            
            # Verify refresh was called
            self.assertTrue(refresh_called[0])
            
            # Verify the subscription was updated
            self.assertFalse(subscription.cancel_at_period_end)


class TestConditionalUpdateIntegrity(unittest.TestCase):
    """Test that conditional UPDATEs properly detect stale conditions."""
    
    def test_stale_conditions_raise_error(self):
        """Verify that stale WHERE conditions raise SubscriptionIntegrityError."""
        subscription = Subscription(
            id=1,
            plan='basic',
            billing_period_end='2024-01-01',
        )
        
        updates = SubscriptionUpdates()
        updates.add_field('plan', 'pro')
        
        # Simulate another process changed the plan
        subscription.plan = 'enterprise'
        
        # Build conditions with stale values
        stale_conditions = {
            'id': 1,
            'plan': 'basic',  # Stale! Actual value is 'enterprise'
            'billing_period_end': '2024-01-01',
        }
        
        # This should raise SubscriptionIntegrityError
        with self.assertRaises(SubscriptionIntegrityError) as ctx:
            updates.apply(subscription, conditions=stale_conditions)
        
        self.assertIn('concurrent modification detected', str(ctx.exception))
    
    def test_fresh_conditions_succeed(self):
        """Verify that fresh WHERE conditions succeed."""
        subscription = Subscription(
            id=1,
            plan='basic',
            billing_period_end='2024-01-01',
        )
        
        updates = SubscriptionUpdates()
        updates.add_field('plan', 'pro')
        
        # Build conditions with current values
        conditions = {
            'id': subscription.id,
            'plan': subscription.plan,
            'billing_period_end': subscription.billing_period_end,
        }
        
        # This should succeed
        updates.apply(subscription, conditions=conditions)
        
        # Verify the update was applied
        self.assertEqual(subscription.plan, 'pro')


class TestRaceConditionScenarios(unittest.TestCase):
    """Test specific race condition scenarios that the fix addresses."""
    
    def test_concurrent_cancellation_requests(self):
        """
        Test scenario: Two requests try to cancel the same subscription.
        
        Without the fix, the second request would fail with SubscriptionIntegrityError
        because cancel_at_period_end changed from False to True.
        
        With the fix, the second request refreshes after lock and sees the updated
        state, so it either succeeds idempotently or is handled gracefully.
        """
        subscription = Subscription(
            id=1,
            plan='basic',
            billing_period_end='2024-01-01',
            cancel_at_period_end=False,
        )
        
        # First cancellation
        with patch.object(subscription, 'refresh_from_db'):
            cancel_at_period_end(subscription, reason='user_request')
        
        self.assertTrue(subscription.cancel_at_period_end)
        
        # Second cancellation (simulates concurrent request)
        # The decorator will refresh and see cancel_at_period_end=True
        with patch.object(subscription, 'refresh_from_db'):
            # This should not raise an error
            cancel_at_period_end(subscription, reason='duplicate_request')
    
    def test_plan_change_during_cancellation(self):
        """
        Test scenario: Plan change happens while cancellation is in progress.
        
        Without the fix, if a plan change happens between reading state and
        executing UPDATE, the cancellation would fail.
        
        With the fix, the cancellation refreshes and uses the new plan value
        in its WHERE conditions.
        """
        subscription = Subscription(
            id=1,
            plan='basic',
            billing_period_end='2024-01-01',
            cancel_at_period_end=False,
        )
        
        def simulate_concurrent_change():
            """Simulate a plan change from another process."""
            subscription.plan = 'pro'
            subscription.billing_period_end = '2024-02-01'
            subscription._Subscription__options = {}
        
        # The refresh will see the changed plan
        with patch.object(subscription, 'refresh_from_db', 
                         side_effect=simulate_concurrent_change):
            # This should succeed with the updated plan value
            cancel_at_period_end(subscription, reason='user_request')
        
        # Verify cancellation succeeded
        self.assertTrue(subscription.cancel_at_period_end)


class TestDocumentationCompliance(unittest.TestCase):
    """Verify that the implementation matches the documented behavior."""
    
    def test_subscription_lock_decorator_documentation(self):
        """Verify subscription_lock decorator behavior matches its documentation."""
        subscription = Subscription(id=1, plan='basic', billing_period_end='2024-01-01')
        
        @subscription_lock
        def test_func(sub):
            return sub
        
        with patch.object(subscription, '_acquire_lock') as mock_lock, \
             patch.object(subscription, 'refresh_from_db') as mock_refresh:
            
            # Configure mock to work as context manager
            mock_lock.return_value.__enter__ = Mock()
            mock_lock.return_value.__exit__ = Mock()
            
            test_func(subscription)
            
            # Verify lock was acquired
            mock_lock.assert_called_once()
            
            # Verify refresh_from_db was called after lock acquisition
            mock_refresh.assert_called_once()
    
    def test_subscription_updates_apply_documentation(self):
        """Verify SubscriptionUpdates.apply() behavior matches documentation."""
        subscription = Subscription(id=1, plan='basic', billing_period_end='2024-01-01')
        
        updates = SubscriptionUpdates()
        updates.add_field('plan', 'pro')
        
        # Test with matching conditions (should succeed)
        conditions = {
            'id': subscription.id,
            'plan': subscription.plan,
        }
        
        updates.apply(subscription, conditions=conditions)
        self.assertEqual(subscription.plan, 'pro')
        
        # Test with mismatched conditions (should raise error)
        updates2 = SubscriptionUpdates()
        updates2.add_field('billing_period_end', '2024-02-01')
        
        stale_conditions = {
            'id': subscription.id,
            'plan': 'basic',  # Stale value
        }
        
        with self.assertRaises(SubscriptionIntegrityError):
            updates2.apply(subscription, conditions=stale_conditions)


if __name__ == '__main__':
    unittest.main()
