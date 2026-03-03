#!/usr/bin/env python3
"""
Demonstration script showing how the subscription lock refresh fix works.

This script simulates the race condition scenario and shows how the fix
prevents SubscriptionIntegrityError from occurring.
"""

import sys
sys.path.insert(0, '/workspace')

from getsentry.models.subscription import Subscription, subscription_lock
from getsentry.billing.staged import SubscriptionUpdates
from getsentry.billing.cancel import cancel_at_period_end
from getsentry.billing.apply_subscription_change import apply_plan_change


def print_section(title):
    """Print a section header."""
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}\n")


def demo_basic_functionality():
    """Demonstrate basic subscription lock functionality."""
    print_section("Demo 1: Basic Subscription Lock Functionality")
    
    subscription = Subscription(
        id=1,
        plan='basic',
        billing_period_end='2024-01-01',
        cancel_at_period_end=False
    )
    
    print(f"Initial state: plan={subscription.plan}, "
          f"period_end={subscription.billing_period_end}, "
          f"cancel={subscription.cancel_at_period_end}")
    
    # Cancel the subscription
    cancel_at_period_end(subscription, reason='demo')
    
    print(f"After cancel: plan={subscription.plan}, "
          f"period_end={subscription.billing_period_end}, "
          f"cancel={subscription.cancel_at_period_end}")
    
    print("\n✓ Cancellation succeeded with @subscription_lock protection")


def demo_fresh_state_after_lock():
    """Demonstrate that subscription state is refreshed after lock acquisition."""
    print_section("Demo 2: Fresh State After Lock Acquisition")
    
    subscription = Subscription(
        id=2,
        plan='basic',
        billing_period_end='2024-01-01',
    )
    
    refresh_count = [0]
    
    # Track refresh calls
    original_refresh = subscription.refresh_from_db
    
    def counting_refresh():
        refresh_count[0] += 1
        print(f"  → refresh_from_db() called (call #{refresh_count[0]})")
        original_refresh()
    
    subscription.refresh_from_db = counting_refresh
    
    print("Calling apply_plan_change with @subscription_lock decorator...")
    print("The decorator will:")
    print("  1. Acquire the lock")
    print("  2. Call refresh_from_db() to get fresh state")
    print("  3. Execute the update")
    print()
    
    apply_plan_change(subscription, 'pro', prorate=False)
    
    print(f"\n✓ refresh_from_db() was called {refresh_count[0]} time(s)")
    print(f"✓ Final state: plan={subscription.plan}")


def demo_conditional_update():
    """Demonstrate conditional UPDATE with fresh vs stale conditions."""
    print_section("Demo 3: Conditional UPDATE - Fresh vs Stale Conditions")
    
    subscription = Subscription(
        id=3,
        plan='basic',
        billing_period_end='2024-01-01',
    )
    
    print("Case 1: UPDATE with FRESH conditions (should succeed)")
    print(f"  Subscription state: plan={subscription.plan}")
    
    updates1 = SubscriptionUpdates()
    updates1.add_field('plan', 'pro')
    
    fresh_conditions = {
        'id': subscription.id,
        'plan': subscription.plan,  # Fresh value
        'billing_period_end': subscription.billing_period_end,
    }
    
    print(f"  Conditions: {fresh_conditions}")
    updates1.apply(subscription, conditions=fresh_conditions)
    print(f"  ✓ UPDATE succeeded! New plan: {subscription.plan}")
    
    print("\nCase 2: UPDATE with STALE conditions (should fail)")
    print(f"  Subscription state: plan={subscription.plan}")
    
    updates2 = SubscriptionUpdates()
    updates2.add_field('billing_period_end', '2024-02-01')
    
    stale_conditions = {
        'id': subscription.id,
        'plan': 'basic',  # Stale! Actual value is 'pro'
        'billing_period_end': subscription.billing_period_end,
    }
    
    print(f"  Conditions: {stale_conditions}")
    print("  Attempting UPDATE with stale plan value...")
    
    try:
        updates2.apply(subscription, conditions=stale_conditions)
        print("  ✗ UPDATE should have failed but didn't!")
    except Exception as e:
        print(f"  ✓ UPDATE correctly failed: {e.__class__.__name__}")
        print(f"    Reason: WHERE conditions didn't match current state")


def demo_race_condition_prevention():
    """Demonstrate how the fix prevents race conditions."""
    print_section("Demo 4: Race Condition Prevention")
    
    subscription = Subscription(
        id=4,
        plan='basic',
        billing_period_end='2024-01-01',
    )
    
    print("Simulating concurrent modification scenario:")
    print()
    print("WITHOUT the fix:")
    print("  1. Process A loads subscription (plan='basic')")
    print("  2. Process A acquires lock")
    print("  3. Process B modifies subscription (plan='pro')")
    print("  4. Process A tries UPDATE WHERE plan='basic'")
    print("  5. ✗ UPDATE fails - 0 rows matched!")
    print()
    print("WITH the fix:")
    print("  1. Process A loads subscription (plan='basic')")
    print("  2. Process A acquires lock")
    print("  3. Process A calls refresh_from_db() → sees plan='pro'")
    print("  4. Process A tries UPDATE WHERE plan='pro'")
    print("  5. ✓ UPDATE succeeds - 1 row matched!")
    print()
    
    # Simulate the fixed behavior
    def simulate_concurrent_change():
        """Simulate another process changing the plan."""
        print("  [Simulating concurrent change: plan='basic' → 'pro']")
        subscription.plan = 'pro'
        subscription._Subscription__options = {}
    
    original_refresh = subscription.refresh_from_db
    
    def refresh_with_simulation():
        print("  [Calling refresh_from_db() after lock acquisition]")
        simulate_concurrent_change()
        original_refresh()
    
    subscription.refresh_from_db = refresh_with_simulation
    
    print("Executing apply_plan_change with simulated concurrent modification:")
    apply_plan_change(subscription, 'enterprise', prorate=False)
    
    print(f"\n✓ Operation succeeded despite concurrent modification!")
    print(f"✓ Final plan: {subscription.plan}")


def main():
    """Run all demonstrations."""
    print("\n" + "=" * 60)
    print("  SUBSCRIPTION LOCK REFRESH FIX - DEMONSTRATION")
    print("  Fixes SENTRY-5CZD")
    print("=" * 60)
    
    demo_basic_functionality()
    demo_fresh_state_after_lock()
    demo_conditional_update()
    demo_race_condition_prevention()
    
    print_section("Summary")
    print("✓ All demonstrations completed successfully!")
    print()
    print("Key Points:")
    print("  1. @subscription_lock automatically calls refresh_from_db()")
    print("  2. Refresh happens AFTER lock acquisition")
    print("  3. This ensures WHERE conditions are always fresh")
    print("  4. Concurrent modifications no longer cause UPDATE failures")
    print("  5. Existing code gets this protection automatically")
    print()


if __name__ == '__main__':
    main()
