# Subscription Lock Refresh Fix

**Fixes SENTRY-5CZD**

## Problem

Concurrent subscription modifications were causing failed UPDATE statements due to mismatched WHERE clause conditions, resulting in `SubscriptionIntegrityError` exceptions.

### Root Cause

The race condition occurred in this sequence:

1. **Process A** loads a subscription and reads its `plan`, `billing_period_end`, and other state
2. **Process A** acquires a lock on the subscription
3. **Process B** (before or during Process A's lock wait) modifies the subscription's plan or billing_period_end
4. **Process A** executes a conditional UPDATE using the originally-read values in the WHERE clause
5. The UPDATE fails to match any rows because the subscription state has changed since it was read
6. `SubscriptionIntegrityError` is raised

### Example Scenario

```python
# Time  Process A                                    Process B
# t1    Load subscription (plan='basic')            
# t2    Acquire lock on subscription                
# t3                                                 Modify subscription (plan='pro')
# t4    UPDATE subscriptions SET ...                
#       WHERE id=1 AND plan='basic'  ← FAILS!       
#       (actual plan is 'pro', 0 rows matched)
# t5    Raise SubscriptionIntegrityError
```

## Solution

Refresh subscription state immediately after acquiring the lock, ensuring that conditional UPDATE statements use current database values in their WHERE clauses.

### Implementation

#### 1. Modified `@subscription_lock` Decorator

The `@subscription_lock` decorator in `getsentry/models/subscription.py` now:

1. Acquires the lock on the subscription
2. **Immediately calls `subscription.refresh_from_db()`** to reload all fields from the database
3. Clears the `__options` cache to avoid stale cached subscription options
4. Executes the decorated function with fresh subscription data

```python
def subscription_lock(func: Callable) -> Callable:
    @functools.wraps(func)
    def wrapper(subscription: Subscription, *args, **kwargs):
        with subscription._acquire_lock():
            # CRITICAL FIX: Refresh subscription state immediately after
            # acquiring the lock to ensure we have the latest database values
            subscription.refresh_from_db()
            
            # Now call the actual function with fresh subscription data
            return func(subscription, *args, **kwargs)
    
    return wrapper
```

#### 2. Updated `SubscriptionUpdates.apply()` Documentation

Added comprehensive documentation in `getsentry/billing/staged.py` explaining:

- How the `@subscription_lock` decorator ensures condition freshness
- The race condition that this pattern prevents
- Example usage showing fresh conditions from refreshed state
- What happens when conditions don't match (SubscriptionIntegrityError)

#### 3. Verified Conditional UPDATE Patterns

Reviewed and verified that functions in:
- `getsentry/billing/apply_subscription_change.py`
- `getsentry/billing/cancel.py`

All use the `@subscription_lock` decorator and build conditions from subscription properties that are now guaranteed to be fresh.

### How It Works

**After the Fix:**

```python
# Time  Process A                                    Process B
# t1    Load subscription (plan='basic')            
# t2    Acquire lock on subscription                
# t3    refresh_from_db() → plan='pro'              (Process B already modified it)
# t4    UPDATE subscriptions SET ...                
#       WHERE id=1 AND plan='pro'  ← SUCCEEDS!      
#       (using fresh state, 1 row matched)
# t5    Success!
```

## Key Benefits

1. **Eliminates Race Conditions**: Concurrent modifications no longer cause UPDATE failures
2. **Automatic**: Developers using `@subscription_lock` get this protection automatically
3. **Transparent**: No changes needed to existing code that already uses the decorator
4. **Cache-Safe**: Clears `__options` cache to prevent stale cached subscription options

## Usage Examples

### Cancelling a Subscription

```python
from getsentry.billing.cancel import cancel_at_period_end

# The decorator ensures fresh state
cancel_at_period_end(subscription, reason='user_request')
```

### Changing a Plan

```python
from getsentry.billing.apply_subscription_change import apply_plan_change

# Even if another process modified the subscription,
# this will work because state is refreshed after lock acquisition
apply_plan_change(subscription, 'enterprise', prorate=True)
```

### Custom Subscription Update

```python
from getsentry.models.subscription import subscription_lock
from getsentry.billing.staged import SubscriptionUpdates

@subscription_lock
def custom_update(subscription, new_value):
    updates = SubscriptionUpdates()
    updates.add_field('custom_field', new_value)
    
    # These values are guaranteed fresh by @subscription_lock
    conditions = {
        'id': subscription.id,
        'plan': subscription.plan,  # Fresh!
        'billing_period_end': subscription.billing_period_end,  # Fresh!
    }
    
    # This will succeed because conditions match current DB state
    updates.apply(subscription, conditions=conditions)
```

## Testing

Run the test suite to verify the fix:

```bash
python -m pytest getsentry/tests/test_subscription_lock_fix.py -v
```

The tests verify:

1. ✅ `@subscription_lock` calls `refresh_from_db()` after acquiring lock
2. ✅ `refresh_from_db()` clears the `__options` cache
3. ✅ Concurrent plan changes succeed instead of failing
4. ✅ All billing functions use fresh subscription state
5. ✅ Stale conditions raise `SubscriptionIntegrityError` as expected
6. ✅ Fresh conditions succeed

## Files Changed

### Core Fix

- **`getsentry/models/subscription.py`**
  - Modified `@subscription_lock` decorator to call `refresh_from_db()` after lock acquisition
  - Ensured `refresh_from_db()` clears `__options` cache

### Documentation

- **`getsentry/billing/staged.py`**
  - Added comprehensive documentation to `SubscriptionUpdates.apply()` explaining:
    - How `@subscription_lock` ensures condition freshness
    - The race condition this prevents
    - Example usage patterns

### Verified Usage

- **`getsentry/billing/apply_subscription_change.py`**
  - Verified `apply_plan_change()`, `apply_upgrade()`, `apply_downgrade()` use `@subscription_lock`
  - Added comments explaining how decorator ensures fresh state

- **`getsentry/billing/cancel.py`**
  - Verified `cancel_at_period_end()`, `cancel_immediately()`, `reactivate_subscription()` use `@subscription_lock`
  - Added examples showing race condition prevention

## Migration Guide

If you have existing code that uses `@subscription_lock`:

**No changes required!** The fix is automatic and transparent.

If you're writing new subscription update code:

1. Use `@subscription_lock` decorator on any function that modifies subscriptions
2. Build conditions from subscription properties (they'll be fresh automatically)
3. Pass conditions to `SubscriptionUpdates.apply()`

```python
@subscription_lock
def my_update_function(subscription, new_value):
    updates = SubscriptionUpdates()
    updates.add_field('field_name', new_value)
    
    # These are fresh thanks to @subscription_lock
    conditions = {
        'id': subscription.id,
        'plan': subscription.plan,
        'billing_period_end': subscription.billing_period_end,
    }
    
    updates.apply(subscription, conditions=conditions)
```

## Technical Details

### The Lock-Then-Refresh Pattern

The fix implements the "Lock-Then-Refresh" pattern:

```python
1. Acquire exclusive lock on subscription
2. Immediately refresh from database
3. Perform operations with fresh data
4. Release lock
```

This ensures that:
- Only one process can modify the subscription at a time (lock)
- That process always has the latest data (refresh)
- Conditional UPDATEs use current WHERE conditions (fresh data)

### Why This Works

**Before:** Lock → Use Stale Data → UPDATE Fails
**After:** Lock → Refresh → Use Fresh Data → UPDATE Succeeds

The key insight is that refreshing *after* lock acquisition guarantees we see all changes made by previous lock holders, eliminating the window for stale data.

### Performance Considerations

- **Cost**: One additional SELECT query per locked operation
- **Benefit**: Eliminates SubscriptionIntegrityError retries and failures
- **Net Impact**: Positive (fewer errors, less retry overhead)

## References

- **Issue**: SENTRY-5CZD
- **Root Cause**: Concurrent subscription modifications with stale WHERE conditions
- **Solution**: Refresh subscription state after acquiring lock
