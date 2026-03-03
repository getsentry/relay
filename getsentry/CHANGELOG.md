# Changelog

All notable changes to the getsentry subscription management module will be documented in this file.

## [Unreleased]

### Fixed

- **[SENTRY-5CZD] Subscription Lock Refresh**: Fixed race condition causing `SubscriptionIntegrityError` from concurrent subscription modifications
  - Modified `@subscription_lock` decorator to call `refresh_from_db()` immediately after acquiring lock
  - Ensures conditional UPDATE statements use fresh database values in WHERE clauses
  - Clears `__options` cache after refresh to prevent stale cached subscription options
  - Added comprehensive documentation to `SubscriptionUpdates.apply()` explaining the pattern
  - All existing code using `@subscription_lock` automatically benefits from this fix
  
  **Impact**: Eliminates failed UPDATE statements when multiple processes modify the same subscription concurrently. The fix ensures that when a lock is acquired, the subscription state is immediately refreshed from the database, preventing the conditional UPDATE from using stale values in its WHERE clause.
  
  **Files Changed**:
  - `getsentry/models/subscription.py`: Updated `@subscription_lock` decorator
  - `getsentry/billing/staged.py`: Added documentation to `SubscriptionUpdates.apply()`
  - `getsentry/billing/apply_subscription_change.py`: Verified usage patterns
  - `getsentry/billing/cancel.py`: Verified usage patterns
  - `getsentry/exceptions.py`: Added shared `SubscriptionIntegrityError` exception

### Added

- Comprehensive test suite in `getsentry/tests/test_subscription_lock_fix.py` verifying:
  - Lock decorator calls `refresh_from_db()` after lock acquisition
  - Cache clearing functionality
  - Concurrent modification scenarios
  - Stale vs. fresh condition handling
