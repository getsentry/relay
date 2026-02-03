# Fix for SENTRY-5J00: Pydantic Validation Error on In-Flight Tasks

## Issue Summary

Deployment added required `trigger_at` and `sentry_received_trigger_at` fields to the Pydantic model, but in-flight tasks queued before deployment lack these fields, causing validation errors.

## Root Cause Analysis

1. **Initial Problem**: Commit f5076a2 made `trigger_at` and `sentry_received_trigger_at` mandatory fields in the `GitHubWebhookEventPayload` Pydantic model without providing defaults.

2. **Failure Scenario**:
   - Tasks were enqueued with the old payload format (without `trigger_at` and `sentry_received_trigger_at`)
   - New code was deployed with these fields marked as required
   - Workers picked up pre-deployment tasks and attempted Pydantic validation
   - Validation failed with "field required" errors

3. **Impact**: All in-flight tasks from before the deployment failed validation and couldn't be processed.

## Solution

### Changes Made

Made `trigger_at` and `sentry_received_trigger_at` **optional fields with default values** in the Pydantic model:

```python
# Before (BROKEN - causes validation errors):
trigger_at: datetime
sentry_received_trigger_at: datetime

# After (FIXED - backward compatible):
trigger_at: Optional[datetime] = Field(default=None, ...)
sentry_received_trigger_at: Optional[datetime] = Field(default=None, ...)
```

### Implementation Details

**File**: `py/sentry_relay/webhook_models.py`

The `GitHubWebhookEventPayload` model now:

1. **Optional Fields**: Both timestamp fields are now `Optional[datetime]` with `default=None`
2. **Helper Methods**: 
   - `get_trigger_timestamp()`: Returns the appropriate timestamp with fallback logic
   - `is_legacy_payload()`: Identifies payloads created before the timestamp fields were added
3. **Backward Compatibility**: Old payloads without timestamp fields will validate successfully with `None` values

### Code Example

```python
# Legacy payload (queued before deployment) - NOW WORKS
old_payload = {
    "event_id": "123",
    "event_type": "push",
    "payload": {"ref": "refs/heads/main"},
    "repository": "getsentry/relay"
    # Note: No trigger_at or sentry_received_trigger_at fields
}
event = GitHubWebhookEventPayload(**old_payload)
# ✅ Validation succeeds, timestamp fields are None

# New payload (queued after deployment) - WORKS AS BEFORE
new_payload = {
    "event_id": "456",
    "event_type": "pull_request",
    "payload": {"action": "opened"},
    "repository": "getsentry/relay",
    "trigger_at": datetime(2026, 2, 3, 10, 0, 0),
    "sentry_received_trigger_at": datetime(2026, 2, 3, 10, 0, 1)
}
event = GitHubWebhookEventPayload(**new_payload)
# ✅ Validation succeeds, timestamp fields are populated
```

## Best Practices for Future Schema Changes

To avoid similar issues in the future:

### 1. Make New Fields Optional

Always add new fields as optional with sensible defaults:

```python
# ✅ GOOD - Backward compatible
new_field: Optional[str] = Field(default=None, description="...")

# ❌ BAD - Breaks backward compatibility
new_field: str = Field(..., description="...")
```

### 2. Provide Default Values

Use Pydantic's `Field` with `default` or `default_factory`:

```python
# For simple defaults
created_at: Optional[datetime] = Field(default=None)

# For complex defaults
metadata: Dict[str, Any] = Field(default_factory=dict)
```

### 3. Implement Graceful Degradation

Add helper methods to handle missing data:

```python
def get_timestamp(self) -> datetime:
    """Get timestamp with fallback to current time."""
    return self.trigger_at or datetime.utcnow()
```

### 4. Test with Legacy Data

Before deploying schema changes, test with:
- Old payload format (without new fields)
- New payload format (with new fields)
- Mixed scenarios

### 5. Consider Migration Strategies

For critical schema changes, consider:

1. **Two-Phase Rollout**:
   - Phase 1: Add fields as optional
   - Phase 2: (After queue is drained) Make fields required if needed

2. **Backfill Strategy**:
   - Add fields as optional with defaults
   - Background job backfills missing data
   - Monitor until all records are updated

3. **Queue Draining**:
   - Pause task enqueuing
   - Wait for queue to drain
   - Deploy schema changes
   - Resume task enqueuing

## Validation

### Before Fix
```python
# This would fail validation:
old_task_payload = {"event_id": "123", "event_type": "push", ...}
GitHubWebhookEventPayload(**old_task_payload)
# ❌ ValidationError: field required -> trigger_at
# ❌ ValidationError: field required -> sentry_received_trigger_at
```

### After Fix
```python
# This now succeeds:
old_task_payload = {"event_id": "123", "event_type": "push", ...}
event = GitHubWebhookEventPayload(**old_task_payload)
# ✅ Validation succeeds
# event.trigger_at == None
# event.sentry_received_trigger_at == None
# event.is_legacy_payload() == True
```

## Testing

To verify the fix:

1. **Unit Tests**: Test both old and new payload formats
2. **Integration Tests**: Verify tasks from before deployment can be processed
3. **Monitoring**: Watch for validation errors in production logs

## Related Issues

- Commit: f5076a2 (introduced the breaking change)
- Issue: SENTRY-5J00 (validation errors on in-flight tasks)

## References

- [Pydantic Field Defaults](https://docs.pydantic.dev/latest/usage/fields/)
- [Optional Types in Python](https://docs.python.org/3/library/typing.html#typing.Optional)
- [Backward Compatibility Best Practices](https://docs.sentry.io/development/backward-compatibility/)

---

**Fixes SENTRY-5J00**
