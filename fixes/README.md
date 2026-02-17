# Fix for MS Teams Integration BotNotInConversationRoster Error

**Issue**: SENTRY-5HA4

## Problem Description

MS Teams alert notifications fail with 'BotNotInConversationRoster' HTTP 403 error when multiple organizations have MS Teams integrations configured.

### Root Cause

1. When sending MS Teams notifications from a region silo, the `MsTeamsClient` is initialized without organization context
2. The client calls `infer_org_integration(integration_id)` which returns the **first** org_integration record
3. When the request is proxied through control silo, it uses credentials from the wrong organization
4. Microsoft Teams API rejects the request with HTTP 403 because the bot credentials don't match the conversation's organization

### Code Flow

```
send_notification_as_msteams() [notifications.py]
  → MsTeamsClient(integration) [client.py:94]
    → infer_org_integration(integration.id) [proxy.py:72]
      → Returns FIRST org_integration (WRONG!)
    → IntegrationProxyClient.__init__(org_integration_id) [proxy.py:122]
  → proxy request to control silo with wrong org_integration_id
  → control silo fetches wrong credentials
  → Microsoft API returns 403 BotNotInConversationRoster
```

## Solution

The fix involves three key changes:

### 1. Update `MsTeamsClient` to accept organization context

**File**: `src/sentry/integrations/msteams/client.py`

Modify `MsTeamsClient.__init__()` to:
- Accept an optional `org_integration_id` parameter
- Accept an optional `organization_id` parameter to look up the correct org_integration
- Only fall back to `infer_org_integration()` if neither is provided

### 2. Update notification sender to pass organization context

**File**: `src/sentry/integrations/msteams/notifications.py`

Modify `send_notification_as_msteams()` to:
- Look up the correct org_integration for the organization and integration
- Pass the `org_integration_id` when initializing `MsTeamsClient`

### 3. Improve `infer_org_integration` with logging (optional but recommended)

**File**: `src/sentry/shared_integrations/client/proxy.py`

Enhance `infer_org_integration()` to:
- Log warnings when falling back to first org_integration with multiple orgs
- Add Sentry error tracking for this anti-pattern

## Files Changed

1. `src/sentry/integrations/msteams/client.py` - Add organization context support
2. `src/sentry/integrations/msteams/notifications.py` - Pass org_integration_id
3. `src/sentry/shared_integrations/client/proxy.py` - Improve logging (optional)

## Testing

### Manual Testing Steps

1. Set up MS Teams integration in multiple organizations
2. Configure MS Teams alert rule in one organization  
3. Trigger an alert that should send notification via MS Teams
4. Verify notification is sent successfully
5. Check that the correct organization's credentials are used

### Automated Tests

See `fixes/tests/test_msteams_org_context.py` for test cases covering:
- Single organization scenario (backward compatibility)
- Multiple organizations scenario (uses correct org credentials)
- Fallback behavior when org_integration_id not provided

## Backward Compatibility

This fix maintains backward compatibility:
- `MsTeamsClient` can still be initialized with just an `integration` object
- Existing code paths will continue to work (though may use wrong credentials in multi-org scenarios)
- The fix is opt-in: callers that pass `org_integration_id` get correct behavior

## Deployment Notes

- This fix should be deployed to all silos (region and control)
- No database migrations required
- No feature flags required
- Can be deployed without downtime

## Related Issues

- Fixes SENTRY-5HA4
- Related to #inc-649 (Slack integration had similar issue)
