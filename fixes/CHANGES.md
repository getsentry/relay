# Detailed Changes for MS Teams Integration Fix

Fixes SENTRY-5HA4

## Overview

This fix resolves the "BotNotInConversationRoster" HTTP 403 error that occurs when sending MS Teams notifications in multi-organization setups.

## Changed Files

### 1. src/sentry/integrations/msteams/client.py

#### Change in `MsTeamsClient.__init__()`

**Before:**
```python
def __init__(self, integration: Integration | RpcIntegration):
    self.integration = integration
    self.metadata = self.integration.metadata
    self.base_url = self.metadata["service_url"].rstrip("/")
    org_integration_id = infer_org_integration(integration_id=integration.id)  # BUG: Wrong org
    super().__init__(org_integration_id=org_integration_id)
```

**After:**
```python
def __init__(
    self,
    integration: Integration | RpcIntegration,
    org_integration_id: int | None = None,
    organization_id: int | None = None,
):
    """
    Initialize MS Teams client with proper organization context.
    
    Args:
        integration: The MS Teams integration object
        org_integration_id: Optional. The specific org_integration ID to use.
        organization_id: Optional. The organization ID to look up the org_integration.
    """
    self.integration = integration
    self.metadata = self.integration.metadata
    self.base_url = self.metadata["service_url"].rstrip("/")

    # FIX: Determine the correct org_integration_id to use
    if org_integration_id is None and organization_id is not None:
        # Look up the org_integration for this specific organization
        org_integrations = integration_service.get_organization_integrations(
            integration_id=integration.id,
            organization_id=organization_id,
        )
        if org_integrations:
            org_integration_id = org_integrations[0].id

    # Fall back to infer_org_integration if still None
    if org_integration_id is None:
        org_integration_id = infer_org_integration(integration_id=integration.id)

    super().__init__(org_integration_id=org_integration_id)
```

**Impact:**
- Adds support for passing `organization_id` to look up the correct org_integration
- Adds support for passing `org_integration_id` directly
- Maintains backward compatibility by falling back to `infer_org_integration()`
- Ensures correct organization's credentials are used when proxying through control silo

---

### 2. src/sentry/integrations/msteams/notifications.py

#### Change in `send_notification_as_msteams()`

**Before (line ~127):**
```python
for channel, integration in integrations_by_channel.items():
    conversation_id = get_user_conversation_id(integration, channel)
    
    client = MsTeamsClient(integration)  # BUG: No org context
    try:
        with sentry_sdk.start_span(
            op="notification.send_msteams", name="notify_recipient"
        ):
            client.send_card(conversation_id, card)
```

**After:**
```python
for channel, integration in integrations_by_channel.items():
    conversation_id = get_user_conversation_id(integration, channel)
    
    # FIX: Pass organization_id to ensure correct credentials are used
    client = MsTeamsClient(
        integration,
        organization_id=notification.organization.id,
    )
    try:
        with sentry_sdk.start_span(
            op="notification.send_msteams", name="notify_recipient"
        ):
            client.send_card(conversation_id, card)
```

**Impact:**
- Passes the correct organization ID from the notification context
- Ensures `MsTeamsClient` uses the organization's credentials, not a random organization's
- Fixes the root cause of the "BotNotInConversationRoster" error

---

### 3. src/sentry/shared_integrations/client/proxy.py (Optional Enhancement)

#### Change in `infer_org_integration()`

**Added logging and warnings:**

```python
if len(org_integrations) > 0:
    org_integration_id = org_integrations[0].id

    # IMPROVED: Warn when inferring with multiple organizations
    if len(org_integrations) > 1:
        logger.warning(
            "infer_org_integration_multiple_orgs_detected",
            extra={
                "integration_id": integration_id,
                "org_integration_id": org_integration_id,
                "total_org_integrations": len(org_integrations),
                "message": "Multiple organizations have this integration. "
                "Using first org_integration which may be incorrect. "
                "Caller should provide organization_id or org_integration_id.",
            },
        )
        # Track this anti-pattern in Sentry
        sentry_sdk.capture_message(
            "Integration client initialized without organization context",
            level="warning",
            extras={
                "integration_id": integration_id,
                "org_integration_count": len(org_integrations),
            },
        )
```

**Impact:**
- Helps identify other places in the codebase with similar issues
- Provides visibility into when the anti-pattern is being used
- No functional change, just improved observability

---

## Technical Details

### Root Cause

When a notification is sent from a region silo:

1. `send_notification_as_msteams()` creates `MsTeamsClient(integration)`
2. `MsTeamsClient.__init__()` calls `infer_org_integration(integration.id)`
3. `infer_org_integration()` queries all org_integrations for this integration
4. With multiple organizations, it returns `org_integrations[0].id` (wrong org!)
5. Request is proxied to control silo with wrong `org_integration_id`
6. Control silo fetches credentials for wrong organization
7. MS Teams API receives request with wrong bot credentials
8. MS Teams returns HTTP 403: "BotNotInConversationRoster"

### Solution

Pass the correct organization context through the call chain:

```
Notification (has organization.id)
  ↓
send_notification_as_msteams(notification)
  ↓ (pass organization.id)
MsTeamsClient(integration, organization_id=notification.organization.id)
  ↓ (lookup correct org_integration)
IntegrationProxyClient(org_integration_id=correct_id)
  ↓ (proxy with correct org_integration_id)
Control Silo (fetches correct credentials)
  ↓
MS Teams API ✓ Success
```

## Testing Recommendations

### Unit Tests
- Test `MsTeamsClient` with `organization_id` parameter
- Test `MsTeamsClient` with `org_integration_id` parameter
- Test backward compatibility (no parameters provided)
- Test `infer_org_integration` warning logic

### Integration Tests
1. Set up two organizations with MS Teams integration
2. Configure alert rule in Organization A
3. Trigger alert
4. Verify notification is sent successfully
5. Verify Organization A's credentials were used (check logs)

### Manual Testing
1. Create multiple orgs with MS Teams integration in staging
2. Send test notifications from each org
3. Verify all notifications succeed
4. Check logs for any `infer_org_integration_multiple_orgs_detected` warnings

## Rollout Plan

1. **Phase 1**: Deploy to staging
   - Test with multiple organizations
   - Verify no regressions

2. **Phase 2**: Deploy to production (canary)
   - Monitor error rates
   - Check for "BotNotInConversationRoster" errors
   - Verify no increase in other errors

3. **Phase 3**: Full production deployment
   - Monitor for 24-48 hours
   - Verify fix resolves the issue

## Metrics to Monitor

- `msteams.*.notification` metric counts
- HTTP 403 errors from MS Teams API
- `infer_org_integration_multiple_orgs_detected` log events
- General MS Teams notification success/failure rates

## Backward Compatibility

✓ Existing code that doesn't pass `organization_id` will continue to work
✓ No database migrations required
✓ No API changes required
✓ Can be deployed without feature flags

## Future Improvements

1. Audit other integration clients (Slack, PagerDuty, etc.) for similar issues
2. Consider deprecating `infer_org_integration()` entirely
3. Add lint rule to prevent new usages of `infer_org_integration()`
4. Update all call sites to pass explicit organization context
