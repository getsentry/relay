# How to Apply This Fix to Sentry

This directory contains the fix for MS Teams integration "BotNotInConversationRoster" error (SENTRY-5HA4).

## Quick Start

Since this fix was developed in the Relay repository but targets the Sentry application, you need to copy these files to the Sentry repository:

### Step 1: Copy Fixed Files

```bash
# From the Sentry repository root:

# Copy the fixed files
cp /path/to/relay/fixes/src/sentry/integrations/msteams/client.py \
   src/sentry/integrations/msteams/client.py

cp /path/to/relay/fixes/src/sentry/integrations/msteams/notifications.py \
   src/sentry/integrations/msteams/notifications.py

cp /path/to/relay/fixes/src/sentry/shared_integrations/client/proxy.py \
   src/sentry/shared_integrations/client/proxy.py

# Copy the tests
cp /path/to/relay/fixes/tests/test_msteams_org_context.py \
   tests/sentry/integrations/msteams/test_org_context.py
```

### Step 2: Run Tests

```bash
# Run the new tests
pytest tests/sentry/integrations/msteams/test_org_context.py -v

# Run existing MS Teams tests to verify no regressions
pytest tests/sentry/integrations/msteams/ -v
```

### Step 3: Verify Locally

```bash
# Start Sentry in development mode
sentry devserver

# Test with multiple organizations:
# 1. Set up MS Teams integration in org 1
# 2. Set up MS Teams integration in org 2  
# 3. Create alert rule in org 1
# 4. Trigger the alert
# 5. Verify notification is sent successfully
```

### Step 4: Create Pull Request

```bash
git checkout -b fix/msteams-org-context-sentry-5ha4
git add src/sentry/integrations/msteams/client.py
git add src/sentry/integrations/msteams/notifications.py
git add src/sentry/shared_integrations/client/proxy.py
git add tests/sentry/integrations/msteams/test_org_context.py
git commit -m "fix(integrations): Use correct org credentials for MS Teams notifications

Fixes SENTRY-5HA4

When multiple organizations have MS Teams integrations configured,
notifications were failing with 'BotNotInConversationRoster' HTTP 403
errors because the wrong organization's credentials were being used.

Changes:
- MsTeamsClient now accepts organization_id parameter
- send_notification_as_msteams passes organization context
- infer_org_integration logs warnings for multi-org scenarios

The fix ensures that when proxying requests through control silo,
the correct organization's MS Teams bot credentials are used."

git push origin fix/msteams-org-context-sentry-5ha4
```

## PR Description Template

```markdown
## Problem

MS Teams alert notifications fail with `BotNotInConversationRoster` error when sending to a conversation in multi-organization setups.

**Root Cause**: When proxying requests through control silo, `infer_org_integration()` returns the first org_integration record instead of the specific organization's record. This causes the wrong organization's MS Teams bot credentials to be used.

## Solution

1. **Updated `MsTeamsClient`** to accept `organization_id` parameter
2. **Updated `send_notification_as_msteams`** to pass organization context
3. **Improved logging** in `infer_org_integration` to detect this anti-pattern

## Testing

- [ ] Manually tested with multiple organizations
- [ ] Added unit tests for organization context handling
- [ ] Verified backward compatibility
- [ ] Checked that notifications succeed with correct credentials

## Checklist

- [ ] Tests pass locally
- [ ] No new lint errors
- [ ] Documentation updated (if needed)
- [ ] Backward compatible

Fixes SENTRY-5HA4
```

## Diff Summary

### client.py
- Added `org_integration_id` and `organization_id` parameters to `__init__()`
- Added logic to look up correct org_integration when `organization_id` is provided
- Maintains backward compatibility with fallback to `infer_org_integration()`

### notifications.py
- Changed `MsTeamsClient(integration)` to `MsTeamsClient(integration, organization_id=notification.organization.id)`
- One line change with massive impact!

### proxy.py (optional)
- Added warning logging when `infer_org_integration()` is used with multiple organizations
- Added Sentry error tracking for this anti-pattern
- Helps identify other places with similar issues

## File Locations in Sentry Repo

- `src/sentry/integrations/msteams/client.py`
- `src/sentry/integrations/msteams/notifications.py`
- `src/sentry/shared_integrations/client/proxy.py`
- `tests/sentry/integrations/msteams/test_org_context.py` (new file)
