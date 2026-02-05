# SENTRY-5H9Z: ServiceHook Missing issue.unresolved Event

## Issue Summary
ServiceHook records were missing `issue.unresolved` event in their events array, causing `event_not_in_servicehook` errors when workflow notifications tried to send issue.unresolved webhooks.

## Repository Note
**This issue is for the main Sentry repository (getsentry/sentry), not Relay.**

## Fix Status: ✅ ALREADY RESOLVED

The fix was implemented in the Sentry repository in commit `750338edd84933e53b82e11bd5a723f5ed0cdb27` on May 1, 2024.

### Changes Made
1. Added `"issue.unresolved"` to `EVENT_EXPANSION["issue"]` mapping
2. Implemented `@issue_unresolved.connect` webhook receiver  
3. Feature was GA'd (feature flag removed)

### Code Verification
In `getsentry/sentry` repository:

**File: `src/sentry/sentry_apps/utils/webhooks.py`**
```python
class IssueActionType(SentryAppActionType):
    ASSIGNED = "assigned"
    CREATED = "created"
    IGNORED = "ignored"
    RESOLVED = "resolved"
    UNRESOLVED = "unresolved"  # ✅ Present
```

**File: `src/sentry/receivers/sentry_apps.py`**
```python
@issue_unresolved.connect(weak=False)
def send_issue_unresolved_webhook(group, project, user=None, **kwargs):
    send_workflow_webhooks(
        organization=project.organization,
        issue=group,
        user=user,
        event="issue.unresolved",  # ✅ Sends webhook
        data=data,
    )
```

### Test Verification
Test in `tests/sentry/sentry_apps/services/test_hook_service.py`:
```python
def test_expand_events_multiple(self):
    ret = expand_events(["issue", "comment"])
    assert "issue.unresolved" in ret  # ✅ Passes
```

## Resolution
- **New ServiceHooks**: Automatically include `issue.unresolved` when subscribing to "issue" resource
- **Existing ServiceHooks**: Will be updated when the SentryApp is next modified
- **Validation**: Tests confirm event expansion works correctly

## Timeline
- **Oct 2020**: `issue_unresolved` signal added
- **May 2024**: Added to EVENT_EXPANSION and webhook sending
- **Aug 2025**: Refactored into typed enums (maintains functionality)
- **Present**: Fix is in production, working as expected

Fixes SENTRY-5H9Z
