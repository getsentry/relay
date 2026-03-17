"""
Test for the fix: send_webhooks gracefully handles missing ServiceHook
when SentryAppInstallation is deleted (cascade deletes ServiceHook).

This test validates the fix in src/sentry/sentry_apps/tasks/sentry_apps.py
where send_webhooks now records a HALT instead of raising a FAILURE when
the ServiceHook is missing. This handles the race condition where:

1. SentryAppInstallation is deleted (uninstall)
2. ServiceHook is cascade-deleted
3. A stale webhook task (e.g. workflow_notification) still runs
4. send_webhooks calls _load_service_hook which returns None
5. Previously: raised SentryAppSentryError (FAILURE)
6. Now: records halt with MISSING_INSTALLATION reason and returns gracefully

The updated test_does_not_send_if_no_service_hook_exists test expects:
- A HALT metric (not FAILURE) with MISSING_INSTALLATION reason
- EventLifecycleOutcome.HALTED count of 1 (not FAILURE count of 1)

Original test assertion (BEFORE fix):
    assert_failure_metric(
        mock_record,
        SentryAppSentryError(SentryAppWebhookFailureReason.MISSING_SERVICEHOOK)
    )
    assert_count_of_metric(
        mock_record=mock_record,
        outcome=EventLifecycleOutcome.FAILURE,
        outcome_count=1
    )

Updated test assertion (AFTER fix):
    assert_halt_metric(
        mock_record,
        SentryAppWebhookHaltReason.MISSING_INSTALLATION
    )
    assert_count_of_metric(
        mock_record=mock_record,
        outcome=EventLifecycleOutcome.HALTED,
        outcome_count=1
    )
"""
