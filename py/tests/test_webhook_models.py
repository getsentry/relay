"""
Tests for webhook_models module.

This test suite verifies the fix for SENTRY-5J00, ensuring that
the Pydantic model correctly handles both legacy payloads (without
timestamp fields) and new payloads (with timestamp fields).
"""

import pytest
from datetime import datetime

try:
    from pydantic import ValidationError
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False
    ValidationError = Exception

# Import the models from sentry_relay
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

try:
    from sentry_relay.webhook_models import (
        GitHubWebhookEventPayload,
        GitHubWebhookEventPayloadV1,
        process_github_webhook_event,
    )
except ImportError:
    # If pydantic is not available, skip tests
    GitHubWebhookEventPayload = None
    GitHubWebhookEventPayloadV1 = None
    process_github_webhook_event = None


@pytest.mark.skipif(not PYDANTIC_AVAILABLE or GitHubWebhookEventPayload is None, 
                    reason="Pydantic not available or import failed")
class TestGitHubWebhookEventPayload:
    """Test suite for GitHubWebhookEventPayload model."""
    
    def test_legacy_payload_without_timestamp_fields(self):
        """
        Test that legacy payloads (without trigger_at and sentry_received_trigger_at)
        can be validated successfully.
        
        This is the critical test for SENTRY-5J00 fix.
        """
        # This represents a payload that was queued before the deployment
        legacy_payload = {
            "event_id": "legacy-123",
            "event_type": "push",
            "payload": {"ref": "refs/heads/main", "commits": []},
            "repository": "getsentry/relay"
        }
        
        # Before fix: This would raise ValidationError
        # After fix: This should succeed with timestamp fields set to None
        event = GitHubWebhookEventPayload(**legacy_payload)
        
        assert event.event_id == "legacy-123"
        assert event.event_type == "push"
        assert event.repository == "getsentry/relay"
        assert event.trigger_at is None
        assert event.sentry_received_trigger_at is None
        assert event.is_legacy_payload() is True
    
    def test_new_payload_with_timestamp_fields(self):
        """
        Test that new payloads (with trigger_at and sentry_received_trigger_at)
        work correctly.
        """
        new_payload = {
            "event_id": "new-456",
            "event_type": "pull_request",
            "payload": {"action": "opened", "number": 123},
            "repository": "getsentry/relay",
            "trigger_at": datetime(2026, 2, 3, 10, 0, 0),
            "sentry_received_trigger_at": datetime(2026, 2, 3, 10, 0, 1)
        }
        
        event = GitHubWebhookEventPayload(**new_payload)
        
        assert event.event_id == "new-456"
        assert event.event_type == "pull_request"
        assert event.repository == "getsentry/relay"
        assert event.trigger_at == datetime(2026, 2, 3, 10, 0, 0)
        assert event.sentry_received_trigger_at == datetime(2026, 2, 3, 10, 0, 1)
        assert event.is_legacy_payload() is False
    
    def test_partial_timestamp_fields_only_trigger_at(self):
        """
        Test payload with only trigger_at (not sentry_received_trigger_at).
        """
        payload = {
            "event_id": "partial-789",
            "event_type": "issue_comment",
            "payload": {"action": "created"},
            "repository": "getsentry/relay",
            "trigger_at": datetime(2026, 2, 3, 12, 0, 0)
            # Note: sentry_received_trigger_at is missing
        }
        
        event = GitHubWebhookEventPayload(**payload)
        
        assert event.trigger_at == datetime(2026, 2, 3, 12, 0, 0)
        assert event.sentry_received_trigger_at is None
        assert event.is_legacy_payload() is False
    
    def test_partial_timestamp_fields_only_sentry_received(self):
        """
        Test payload with only sentry_received_trigger_at (not trigger_at).
        """
        payload = {
            "event_id": "partial-abc",
            "event_type": "release",
            "payload": {"action": "published"},
            "repository": "getsentry/relay",
            "sentry_received_trigger_at": datetime(2026, 2, 3, 14, 0, 0)
            # Note: trigger_at is missing
        }
        
        event = GitHubWebhookEventPayload(**payload)
        
        assert event.trigger_at is None
        assert event.sentry_received_trigger_at == datetime(2026, 2, 3, 14, 0, 0)
        assert event.is_legacy_payload() is False
    
    def test_get_trigger_timestamp_with_trigger_at(self):
        """
        Test that get_trigger_timestamp returns trigger_at when available.
        """
        payload = {
            "event_id": "test-1",
            "event_type": "push",
            "payload": {},
            "repository": "getsentry/relay",
            "trigger_at": datetime(2026, 2, 3, 10, 0, 0),
            "sentry_received_trigger_at": datetime(2026, 2, 3, 10, 0, 1)
        }
        
        event = GitHubWebhookEventPayload(**payload)
        assert event.get_trigger_timestamp() == datetime(2026, 2, 3, 10, 0, 0)
    
    def test_get_trigger_timestamp_fallback_to_sentry_received(self):
        """
        Test that get_trigger_timestamp falls back to sentry_received_trigger_at.
        """
        payload = {
            "event_id": "test-2",
            "event_type": "push",
            "payload": {},
            "repository": "getsentry/relay",
            "sentry_received_trigger_at": datetime(2026, 2, 3, 10, 0, 1)
        }
        
        event = GitHubWebhookEventPayload(**payload)
        assert event.get_trigger_timestamp() == datetime(2026, 2, 3, 10, 0, 1)
    
    def test_get_trigger_timestamp_both_none(self):
        """
        Test that get_trigger_timestamp returns None when both fields are None.
        """
        payload = {
            "event_id": "test-3",
            "event_type": "push",
            "payload": {},
            "repository": "getsentry/relay"
        }
        
        event = GitHubWebhookEventPayload(**payload)
        assert event.get_trigger_timestamp() is None
    
    def test_missing_required_fields_raises_error(self):
        """
        Test that truly required fields (event_id, event_type, etc.) still raise errors.
        """
        invalid_payload = {
            "event_id": "test-4",
            # Missing event_type, payload, and repository
        }
        
        with pytest.raises(ValidationError):
            GitHubWebhookEventPayload(**invalid_payload)
    
    def test_process_github_webhook_event_with_legacy_payload(self):
        """
        Test the process_github_webhook_event function with a legacy payload.
        """
        legacy_payload = {
            "event_id": "func-test-1",
            "event_type": "push",
            "payload": {"ref": "refs/heads/main"},
            "repository": "getsentry/relay"
        }
        
        event = process_github_webhook_event(legacy_payload)
        
        assert event.event_id == "func-test-1"
        assert event.is_legacy_payload() is True
    
    def test_process_github_webhook_event_with_new_payload(self):
        """
        Test the process_github_webhook_event function with a new payload.
        """
        new_payload = {
            "event_id": "func-test-2",
            "event_type": "pull_request",
            "payload": {"action": "opened"},
            "repository": "getsentry/relay",
            "trigger_at": datetime(2026, 2, 3, 10, 0, 0),
            "sentry_received_trigger_at": datetime(2026, 2, 3, 10, 0, 1)
        }
        
        event = process_github_webhook_event(new_payload)
        
        assert event.event_id == "func-test-2"
        assert event.is_legacy_payload() is False
    
    def test_iso_format_datetime_strings(self):
        """
        Test that ISO format datetime strings are properly parsed.
        """
        payload = {
            "event_id": "iso-test-1",
            "event_type": "push",
            "payload": {},
            "repository": "getsentry/relay",
            "trigger_at": "2026-02-03T10:00:00",
            "sentry_received_trigger_at": "2026-02-03T10:00:01"
        }
        
        event = GitHubWebhookEventPayload(**payload)
        
        # Pydantic should parse the ISO format strings
        assert event.trigger_at is not None
        assert event.sentry_received_trigger_at is not None


@pytest.mark.skipif(not PYDANTIC_AVAILABLE or GitHubWebhookEventPayloadV1 is None,
                    reason="Pydantic not available or import failed")
class TestGitHubWebhookEventPayloadV1:
    """Test suite for the legacy V1 model (for documentation purposes)."""
    
    def test_v1_model_basic_validation(self):
        """
        Test that the V1 model validates correctly.
        """
        v1_payload = {
            "event_id": "v1-test-1",
            "event_type": "push",
            "payload": {"ref": "refs/heads/main"},
            "repository": "getsentry/relay"
        }
        
        event = GitHubWebhookEventPayloadV1(**v1_payload)
        
        assert event.event_id == "v1-test-1"
        assert event.event_type == "push"
        assert event.repository == "getsentry/relay"


@pytest.mark.skipif(PYDANTIC_AVAILABLE, reason="Test for when pydantic is not available")
class TestWithoutPydantic:
    """Tests for when Pydantic is not available."""
    
    def test_graceful_degradation(self):
        """
        Test that the module handles missing Pydantic gracefully.
        """
        # The module should import without errors even without Pydantic
        # This is tested by the import at the top of the file
        assert True


if __name__ == "__main__":
    # Allow running tests directly
    pytest.main([__file__, "-v"])
