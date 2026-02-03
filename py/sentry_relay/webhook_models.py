"""
Pydantic models for processing GitHub webhook events.

This module contains the data models used for validating and processing
GitHub webhook payloads in the event processing pipeline.

BACKWARD COMPATIBILITY FIX (SENTRY-5J00):
The fields `trigger_at` and `sentry_received_trigger_at` were initially
added as required fields in commit f5076a2, which caused validation failures
for in-flight tasks that were queued before the deployment.

To maintain backward compatibility, these fields are now optional with
sensible defaults, allowing older payloads to be processed successfully.
"""

from datetime import datetime
from typing import Any, Dict, Optional

try:
    from pydantic import BaseModel, Field
except ImportError:
    # Fallback for environments without pydantic
    class BaseModel:
        """Stub BaseModel for environments without pydantic."""
        pass
    
    def Field(*args, **kwargs):
        """Stub Field function."""
        return None


class GitHubWebhookEventPayload(BaseModel):
    """
    Model for GitHub webhook event payloads.
    
    This model validates the structure of GitHub webhook events
    before they are processed by the event pipeline.
    
    IMPORTANT: When adding new fields to this model, ensure backward
    compatibility by making them optional with appropriate defaults.
    In-flight tasks may not contain newly added fields.
    """
    
    # Original fields that were always present
    event_id: str = Field(..., description="Unique identifier for the event")
    event_type: str = Field(..., description="Type of GitHub webhook event")
    payload: Dict[str, Any] = Field(..., description="The actual webhook payload from GitHub")
    repository: str = Field(..., description="Repository identifier")
    
    # Fields added later - MUST be optional for backward compatibility
    # These fields were made required in commit f5076a2, causing the validation error
    trigger_at: Optional[datetime] = Field(
        default=None,
        description="Timestamp when the webhook was triggered. Defaults to None for legacy payloads."
    )
    
    sentry_received_trigger_at: Optional[datetime] = Field(
        default=None,
        description="Timestamp when Sentry received the webhook. Defaults to None for legacy payloads."
    )
    
    class Config:
        """Pydantic model configuration."""
        # Allow arbitrary types for flexibility
        arbitrary_types_allowed = True
        # Validate on assignment to catch errors early
        validate_assignment = True
        # Use JSON encoders for datetime
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None
        }
    
    def get_trigger_timestamp(self) -> Optional[datetime]:
        """
        Get the trigger timestamp, falling back to sentry_received_trigger_at if available.
        
        Returns:
            The trigger timestamp or None if both fields are None.
        """
        return self.trigger_at or self.sentry_received_trigger_at
    
    def is_legacy_payload(self) -> bool:
        """
        Check if this is a legacy payload (created before the timestamp fields were added).
        
        Returns:
            True if both timestamp fields are None, False otherwise.
        """
        return self.trigger_at is None and self.sentry_received_trigger_at is None


class GitHubWebhookEventPayloadV1(BaseModel):
    """
    Legacy model for GitHub webhook event payloads (before timestamp fields).
    
    This model represents the payload structure before the addition of
    trigger_at and sentry_received_trigger_at fields. It's kept for
    documentation and migration purposes.
    
    DEPRECATED: Use GitHubWebhookEventPayload instead, which includes
    backward-compatible optional timestamp fields.
    """
    
    event_id: str
    event_type: str
    payload: Dict[str, Any]
    repository: str


# Example usage and validation
def process_github_webhook_event(event_payload: Dict[str, Any]) -> GitHubWebhookEventPayload:
    """
    Process a GitHub webhook event payload.
    
    This function validates the payload structure and handles both legacy
    payloads (without timestamp fields) and new payloads (with timestamp fields).
    
    Args:
        event_payload: Raw event payload dictionary
        
    Returns:
        Validated GitHubWebhookEventPayload instance
        
    Raises:
        ValidationError: If the payload structure is invalid
        
    Example:
        # Legacy payload (before fix) - will now work
        >>> old_payload = {
        ...     "event_id": "123",
        ...     "event_type": "push",
        ...     "payload": {"ref": "refs/heads/main"},
        ...     "repository": "getsentry/relay"
        ... }
        >>> event = process_github_webhook_event(old_payload)
        >>> event.is_legacy_payload()
        True
        
        # New payload (after fix) - works as expected
        >>> new_payload = {
        ...     "event_id": "456",
        ...     "event_type": "pull_request",
        ...     "payload": {"action": "opened"},
        ...     "repository": "getsentry/relay",
        ...     "trigger_at": "2026-02-03T10:00:00Z",
        ...     "sentry_received_trigger_at": "2026-02-03T10:00:01Z"
        ... }
        >>> event = process_github_webhook_event(new_payload)
        >>> event.is_legacy_payload()
        False
    """
    try:
        return GitHubWebhookEventPayload(**event_payload)
    except Exception:
        # Pydantic not available or validation failed
        # In production, proper error handling would be implemented
        raise


__all__ = [
    "GitHubWebhookEventPayload",
    "GitHubWebhookEventPayloadV1",
    "process_github_webhook_event",
]
