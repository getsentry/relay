"""
Producer for processing errors sent to EAP (Error Analytics Platform).

This module handles the production of processing error events to the EAP system,
extracting relevant metadata from events while handling potentially corrupted or
stripped data structures.
"""

from typing import Any, Dict, Optional
import logging

logger = logging.getLogger(__name__)


def produce_processing_errors_to_eap(event: Any) -> None:
    """
    Produce processing errors to the Error Analytics Platform (EAP).
    
    This function extracts trace and SDK information from events that have
    processing errors. It must handle cases where event data structures have
    been corrupted or stripped during upstream validation.
    
    Args:
        event: The event object containing data and processing errors.
    """
    if not event:
        logger.debug("Skipping EAP processing error production: event is None")
        return
    
    if not hasattr(event, 'data'):
        logger.debug("Skipping EAP processing error production: event has no data attribute")
        return
    
    event_data = event.data
    
    if not event_data:
        logger.debug("Skipping EAP processing error production: event.data is None")
        return
    
    # Extract processing errors
    processing_errors = event_data.get('errors', [])
    
    if not processing_errors:
        logger.debug("Skipping EAP processing error production: no processing errors found")
        return
    
    # Defensive null-safety: event_data fields like 'contexts' may be None or missing
    # when upstream validation fails and strips invalid structures. Using `or {}`
    # ensures we can safely chain .get() calls even when the parent key exists but
    # has a None value (e.g., when 'contexts' field validation failed).
    contexts = event_data.get('contexts') or {}
    trace = contexts.get('trace') or {}
    trace_id = trace.get('trace_id')
    
    if not trace_id:
        logger.debug("Skipping EAP processing error production: missing trace_id")
        return
    
    # Extract SDK information with the same defensive pattern for consistency
    # SDK field may also be None/missing due to upstream validation failures
    sdk = event_data.get('sdk') or {}
    sdk_name = sdk.get('name')
    sdk_version = sdk.get('version')
    
    # Extract other relevant metadata
    platform = event_data.get('platform')
    release = event_data.get('release')
    environment = event_data.get('environment')
    
    # Build the EAP payload
    eap_payload = {
        'trace_id': trace_id,
        'processing_errors': processing_errors,
        'metadata': {
            'sdk_name': sdk_name,
            'sdk_version': sdk_version,
            'platform': platform,
            'release': release,
            'environment': environment,
        }
    }
    
    # Log the payload for debugging
    logger.info(
        "Producing processing error to EAP",
        extra={
            'trace_id': trace_id,
            'error_count': len(processing_errors),
        }
    )
    
    # TODO: Implement actual EAP publishing logic
    # This would typically send the payload to a Kafka topic or similar
    # For now, we just log it
    logger.debug("EAP payload prepared", extra={'payload': eap_payload})
