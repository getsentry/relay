"""
Tests for processing errors EAP producer.

These tests verify that the produce_processing_errors_to_eap function
handles corrupted/stripped event data structures safely.
"""

import pytest
import sys
import os

# Add src to path so we can import the module
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from sentry.processing_errors.eap.producer import produce_processing_errors_to_eap


class MockEvent:
    """Mock event object for testing."""
    def __init__(self, data):
        self.data = data


def test_produce_processing_errors_with_none_contexts():
    """
    Test that the function handles None contexts safely.
    
    This is the primary bug fix - when contexts validation fails upstream,
    event_data.get('contexts') returns None instead of a dict, causing
    .get('trace', {}) to fail when called on None.
    """
    event = MockEvent({
        'contexts': None,  # This is the problematic case
        'errors': [
            {
                'type': 'invalid_data',
                'name': 'contexts',
                'value': 'expected an object'
            }
        ],
        'sdk': {
            'name': 'sentry.python',
            'version': '1.0.0'
        }
    })
    
    # Should not raise an exception
    produce_processing_errors_to_eap(event)


def test_produce_processing_errors_with_missing_contexts():
    """Test that the function handles missing contexts key."""
    event = MockEvent({
        'errors': [
            {
                'type': 'invalid_data',
                'name': 'contexts',
            }
        ],
        'sdk': {
            'name': 'sentry.python',
            'version': '1.0.0'
        }
    })
    
    # Should not raise an exception
    produce_processing_errors_to_eap(event)


def test_produce_processing_errors_with_none_trace():
    """Test that the function handles None trace within contexts."""
    event = MockEvent({
        'contexts': {
            'trace': None  # Trace can also be None
        },
        'errors': [
            {
                'type': 'invalid_data',
                'name': 'trace',
            }
        ],
        'sdk': {
            'name': 'sentry.python',
            'version': '1.0.0'
        }
    })
    
    # Should not raise an exception
    produce_processing_errors_to_eap(event)


def test_produce_processing_errors_with_none_sdk():
    """
    Test that the function handles None sdk safely.
    
    SDK field can also be corrupted/stripped, so we apply the same
    defensive pattern for consistency.
    """
    event = MockEvent({
        'contexts': {
            'trace': {
                'trace_id': 'abc123'
            }
        },
        'errors': [
            {
                'type': 'invalid_data',
                'name': 'sdk',
            }
        ],
        'sdk': None  # SDK can be None
    })
    
    # Should not raise an exception
    produce_processing_errors_to_eap(event)


def test_produce_processing_errors_with_valid_data():
    """Test that the function works correctly with valid data."""
    event = MockEvent({
        'contexts': {
            'trace': {
                'trace_id': 'abc123',
                'span_id': 'def456'
            }
        },
        'errors': [
            {
                'type': 'invalid_data',
                'name': 'some_field',
            }
        ],
        'sdk': {
            'name': 'sentry.python',
            'version': '1.0.0'
        },
        'platform': 'python',
        'release': '1.0.0',
        'environment': 'production'
    })
    
    # Should not raise an exception
    produce_processing_errors_to_eap(event)


def test_produce_processing_errors_skips_when_no_trace_id():
    """Test that the function returns early when trace_id is missing."""
    event = MockEvent({
        'contexts': {
            'trace': {
                # No trace_id
                'span_id': 'def456'
            }
        },
        'errors': [
            {
                'type': 'invalid_data',
                'name': 'some_field',
            }
        ]
    })
    
    # Should return early without raising
    produce_processing_errors_to_eap(event)


def test_produce_processing_errors_skips_when_no_errors():
    """Test that the function returns early when no processing errors exist."""
    event = MockEvent({
        'contexts': {
            'trace': {
                'trace_id': 'abc123'
            }
        },
        'errors': []  # No errors
    })
    
    # Should return early without raising
    produce_processing_errors_to_eap(event)


def test_produce_processing_errors_with_none_event_data():
    """Test that the function handles None event.data safely."""
    event = MockEvent(None)
    
    # Should return early without raising
    produce_processing_errors_to_eap(event)


def test_produce_processing_errors_with_none_event():
    """Test that the function handles None event safely."""
    # Should return early without raising
    produce_processing_errors_to_eap(None)


def test_produce_processing_errors_chained_none_values():
    """
    Test the specific bug scenario: contexts exists but is None,
    and we try to chain .get() calls.
    """
    event = MockEvent({
        'contexts': None,
        'sdk': None,
        'errors': [
            {
                'type': 'invalid_data',
                'name': 'contexts',
                'value': 'expected an object'
            }
        ]
    })
    
    # This would fail with AttributeError before the fix:
    # AttributeError: 'NoneType' object has no attribute 'get'
    # After the fix with `or {}`, it should work fine
    produce_processing_errors_to_eap(event)


if __name__ == '__main__':
    # Run tests
    pytest.main([__file__, '-v'])
