#!/usr/bin/env python3
# mypy: ignore-errors
# flake8: noqa
"""
Demonstration of the fix for SENTRY-5KRQ.

This script shows how the defensive null-safety pattern prevents AttributeError
when event data fields are None due to upstream validation failures.
"""

import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from sentry.processing_errors.eap.producer import produce_processing_errors_to_eap  # noqa: E402


class MockEvent:
    """Mock event object for demonstration."""

    def __init__(self, data):
        self.data = data


def demonstrate_bug_scenario():
    """
    Demonstrate the bug scenario that would have caused AttributeError
    before the fix was applied.
    """
    print("=" * 70)
    print("DEMONSTRATION: AttributeError Fix for Processing Errors EAP")
    print("=" * 70)
    print()

    print("Scenario: Event with invalid contexts field")
    print("-" * 70)
    print()

    # This is the problematic event structure
    event_data = {
        "contexts": None,  # <- This is None because validation failed upstream
        "errors": [
            {"type": "invalid_data", "name": "contexts", "value": "expected an object"}
        ],
        "sdk": {"name": "sentry.python", "version": "1.0.0"},
    }

    print("Event data structure:")
    print(f"  contexts: {event_data['contexts']}")
    print(f"  errors: {event_data['errors']}")
    print(f"  sdk: {event_data['sdk']}")
    print()

    print("BEFORE the fix (pseudocode):")
    print("  contexts = event_data.get('contexts', {})")
    print("  # Returns None (not {}!) because 'contexts' key exists but value is None")
    print("  trace = contexts.get('trace', {})")
    print("  # FAILS! AttributeError: 'NoneType' object has no attribute 'get'")
    print()

    print("AFTER the fix (actual code):")
    print("  contexts = event_data.get('contexts') or {}")
    print("  # Returns {} because None is falsy, so 'or {}' provides fallback")
    print("  trace = contexts.get('trace') or {}")
    print("  # SUCCESS! Safely chains .get() calls")
    print()

    print("Testing the fix...")
    event = MockEvent(event_data)

    try:
        produce_processing_errors_to_eap(event)
        print("✓ Function executed successfully without AttributeError!")
        print("✓ The 'or {}' pattern prevented the crash")
    except AttributeError as e:
        print(f"✗ AttributeError occurred: {e}")
        print("✗ The fix is not working properly")
        return False

    print()
    print("=" * 70)
    print("SUCCESS: Fix is working correctly!")
    print("=" * 70)
    return True


def demonstrate_sdk_fix():
    """Demonstrate that the same fix was applied to SDK extraction."""
    print()
    print("=" * 70)
    print("BONUS: SDK Field Also Protected")
    print("=" * 70)
    print()

    event_data = {
        "contexts": {"trace": {"trace_id": "abc123"}},
        "errors": [
            {
                "type": "invalid_data",
                "name": "sdk",
            }
        ],
        "sdk": None,  # <- SDK is also None
    }

    print("Event data structure:")
    print(f"  contexts.trace.trace_id: {event_data['contexts']['trace']['trace_id']}")
    print(f"  sdk: {event_data['sdk']}")
    print()

    print("The same defensive pattern protects SDK extraction:")
    print("  sdk = event_data.get('sdk') or {}")
    print("  sdk_name = sdk.get('name')  # Safely returns None instead of crashing")
    print()

    event = MockEvent(event_data)

    try:
        produce_processing_errors_to_eap(event)
        print("✓ SDK None handling works correctly!")
    except AttributeError as e:
        print(f"✗ AttributeError occurred: {e}")
        return False

    return True


if __name__ == "__main__":
    success = demonstrate_bug_scenario()
    if success:
        demonstrate_sdk_fix()

    sys.exit(0 if success else 1)
