# Processing Errors EAP Producer

This module handles the production of processing error events to the Error Analytics Platform (EAP).

## Overview

The `produce_processing_errors_to_eap` function extracts trace and SDK information from events that have processing errors and prepares them for sending to the EAP system. It must handle cases where event data structures have been corrupted or stripped during upstream validation.

## Key Features

### Defensive Null-Safety

The function implements defensive null-safety patterns to handle corrupted event data:

```python
# Defensive pattern for contexts extraction
contexts = event_data.get('contexts') or {}
trace = contexts.get('trace') or {}
trace_id = trace.get('trace_id')

# Defensive pattern for SDK extraction
sdk = event_data.get('sdk') or {}
sdk_name = sdk.get('name')
```

### Why the `or {}` Pattern?

When upstream validation fails, fields like `contexts` or `sdk` may exist in `event_data` but have a `None` value instead of a dictionary. The standard pattern `event_data.get('contexts', {})` does NOT help in this case because:

- `get('contexts', {})` only returns `{}` when the key is **missing**
- When the key **exists** but has value `None`, it returns `None`
- Calling `.get()` on `None` raises `AttributeError`

The `or {}` pattern solves this:

```python
event_data.get('contexts') or {}
# If 'contexts' is None or missing, returns {}
# If 'contexts' is a dict, returns the dict
```

## Common Scenarios

### Scenario 1: Invalid Contexts Field

```python
event.data = {
    'contexts': None,  # Stripped due to validation failure
    'errors': [{'type': 'invalid_data', 'name': 'contexts'}]
}
```

**Without fix**: `AttributeError: 'NoneType' object has no attribute 'get'`  
**With fix**: Safely returns early with "missing trace_id" message

### Scenario 2: Invalid SDK Field

```python
event.data = {
    'contexts': {'trace': {'trace_id': 'abc123'}},
    'sdk': None,  # Stripped due to validation failure
    'errors': [{'type': 'invalid_data', 'name': 'sdk'}]
}
```

**Without fix**: `AttributeError: 'NoneType' object has no attribute 'get'`  
**With fix**: Safely extracts trace_id, sdk_name and sdk_version are None

## Testing

Run the test suite to verify the fix handles all edge cases:

```bash
pytest tests/test_processing_errors_eap.py -v
```

Run the demonstration script to see the fix in action:

```bash
python3 demo_fix.py
```

## Related Issues

- **SENTRY-5KRQ**: AttributeError in processing errors EAP producer when contexts is None
