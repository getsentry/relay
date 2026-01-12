# Generator Exhaustion Fix

## Issue Summary

This document describes a common Python bug related to generator exhaustion that was identified in the raven-python SDK, and provides a general solution applicable to any Python codebase.

## Problem Description

### Root Cause

In the original raven-python SDK, the `get_stack_info` function was attempting to iterate over an `iter_stack_frames` generator multiple times. Since generators can only be iterated once, subsequent iterations would fail or return empty results.

### Stacktrace Analysis

```
main → send_test_message → captureMessage → capture → build_msg
```

The issue occurred in `build_msg` at this line:

```python
data.update({
    'sentry.interfaces.Stacktrace': {
        'frames': get_stack_info(frames,  # frames is a generator
            transformer=self.transform)
    },
})
```

Where `frames` was a generator object from `iter_stack_frames()`.

### Why It Failed

1. **Generator Creation**: `iter_stack_frames()` returns a generator object
2. **First Iteration**: Inside `get_stack_info`, the generator is iterated (e.g., for validation or counting)
3. **Generator Exhaustion**: The generator is now exhausted and cannot yield any more values
4. **Second Iteration**: When `get_stack_info` tries to iterate again to actually process frames, no values are yielded
5. **Failure**: The result is empty or an error occurs

## Solution

### The Fix

Convert the generator to a list before passing it to functions that may iterate multiple times:

```python
# BEFORE (Broken):
frames = iter_stack_frames()  # Returns a generator
data.update({
    'sentry.interfaces.Stacktrace': {
        'frames': get_stack_info(frames, transformer=self.transform)
    },
})

# AFTER (Fixed):
frames = iter_stack_frames()  # Returns a generator
if not isinstance(frames, list):
    frames = list(frames)  # Convert to list
data.update({
    'sentry.interfaces.Stacktrace': {
        'frames': get_stack_info(frames, transformer=self.transform)
    },
})
```

### Alternative Approaches

#### 1. Convert at the Call Site (Recommended)

```python
def build_msg(self, event_type, data=None, stack=None, **kwargs):
    if stack:
        frames = stack
        # Convert generator to list immediately
        if not isinstance(frames, list):
            frames = list(frames)
        
        data.update({
            'sentry.interfaces.Stacktrace': {
                'frames': get_stack_info(frames, transformer=self.transform)
            },
        })
    return data
```

#### 2. Convert Inside the Function

```python
def get_stack_info(frames, transformer=None):
    # Convert generator to list at the start
    if not isinstance(frames, list):
        frames = list(frames)
    
    # Now safe to iterate multiple times
    for frame in frames:
        # ... process frames
    
    return processed_frames
```

#### 3. Use itertools.tee for Multiple Iterations

If you need to preserve the lazy evaluation of generators:

```python
from itertools import tee

def get_stack_info(frames, transformer=None):
    # Create two independent iterators
    frames_iter1, frames_iter2 = tee(frames, 2)
    
    # First iteration
    count = sum(1 for _ in frames_iter1)
    
    # Second iteration
    result = [process(frame) for frame in frames_iter2]
    
    return result
```

## Best Practices

### 1. Document Generator Behavior

```python
def iter_stack_frames():
    """
    Returns a generator that yields stack frames.
    
    Note: This is a generator and can only be iterated once.
    If you need to iterate multiple times, convert to a list first:
        frames = list(iter_stack_frames())
    """
    # ... generator code
```

### 2. Type Hints

```python
from typing import Iterator, List, Union

def get_stack_info(
    frames: Union[Iterator, List],
    transformer=None
) -> List[dict]:
    """Process stack frames."""
    if not isinstance(frames, list):
        frames = list(frames)
    # ... process frames
```

### 3. Early Conversion

When you know a function will need multiple iterations, convert immediately:

```python
# Good: Convert early
frames = list(iter_stack_frames())
result = get_stack_info(frames)

# Avoid: Passing generator when multiple iterations are needed
frames = iter_stack_frames()  # Generator
result = get_stack_info(frames)  # May fail if iterates multiple times
```

## Testing

The fix includes comprehensive tests in `tests/integration/test_generator_exhaustion_fix.py`:

1. **Bug Demonstration**: Shows how the bug manifests
2. **Fix Verification**: Proves the fix works
3. **Edge Cases**: Tests with lists, transformers, and various input types

Run tests:
```bash
python tests/integration/test_generator_exhaustion_fix.py
```

Or with pytest:
```bash
pytest tests/integration/test_generator_exhaustion_fix.py -v
```

## Prevention

### Code Review Checklist

- [ ] Are generators being passed to functions?
- [ ] Does the receiving function iterate multiple times?
- [ ] Is there explicit documentation about single-use iteration?
- [ ] Are there type hints indicating Iterator vs List?

### Static Analysis

Use tools like `pylint` or `mypy` to detect potential issues:

```python
# .pylintrc
[TYPECHECK]
# Check for generator exhaustion patterns
```

## Impact

This fix:
- ✅ Prevents generator exhaustion errors
- ✅ Ensures stack traces are captured correctly
- ✅ Maintains backward compatibility (works with both generators and lists)
- ✅ Has minimal performance impact (one-time list conversion)

## References

- Python Generators: https://docs.python.org/3/howto/functional.html#generators
- itertools.tee: https://docs.python.org/3/library/itertools.html#itertools.tee
- Original Issue: Raven-python SDK generator exhaustion in stack trace collection
