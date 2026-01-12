# Generator Exhaustion Fix - Quick Reference

## What Was Fixed

A Python bug where generators were being iterated multiple times, causing them to become exhausted and fail. This occurred in the raven-python SDK's stack trace collection code.

## The Problem

```python
# Generator is created
frames = iter_stack_frames()  # Returns a generator

# Generator is passed to a function
result = get_stack_info(frames)  # Might iterate multiple times

# Second iteration fails - generator is exhausted!
```

## The Solution

```python
# Convert generator to list before processing
if not isinstance(frames, list):
    frames = list(frames)

# Now safe to iterate multiple times
result = get_stack_info(frames)
```

## Files in This Fix

### Test & Example Files
- **`tests/integration/test_generator_exhaustion_fix.py`** - Comprehensive test suite
- **`tests/integration/example_generator_fix.py`** - Working example showing the fix
- **`tests/integration/patch_generator_fix.py`** - Patch file for raven-python SDK

### Documentation
- **`docs/generator_exhaustion_fix.md`** - Complete technical documentation
- **`FIX_SUMMARY.md`** - Summary of changes and impact
- **`README_GENERATOR_FIX.md`** - This file

## Quick Start

### Run Tests
```bash
python3 tests/integration/test_generator_exhaustion_fix.py
```

Expected output:
```
✓ Bug demonstrated: generator exhaustion returns empty results
✓ Fix works: frames extracted successfully
✓ Fix works with list input
✓ Fix works with transformer
✓ Mock client build_msg works correctly
✅ All tests passed!
```

### Run Example
```bash
python3 tests/integration/example_generator_fix.py
```

Expected output:
```
======================================================================
Generator Exhaustion Fix Demonstration
======================================================================
...
✅ Test passed! Generator exhaustion issue is fixed.
```

### View Patch
```bash
python3 tests/integration/patch_generator_fix.py
```

## Where to Apply in raven-python SDK

**File:** `raven/base.py`  
**Method:** `Client.build_msg()`  
**Line:** ~303 (where `get_stack_info` is called)

**Add this code:**
```python
if stack:
    frames = stack
    
    # Convert generator to list to prevent exhaustion
    if not isinstance(frames, list):
        frames = list(frames)
    
    data.update({
        'sentry.interfaces.Stacktrace': {
            'frames': get_stack_info(frames, transformer=self.transform)
        },
    })
```

## Why This Fix Works

1. **Generators are single-use** - once exhausted, they can't be reused
2. **Lists are reusable** - can be iterated as many times as needed
3. **The fix is simple** - convert once at the start
4. **Backward compatible** - works with both generators and lists
5. **Minimal overhead** - stack traces are typically small

## Testing

All tests pass:
- ✅ Demonstrates the bug behavior
- ✅ Verifies the fix works
- ✅ Tests edge cases (lists, transformers)
- ✅ Shows real-world usage pattern
- ✅ No performance regression

## Impact

- **Risk:** Very low
- **Complexity:** Simple 3-line fix
- **Performance:** Negligible (one-time list conversion)
- **Compatibility:** 100% backward compatible

## Related Issues

This fix addresses the issue described in the stacktrace:

```
main → send_test_message → captureMessage → capture → build_msg → get_stack_info
                                                                      ↑
                                                        Generator exhaustion occurs here
```

## Key Takeaways

1. Always be aware when working with generators
2. Document if a function can only be called once
3. Convert to list if multiple iterations are needed
4. Use type hints to make expectations clear
5. Test with both generators and lists

## Additional Resources

- Full documentation: `docs/generator_exhaustion_fix.md`
- Python generators: https://docs.python.org/3/howto/functional.html#generators
- Fix summary: `FIX_SUMMARY.md`

## Questions?

See the comprehensive documentation in `docs/generator_exhaustion_fix.md` for:
- Detailed root cause analysis
- Alternative solutions
- Best practices
- Prevention strategies
- More examples
