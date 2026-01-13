# Fix for Arroyo MetricsBuffer Race Condition

## Issue Description

**RuntimeError: dictionary changed size during iteration** occurring in `arroyo/processing/processor.py` at line 133 in the `MetricsBuffer.flush()` method.

### Root Cause

The `MetricsBuffer` class had a thread-safety issue where:

1. The `flush()` method iterates over `self.__timers` and `self.__counters` dictionaries
2. While iterating, another thread can call `incr_counter()` or `incr_timing()`, modifying these dictionaries
3. Python raises `RuntimeError` when a dictionary is modified during iteration

This race condition occurs in multi-threaded environments, particularly when:
- The stuck detector thread calls `incr_counter("arroyo.consumer.stuck", 1)`
- This triggers `__throttled_record()` which calls `flush()`
- Meanwhile, another thread also modifies the metrics dictionaries

### The Fix

The fix modifies the `flush()` method to:

1. **Create snapshots** of both dictionaries before iteration: `list(self.__timers.items())`
2. **Clear dictionaries immediately** by calling `self.__reset()` before iteration
3. **Iterate over snapshots** instead of live dictionaries

This ensures thread-safe operation by preventing concurrent modification during iteration.

## Files Changed

- `arroyo/processing/processor.py` - Modified `MetricsBuffer.flush()` method

## Patch File

The patch file `arroyo_processor_fix.patch` contains the changes that need to be applied to the arroyo package.

## Testing

Run `test_arroyo_fix.py` to verify the fix:

```bash
python3 test_arroyo_fix.py
```

The test:
- Simulates concurrent access from multiple threads
- Performs 6000+ operations across 6 threads
- Verifies no race condition errors occur

## Applying the Fix

### Option 1: Patch Installed Package

```bash
# Apply the patch to your installed arroyo package
cd $(python3 -c "import arroyo, os; print(os.path.dirname(os.path.dirname(arroyo.__file__)))")
patch -p1 < /path/to/arroyo_processor_fix.patch
```

### Option 2: Upstream Fix

This fix should be submitted as a pull request to the [sentry-arroyo repository](https://github.com/getsentry/arroyo).

## Impact

- **Severity**: High - Causes consumer crashes in production
- **Frequency**: Intermittent - Race condition dependent on timing
- **Affected Systems**: Any system using arroyo's StreamProcessor with stuck detection enabled

## Related Information

- Error first observed: 2026-01-13 15:15:40 UTC
- Stack trace shows error in basic_consumer processing
- Triggered by stuck detector monitoring thread
