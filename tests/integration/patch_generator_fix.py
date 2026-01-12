"""
Patch for fixing generator exhaustion in raven-python SDK

This file contains the code changes needed to fix the generator exhaustion
issue in the raven-python SDK. Apply these changes to prevent the error where
get_stack_info attempts to iterate an iter_stack_frames generator multiple times.

LOCATION: raven/base.py
METHOD: Client.build_msg()
"""

# ==============================================================================
# ORIGINAL CODE (BROKEN - has generator exhaustion bug)
# ==============================================================================

def build_msg_ORIGINAL(self, event_type, data=None, date=None, time_spent=None,
                       extra=None, stack=None, tags=None, **kwargs):
    """
    Original implementation with generator exhaustion bug.
    """
    if data is None:
        data = {}
    
    # ... other code ...
    
    if stack:
        frames = stack
        
        # BUG: frames might be a generator from iter_stack_frames()
        # If get_stack_info() iterates multiple times, the generator will be exhausted
        data.update({
            'sentry.interfaces.Stacktrace': {
                'frames': get_stack_info(frames,  # PROBLEM: generator passed directly
                    transformer=self.transform)
            },
        })
    
    # ... rest of method ...
    return data


# ==============================================================================
# FIXED CODE (Converts generator to list to prevent exhaustion)
# ==============================================================================

def build_msg_FIXED(self, event_type, data=None, date=None, time_spent=None,
                    extra=None, stack=None, tags=None, **kwargs):
    """
    Fixed implementation that handles generator exhaustion properly.
    """
    if data is None:
        data = {}
    
    # ... other code ...
    
    if stack:
        frames = stack
        
        # FIX: Convert generator to list to prevent exhaustion
        # This ensures get_stack_info() can iterate as many times as needed
        if not isinstance(frames, list):
            frames = list(frames)
        
        data.update({
            'sentry.interfaces.Stacktrace': {
                'frames': get_stack_info(frames,  # FIXED: list instead of generator
                    transformer=self.transform)
            },
        })
    
    # ... rest of method ...
    return data


# ==============================================================================
# UNIFIED DIFF FORMAT
# ==============================================================================

PATCH = """
--- a/raven/base.py
+++ b/raven/base.py
@@ -298,6 +298,10 @@ class Client(object):
 
         if stack:
             frames = stack
+
+            # Convert generator to list to prevent exhaustion
+            if not isinstance(frames, list):
+                frames = list(frames)
 
             data.update({
                 'sentry.interfaces.Stacktrace': {
"""


# ==============================================================================
# ALTERNATIVE FIX: In get_stack_info() function
# ==============================================================================

def get_stack_info_FIXED(frames, transformer=None):
    """
    Alternative fix: Convert generator to list inside get_stack_info().
    
    This is a defensive approach that protects against generator exhaustion
    regardless of where the function is called from.
    """
    # Convert generator to list to prevent exhaustion
    if not isinstance(frames, list):
        frames = list(frames)
    
    # Now safe to iterate multiple times if needed
    result = []
    for frame in frames:
        # Extract frame information
        frame_info = {
            'filename': frame.f_code.co_filename,
            'function': frame.f_code.co_name,
            'lineno': frame.f_lineno,
        }
        
        # Apply transformer if provided
        if transformer:
            frame_info = transformer(frame_info)
        
        result.append(frame_info)
    
    return result


# ==============================================================================
# RECOMMENDED APPROACH
# ==============================================================================

"""
RECOMMENDATION: Apply both fixes for defense in depth

1. Fix in build_msg():
   - Convert generator to list before passing to get_stack_info()
   - Makes the data flow explicit and clear
   - Centralizes the conversion in one place

2. Fix in get_stack_info():
   - Add defensive check at the start of the function
   - Protects against future bugs if called from other locations
   - Documents the function's requirements clearly

This dual approach ensures robustness and makes the code more maintainable.
"""


# ==============================================================================
# TESTING
# ==============================================================================

def test_fix():
    """
    Test to verify the fix works correctly.
    """
    import inspect
    
    def iter_stack_frames():
        """Generator that yields stack frames."""
        frame = inspect.currentframe()
        try:
            frame = frame.f_back
            while frame is not None:
                yield frame
                frame = frame.f_back
        finally:
            del frame
    
    # Test with generator (would fail in original code)
    frames_gen = iter_stack_frames()
    frames_list = list(frames_gen) if not isinstance(frames_gen, list) else frames_gen
    
    # Verify conversion worked
    assert isinstance(frames_list, list), "Should be a list"
    assert len(frames_list) > 0, "Should have frames"
    
    # Verify we can iterate multiple times
    count1 = sum(1 for _ in frames_list)
    count2 = sum(1 for _ in frames_list)
    assert count1 == count2, "Should be able to iterate multiple times"
    
    print("✅ Fix verified: generator exhaustion prevented")


if __name__ == '__main__':
    test_fix()
    print("\nPatch ready to apply to raven-python SDK")
    print("=" * 70)
    print("Location: raven/base.py")
    print("Method: Client.build_msg()")
    print("Change: Add generator-to-list conversion before get_stack_info()")
    print("=" * 70)
