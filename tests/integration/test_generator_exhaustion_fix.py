"""
Test demonstrating and fixing the generator exhaustion issue.

This test reproduces the issue described in the stacktrace where
`get_stack_info` attempts to iterate an `iter_stack_frames` generator
multiple times, causing exhaustion and failure.

Root Cause: Generators can only be iterated once. When passed to a function
that needs to iterate multiple times, the generator becomes exhausted after
the first iteration.

Solution: Convert the generator to a list before passing it to functions
that may iterate multiple times.
"""

import inspect


def iter_stack_frames():
    """
    Simulates the iter_stack_frames function that returns a generator.
    This mimics the behavior of the raven-python SDK.
    """
    frame = inspect.currentframe()
    try:
        # Skip the current frame and yield parent frames
        frame = frame.f_back
        while frame is not None:
            yield frame
            frame = frame.f_back
    finally:
        del frame


def get_stack_info_broken(frames, transformer=None):
    """
    Broken implementation that attempts to iterate the generator multiple times.
    This simulates the bug in the original raven-python code.
    """
    # First iteration - might be for validation or counting
    count = 0
    for _ in frames:
        count += 1
    
    # Second iteration - tries to actually process frames
    # This will fail because the generator is exhausted
    result = []
    for frame in frames:  # Generator is already exhausted here!
        frame_info = {
            'filename': frame.f_code.co_filename,
            'function': frame.f_code.co_name,
            'lineno': frame.f_lineno,
        }
        if transformer:
            frame_info = transformer(frame_info)
        result.append(frame_info)
    
    return result


def get_stack_info_fixed(frames, transformer=None):
    """
    Fixed implementation that converts generator to list first.
    This ensures the frames can be iterated multiple times if needed.
    """
    # Convert generator to list immediately
    if not isinstance(frames, list):
        frames = list(frames)
    
    # Now we can iterate multiple times without exhaustion
    count = len(frames)
    
    # Process frames
    result = []
    for frame in frames:
        frame_info = {
            'filename': frame.f_code.co_filename,
            'function': frame.f_code.co_name,
            'lineno': frame.f_lineno,
        }
        if transformer:
            frame_info = transformer(frame_info)
        result.append(frame_info)
    
    return result


def test_generator_exhaustion_demonstrates_bug():
    """
    Demonstrates the bug: generator exhaustion when iterated multiple times.
    """
    frames = iter_stack_frames()
    
    # This should fail or return empty results
    result = get_stack_info_broken(frames)
    
    # The result will be empty because the generator was exhausted
    # on the first iteration (counting)
    assert result == [], "Generator was exhausted, no frames extracted"


def test_generator_exhaustion_fix():
    """
    Demonstrates the fix: convert generator to list before processing.
    """
    frames = iter_stack_frames()
    
    # This should work correctly
    result = get_stack_info_fixed(frames)
    
    # The result should contain frame information
    assert len(result) > 0, "Frames should be extracted successfully"
    assert all('filename' in frame for frame in result)
    assert all('function' in frame for frame in result)
    assert all('lineno' in frame for frame in result)


def test_generator_exhaustion_fix_with_list():
    """
    Shows that the fix also works when a list is passed instead of a generator.
    """
    frames = list(iter_stack_frames())
    
    # Should work with list input too
    result = get_stack_info_fixed(frames)
    
    assert len(result) > 0, "Frames should be extracted successfully"
    assert len(result) == len(frames), "All frames should be processed"


def test_generator_exhaustion_with_transformer():
    """
    Tests the fix with a transformer function.
    """
    def uppercase_transformer(frame_info):
        """Transform frame info by uppercasing the function name."""
        frame_info['function'] = frame_info['function'].upper()
        return frame_info
    
    frames = iter_stack_frames()
    result = get_stack_info_fixed(frames, transformer=uppercase_transformer)
    
    assert len(result) > 0, "Frames should be extracted successfully"
    assert all(frame['function'].isupper() for frame in result), \
        "All function names should be uppercase"


# Demonstration of the proper fix pattern for the raven-python SDK
class MockClient:
    """
    Simulates the raven-python Client with the fixed build_msg method.
    """
    
    def __init__(self):
        self.include_paths = ['raven']
    
    def transform(self, frame_info):
        """Mock transformer that filters by include_paths."""
        return frame_info
    
    def build_msg_fixed(self, event_type, data=None, stack=None, **kwargs):
        """
        Fixed version of build_msg that properly handles generator exhaustion.
        
        The key fix is on line where get_stack_info is called:
        Convert the generator to a list before passing to get_stack_info.
        """
        if data is None:
            data = {}
        
        if stack:
            frames = stack
            
            # KEY FIX: Convert generator to list to prevent exhaustion
            if not isinstance(frames, list):
                frames = list(frames)
            
            data.update({
                'sentry.interfaces.Stacktrace': {
                    'frames': get_stack_info_fixed(
                        frames,
                        transformer=self.transform
                    )
                },
            })
        
        return data


def test_mock_client_build_msg():
    """
    Tests the fixed build_msg method with a generator.
    """
    client = MockClient()
    frames = iter_stack_frames()
    
    result = client.build_msg_fixed('raven.events.Message', stack=frames)
    
    assert 'sentry.interfaces.Stacktrace' in result
    assert 'frames' in result['sentry.interfaces.Stacktrace']
    assert len(result['sentry.interfaces.Stacktrace']['frames']) > 0


if __name__ == '__main__':
    # Run tests
    print("Testing generator exhaustion bug...")
    test_generator_exhaustion_demonstrates_bug()
    print("✓ Bug demonstrated: generator exhaustion returns empty results")
    
    print("\nTesting fix...")
    test_generator_exhaustion_fix()
    print("✓ Fix works: frames extracted successfully")
    
    print("\nTesting fix with list input...")
    test_generator_exhaustion_fix_with_list()
    print("✓ Fix works with list input")
    
    print("\nTesting fix with transformer...")
    test_generator_exhaustion_with_transformer()
    print("✓ Fix works with transformer")
    
    print("\nTesting mock client...")
    test_mock_client_build_msg()
    print("✓ Mock client build_msg works correctly")
    
    print("\n✅ All tests passed! Generator exhaustion issue is fixed.")
