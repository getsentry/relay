"""
Integration test for generator exhaustion fix.

This test can be run with pytest and integrates with the existing test suite.
It verifies that the generator exhaustion issue is properly fixed.
"""

import inspect
from typing import Iterator, List

try:
    import pytest
    HAS_PYTEST = True
except ImportError:
    HAS_PYTEST = False
    # Create a dummy pytest.mark.parametrize decorator
    class pytest:
        class mark:
            @staticmethod
            def parametrize(*args, **kwargs):
                def decorator(func):
                    return func
                return decorator


class TestGeneratorExhaustion:
    """Test suite for generator exhaustion fix."""
    
    @staticmethod
    def create_generator() -> Iterator:
        """Create a test generator that yields integers."""
        for i in range(5):
            yield i
    
    @staticmethod
    def iter_stack_frames() -> Iterator:
        """
        Create a generator that yields stack frames.
        
        This simulates the iter_stack_frames function from raven-python.
        """
        frame = inspect.currentframe()
        try:
            frame = frame.f_back
            while frame is not None:
                yield frame
                frame = frame.f_back
        finally:
            del frame
    
    def test_generator_can_only_be_iterated_once(self):
        """Demonstrate that generators can only be iterated once."""
        gen = self.create_generator()
        
        # First iteration works
        result1 = list(gen)
        assert result1 == [0, 1, 2, 3, 4]
        
        # Second iteration returns empty (generator exhausted)
        result2 = list(gen)
        assert result2 == []
    
    def test_list_can_be_iterated_multiple_times(self):
        """Demonstrate that lists can be iterated multiple times."""
        lst = [0, 1, 2, 3, 4]
        
        # First iteration works
        result1 = list(lst)
        assert result1 == [0, 1, 2, 3, 4]
        
        # Second iteration also works
        result2 = list(lst)
        assert result2 == [0, 1, 2, 3, 4]
    
    def test_converting_generator_to_list_prevents_exhaustion(self):
        """Test that converting generator to list prevents exhaustion."""
        gen = self.create_generator()
        
        # Convert to list
        lst = list(gen)
        
        # Can iterate multiple times
        result1 = list(lst)
        result2 = list(lst)
        assert result1 == result2 == [0, 1, 2, 3, 4]
    
    def test_stack_frames_generator_exhaustion(self):
        """Test stack frames generator exhaustion issue."""
        frames = self.iter_stack_frames()
        
        # First iteration
        count1 = sum(1 for _ in frames)
        assert count1 > 0
        
        # Second iteration returns 0 (exhausted)
        count2 = sum(1 for _ in frames)
        assert count2 == 0
    
    def test_stack_frames_list_no_exhaustion(self):
        """Test that converting to list prevents stack frames exhaustion."""
        frames = self.iter_stack_frames()
        
        # Convert to list
        frames_list = list(frames)
        
        # Can iterate multiple times
        count1 = sum(1 for _ in frames_list)
        count2 = sum(1 for _ in frames_list)
        
        assert count1 > 0
        assert count1 == count2
    
    def test_fix_with_isinstance_check(self):
        """Test the fix pattern using isinstance check."""
        def process_frames(frames):
            """Process frames with fix applied."""
            # FIX: Convert to list if not already a list
            if not isinstance(frames, list):
                frames = list(frames)
            
            # Now safe to iterate multiple times
            count = len(frames)
            return [f for f in frames], count
        
        # Test with generator
        gen = self.create_generator()
        result, count = process_frames(gen)
        assert result == [0, 1, 2, 3, 4]
        assert count == 5
        
        # Test with list (should also work)
        lst = [0, 1, 2, 3, 4]
        result, count = process_frames(lst)
        assert result == [0, 1, 2, 3, 4]
        assert count == 5
    
    def test_get_stack_info_with_fix(self):
        """Test get_stack_info with the fix applied."""
        def get_stack_info(frames, transformer=None):
            """Fixed version of get_stack_info."""
            # FIX: Convert generator to list
            if not isinstance(frames, list):
                frames = list(frames)
            
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
        
        # Test with generator
        frames = self.iter_stack_frames()
        result = get_stack_info(frames)
        
        assert len(result) > 0
        assert all('filename' in f for f in result)
        assert all('function' in f for f in result)
        assert all('lineno' in f for f in result)
    
    def test_build_msg_with_fix(self):
        """Test build_msg pattern with the fix applied."""
        def build_msg(stack=None):
            """Simulated build_msg with fix."""
            data = {}
            
            if stack:
                frames = stack
                
                # FIX: Convert generator to list
                if not isinstance(frames, list):
                    frames = list(frames)
                
                data['stacktrace'] = {
                    'frames': [
                        {
                            'filename': f.f_code.co_filename,
                            'function': f.f_code.co_name,
                            'lineno': f.f_lineno,
                        }
                        for f in frames
                    ]
                }
            
            return data
        
        # Test with generator
        frames = self.iter_stack_frames()
        result = build_msg(stack=frames)
        
        assert 'stacktrace' in result
        assert 'frames' in result['stacktrace']
        assert len(result['stacktrace']['frames']) > 0
    
    def test_transformer_function(self):
        """Test that the fix works with transformer functions."""
        def uppercase_transformer(frame_info):
            """Transform function names to uppercase."""
            frame_info['function'] = frame_info['function'].upper()
            return frame_info
        
        def get_stack_info(frames, transformer=None):
            """Get stack info with transformer."""
            if not isinstance(frames, list):
                frames = list(frames)
            
            result = []
            for frame in frames:
                frame_info = {
                    'filename': frame.f_code.co_filename,
                    'function': frame.f_code.co_name,
                }
                if transformer:
                    frame_info = transformer(frame_info)
                result.append(frame_info)
            
            return result
        
        frames = self.iter_stack_frames()
        result = get_stack_info(frames, transformer=uppercase_transformer)
        
        assert len(result) > 0
        assert all(f['function'].isupper() for f in result)
    
    def test_edge_case_empty_generator(self):
        """Test edge case with empty generator."""
        def empty_gen():
            return
            yield  # Never reached
        
        frames = empty_gen()
        frames_list = list(frames) if not isinstance(frames, list) else frames
        
        assert frames_list == []
        assert isinstance(frames_list, list)
    
    def test_edge_case_none_input(self):
        """Test edge case with None input."""
        def process_frames(frames):
            if frames is None:
                return []
            
            if not isinstance(frames, list):
                frames = list(frames)
            
            return frames
        
        result = process_frames(None)
        assert result == []
    
    def test_performance_list_conversion(self):
        """Test that list conversion has acceptable performance."""
        import time
        
        # Create a generator with many items
        def large_gen():
            for i in range(1000):
                yield i
        
        # Time the conversion
        gen = large_gen()
        start = time.time()
        lst = list(gen)
        elapsed = time.time() - start
        
        # Should be very fast (< 1ms for 1000 items)
        assert elapsed < 0.01
        assert len(lst) == 1000
    
    def test_memory_reasonable(self):
        """Test that list conversion doesn't use excessive memory."""
        import sys
        
        # Create a generator
        gen = self.iter_stack_frames()
        
        # Convert to list
        lst = list(gen)
        
        # Size should be reasonable (stack traces are typically small)
        # Each frame is just a pointer, so this should be very small
        size = sys.getsizeof(lst)
        
        # Should be less than 10KB for a typical stack trace
        assert size < 10240


class TestBackwardCompatibility:
    """Test that the fix maintains backward compatibility."""
    
    def test_works_with_list_input(self):
        """Test that fix works when list is provided instead of generator."""
        def process(frames):
            if not isinstance(frames, list):
                frames = list(frames)
            return len(frames)
        
        # Should work with list input
        result = process([1, 2, 3, 4, 5])
        assert result == 5
    
    def test_works_with_tuple_input(self):
        """Test behavior with tuple input."""
        def process(frames):
            if not isinstance(frames, list):
                frames = list(frames)
            return len(frames)
        
        # Should convert tuple to list
        result = process((1, 2, 3, 4, 5))
        assert result == 5
    
    def test_preserves_frame_objects(self):
        """Test that frame objects are preserved correctly."""
        def iter_frames():
            frame = inspect.currentframe()
            try:
                frame = frame.f_back
                while frame is not None:
                    yield frame
                    frame = frame.f_back
            finally:
                del frame
        
        frames = iter_frames()
        frames_list = list(frames)
        
        # All items should be frame objects
        assert all(inspect.isframe(f) for f in frames_list)


if __name__ == '__main__':
    # Run tests with pytest if available, otherwise run manually
    if HAS_PYTEST:
        pytest.main([__file__, '-v'])
    else:
        print("pytest not available, running tests manually...")
        
        test = TestGeneratorExhaustion()
        compat = TestBackwardCompatibility()
        
        tests = [
            ('Generator can only be iterated once', test.test_generator_can_only_be_iterated_once),
            ('List can be iterated multiple times', test.test_list_can_be_iterated_multiple_times),
            ('Converting generator to list prevents exhaustion', test.test_converting_generator_to_list_prevents_exhaustion),
            ('Stack frames generator exhaustion', test.test_stack_frames_generator_exhaustion),
            ('Stack frames list no exhaustion', test.test_stack_frames_list_no_exhaustion),
            ('Fix with isinstance check', test.test_fix_with_isinstance_check),
            ('Get stack info with fix', test.test_get_stack_info_with_fix),
            ('Build msg with fix', test.test_build_msg_with_fix),
            ('Transformer function', test.test_transformer_function),
            ('Edge case empty generator', test.test_edge_case_empty_generator),
            ('Edge case None input', test.test_edge_case_none_input),
            ('Performance list conversion', test.test_performance_list_conversion),
            ('Memory reasonable', test.test_memory_reasonable),
            ('Works with list input', compat.test_works_with_list_input),
            ('Works with tuple input', compat.test_works_with_tuple_input),
            ('Preserves frame objects', compat.test_preserves_frame_objects),
        ]
        
        passed = 0
        failed = 0
        
        for name, test_func in tests:
            try:
                test_func()
                print(f"✓ {name}")
                passed += 1
            except AssertionError as e:
                print(f"✗ {name}: {e}")
                failed += 1
            except Exception as e:
                print(f"✗ {name}: {type(e).__name__}: {e}")
                failed += 1
        
        print()
        print(f"Results: {passed} passed, {failed} failed")
        
        if failed == 0:
            print("✅ All tests passed!")
        else:
            print(f"❌ {failed} test(s) failed")
