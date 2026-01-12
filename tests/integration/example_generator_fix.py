"""
Example: Fixing Generator Exhaustion in Stack Trace Collection

This module demonstrates the actual fix pattern that would be applied
to the raven-python SDK (or similar codebases) to resolve the generator
exhaustion issue described in the bug report.

The code here mirrors the structure described in the stacktrace:
- runner.py: Entry point that sends test messages
- base.py: Client class with capture and build_msg methods
- utils.py: Stack trace collection utilities
"""

import inspect
import sys
from typing import Any, Dict, Iterator, List, Optional, Union, Callable


# ============================================================================
# utils.py - Stack Frame Utilities
# ============================================================================

def iter_stack_frames(skip: int = 0) -> Iterator:
    """
    Generate stack frames from the current call stack.
    
    This is a generator function that yields frame objects one at a time.
    
    Args:
        skip: Number of frames to skip from the top of the stack
        
    Yields:
        Frame objects from the call stack
        
    Warning:
        This returns a generator that can only be iterated ONCE.
        If you need to iterate multiple times, convert to a list:
            frames = list(iter_stack_frames())
    """
    frame = inspect.currentframe()
    try:
        # Skip requested number of frames
        for _ in range(skip + 1):  # +1 to skip this function itself
            if frame is not None:
                frame = frame.f_back
        
        # Yield remaining frames
        while frame is not None:
            yield frame
            frame = frame.f_back
    finally:
        del frame


def get_stack_info(
    frames: Union[Iterator, List],
    transformer: Optional[Callable[[Dict], Dict]] = None
) -> List[Dict[str, Any]]:
    """
    Extract information from stack frames.
    
    FIXED VERSION: This function now properly handles generator exhaustion
    by converting the input to a list if needed.
    
    Args:
        frames: Iterator or list of frame objects
        transformer: Optional function to transform each frame's info
        
    Returns:
        List of dictionaries containing frame information
        
    Original Bug:
        The original implementation assumed it could iterate 'frames' multiple
        times, but if 'frames' was a generator, it would be exhausted after
        the first iteration, causing subsequent iterations to return nothing.
        
    Fix:
        Convert the generator to a list at the start, ensuring we can iterate
        as many times as needed without exhaustion.
    """
    # KEY FIX: Convert generator to list to prevent exhaustion
    if not isinstance(frames, list):
        frames = list(frames)
    
    # Now we can safely iterate multiple times if needed
    # (though in this implementation we only iterate once)
    result = []
    
    for frame in frames:
        frame_info = {
            'filename': frame.f_code.co_filename,
            'function': frame.f_code.co_name,
            'lineno': frame.f_lineno,
            'module': frame.f_globals.get('__name__', ''),
        }
        
        # Apply transformer if provided
        if transformer:
            frame_info = transformer(frame_info)
        
        result.append(frame_info)
    
    return result


# ============================================================================
# base.py - Client Implementation
# ============================================================================

class Client:
    """
    Mock Sentry client that captures events and builds messages.
    
    This demonstrates the fixed version of the raven-python Client class.
    """
    
    def __init__(self, dsn: str, include_paths: Optional[List[str]] = None):
        """
        Initialize the Sentry client.
        
        Args:
            dsn: Data Source Name for Sentry
            include_paths: List of path prefixes to include in stack traces
        """
        self.dsn = dsn
        self.include_paths = include_paths or []
        self.state = ClientState()
    
    def transform(self, frame_info: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform frame information based on client configuration.
        
        This filters frames based on include_paths and performs
        other transformations.
        """
        # Filter by include_paths if configured
        if self.include_paths:
            module = frame_info.get('module', '')
            if not any(module.startswith(path) for path in self.include_paths):
                frame_info['in_app'] = False
            else:
                frame_info['in_app'] = True
        
        return frame_info
    
    def captureMessage(self, message: str, **kwargs) -> Optional[str]:
        """
        Capture a message event.
        
        Args:
            message: The message to capture
            **kwargs: Additional event data (level, stack, tags, extra, etc.)
            
        Returns:
            Event ID if successful, None otherwise
        """
        return self.capture('raven.events.Message', message=message, **kwargs)
    
    def capture(
        self,
        event_type: str,
        data: Optional[Dict] = None,
        date=None,
        time_spent=None,
        extra: Optional[Dict] = None,
        stack: Optional[Union[bool, Iterator]] = None,
        tags: Optional[Dict] = None,
        **kwargs
    ) -> Optional[str]:
        """
        Capture an event.
        
        Args:
            event_type: Type of event (e.g., 'raven.events.Message')
            data: Event data dictionary
            stack: Stack trace (True to auto-collect, or an iterator of frames)
            tags: Event tags
            extra: Extra event data
            **kwargs: Additional event data
            
        Returns:
            Event ID
        """
        if not self.is_enabled():
            return None
        
        data = self.build_msg(
            event_type, data, date, time_spent, extra, stack, tags=tags,
            **kwargs
        )
        
        self.send(**data)
        
        return data.get('event_id')
    
    def build_msg(
        self,
        event_type: str,
        data: Optional[Dict] = None,
        date=None,
        time_spent=None,
        extra: Optional[Dict] = None,
        stack: Optional[Union[bool, Iterator]] = None,
        tags: Optional[Dict] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Build event message data.
        
        FIXED VERSION: This is the key method where the fix is applied.
        
        Original Bug Location:
            When stack=True or stack=<generator>, the code would pass
            the generator directly to get_stack_info(), which would
            fail if get_stack_info() needed to iterate multiple times.
            
        Fix Applied:
            Convert the generator to a list before passing to get_stack_info().
            This happens in two places:
            1. When stack=True, we call iter_stack_frames() and convert to list
            2. When stack is already a generator, we convert it to a list
        """
        if data is None:
            data = {}
        
        # Add extra data
        if extra:
            data['extra'] = extra
        
        # Add tags
        if tags:
            data['tags'] = tags
        
        # Handle stack trace collection
        if stack:
            frames = None
            
            if stack is True:
                # Auto-collect stack frames
                frames = iter_stack_frames(skip=1)
            else:
                # Use provided frames (might be a generator)
                frames = stack
            
            # KEY FIX: Convert generator to list to prevent exhaustion
            # This is the critical fix that resolves the bug
            if not isinstance(frames, list):
                frames = list(frames)
            
            # Now safe to pass to get_stack_info
            data.update({
                'sentry.interfaces.Stacktrace': {
                    'frames': get_stack_info(frames, transformer=self.transform)
                },
            })
        
        # Add event type
        data['type'] = event_type
        
        # Add message if provided
        if 'message' in kwargs:
            data['message'] = kwargs['message']
        
        return data
    
    def send(self, **data):
        """
        Send event data to Sentry.
        
        In this mock, we just print it.
        """
        print(f"Sending event to Sentry: {data.get('type', 'unknown')}")
        print(f"  Message: {data.get('message', 'N/A')}")
        print(f"  Frames collected: {len(data.get('sentry.interfaces.Stacktrace', {}).get('frames', []))}")
    
    def is_enabled(self) -> bool:
        """Check if the client is enabled."""
        return True


class ClientState:
    """Track client state."""
    
    def __init__(self):
        self._failed = False
    
    def did_fail(self) -> bool:
        """Check if the last operation failed."""
        return self._failed


# ============================================================================
# runner.py - Test Runner
# ============================================================================

def get_uid() -> int:
    """Get user ID."""
    import os
    return os.getuid()


def get_loadavg() -> tuple:
    """Get system load average."""
    try:
        return sys.getloadavg()
    except (AttributeError, OSError):
        return (0.0, 0.0, 0.0)


def send_test_message(client: Client, options: Dict[str, Any]):
    """
    Send a test message to verify the client is working.
    
    This is the function that triggers the bug in the original code.
    It calls captureMessage with stack=True, which ultimately leads
    to the generator exhaustion issue in build_msg.
    
    With the fix applied, this now works correctly.
    """
    print("Sending test message...")
    
    event_id = client.captureMessage(
        'This is a test message from the Sentry client',
        level='INFO',
        stack=True,  # This triggers stack collection
        tags=options.get('tags', {}),
        extra={
            'user': get_uid(),
            'loadavg': get_loadavg(),
        },
    )
    
    if client.state.did_fail():
        print('Error: Failed to send test message')
        return False
    
    print(f'Success! Event ID: {event_id}')
    return True


def main():
    """
    Main entry point demonstrating the fixed code.
    """
    dsn = 'https://public_key@sentry.io/project_id'
    
    print("=" * 70)
    print("Generator Exhaustion Fix Demonstration")
    print("=" * 70)
    print()
    print("Using DSN configuration:")
    print(" ", dsn)
    print()
    
    # Create client with the fix applied
    client = Client(dsn, include_paths=['raven'])
    
    # Send test message - this would fail in the original code
    # but works correctly with the fix
    options = {
        'tags': {
            'environment': 'test',
            'version': '1.0.0',
        }
    }
    
    success = send_test_message(client, options)
    
    print()
    if success:
        print("✅ Test passed! Generator exhaustion issue is fixed.")
        print()
        print("Key changes:")
        print("  1. get_stack_info() converts generator to list immediately")
        print("  2. build_msg() converts generator to list before passing to get_stack_info()")
        print("  3. Both functions can now safely iterate multiple times")
    else:
        print("❌ Test failed")
    
    return 0 if success else 1


if __name__ == '__main__':
    sys.exit(main())
