#!/usr/bin/env python3
"""
Script to automatically apply the MetricsBuffer race condition fix to arroyo.

This script patches the installed arroyo package to fix the
"RuntimeError: dictionary changed size during iteration" issue.
"""
import os
import sys


def find_processor_file():
    """Find the processor.py file in the installed arroyo package."""
    try:
        import arroyo
        arroyo_dir = os.path.dirname(arroyo.__file__)
        processor_path = os.path.join(arroyo_dir, "processing", "processor.py")
        
        if os.path.exists(processor_path):
            return processor_path
        else:
            print(f"Error: Could not find processor.py at {processor_path}")
            return None
    except ImportError:
        print("Error: arroyo package is not installed")
        return None


def check_if_already_patched(file_path):
    """Check if the file has already been patched."""
    with open(file_path, 'r') as f:
        content = f.read()
    
    # Look for the comment that indicates our patch
    return "Create snapshots of the dictionaries before iterating" in content


def apply_fix(file_path):
    """Apply the fix to the processor.py file."""
    with open(file_path, 'r') as f:
        content = f.read()
    
    # The original code to replace
    original = """    def flush(self) -> None:
        metric: Union[ConsumerTiming, ConsumerCounter]
        value: Union[float, int]

        for metric, value in self.__timers.items():
            self.metrics.timing(metric, value)
        for metric, value in self.__counters.items():
            self.metrics.increment(metric, value)
        self.__reset()"""
    
    # The fixed code
    fixed = """    def flush(self) -> None:
        metric: Union[ConsumerTiming, ConsumerCounter]
        value: Union[float, int]

        # Create snapshots of the dictionaries before iterating to avoid
        # "dictionary changed size during iteration" errors in multi-threaded contexts
        timers_snapshot = list(self.__timers.items())
        counters_snapshot = list(self.__counters.items())
        self.__reset()
        
        for metric, value in timers_snapshot:
            self.metrics.timing(metric, value)
        for metric, value in counters_snapshot:
            self.metrics.increment(metric, value)"""
    
    if original not in content:
        print("Error: Could not find the expected code pattern in processor.py")
        print("The file may have already been modified or is from a different version.")
        return False
    
    # Apply the fix
    content = content.replace(original, fixed)
    
    # Write the fixed content back
    try:
        with open(file_path, 'w') as f:
            f.write(content)
        return True
    except PermissionError:
        print(f"Error: Permission denied writing to {file_path}")
        print("Try running with sudo or in a virtual environment you own.")
        return False


def main():
    print("Arroyo MetricsBuffer Race Condition Fix")
    print("=" * 50)
    
    # Find the processor file
    processor_path = find_processor_file()
    if not processor_path:
        return 1
    
    print(f"Found processor.py at: {processor_path}")
    
    # Check if already patched
    if check_if_already_patched(processor_path):
        print("✅ The fix has already been applied!")
        return 0
    
    print("Applying fix...")
    
    # Apply the fix
    if apply_fix(processor_path):
        print("✅ Fix applied successfully!")
        print("\nYou can verify the fix by running: python3 test_arroyo_fix.py")
        return 0
    else:
        print("❌ Failed to apply fix")
        return 1


if __name__ == "__main__":
    sys.exit(main())
