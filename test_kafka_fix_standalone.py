#!/usr/bin/env python3
"""
Standalone test to verify the fix for SENTRY-5DEZ.

This demonstrates that the thread-safe dictionary snapshot approach
prevents RuntimeError from concurrent modifications.
"""

import threading
import time


class BuggyMetricsTracker:
    """
    Demonstrates the BUGGY approach that causes RuntimeError.
    
    DO NOT USE - This is the broken implementation for comparison.
    """
    def __init__(self):
        self.counters = {}
    
    def update(self, key):
        if key not in self.counters:
            self.counters[key] = 0
        self.counters[key] += 1
    
    def flush(self):
        """BUGGY: Iterates dict.items() without protection."""
        for key, value in self.counters.items():  # RuntimeError can happen here!
            # Simulate some work
            time.sleep(0.001)
        self.counters.clear()


class FixedMetricsTracker:
    """
    Demonstrates the FIXED approach using snapshot + lock.
    
    This is the correct implementation that fixes SENTRY-5DEZ.
    """
    def __init__(self):
        self.counters = {}
        self.lock = threading.Lock()
    
    def update(self, key):
        with self.lock:
            if key not in self.counters:
                self.counters[key] = 0
            self.counters[key] += 1
    
    def flush(self):
        """FIXED: Creates snapshot before iteration."""
        with self.lock:
            # Create snapshot and clear original dict while holding lock
            snapshot = dict(self.counters)
            self.counters.clear()
        
        # Iterate over snapshot - concurrent updates go to the fresh dict
        for key, value in snapshot.items():
            # Simulate some work
            time.sleep(0.001)


def test_buggy_implementation():
    """Test that the buggy implementation can fail with concurrent access."""
    tracker = BuggyMetricsTracker()
    errors = []
    
    def update_worker():
        for i in range(50):
            tracker.update(f"key_{i % 5}")
            time.sleep(0.001)
    
    def flush_worker():
        for i in range(10):
            try:
                tracker.flush()
                time.sleep(0.005)
            except RuntimeError as e:
                errors.append(e)
    
    threads = [
        threading.Thread(target=update_worker) for _ in range(3)
    ] + [
        threading.Thread(target=flush_worker)
    ]
    
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    
    print(f"Buggy implementation: {len(errors)} RuntimeError(s) occurred")
    return len(errors) > 0  # Usually fails, but may pass occasionally


def test_fixed_implementation():
    """Test that the fixed implementation handles concurrent access safely."""
    tracker = FixedMetricsTracker()
    errors = []
    
    def update_worker():
        for i in range(100):
            tracker.update(f"key_{i % 5}")
            time.sleep(0.001)
    
    def flush_worker():
        for i in range(20):
            try:
                tracker.flush()
                time.sleep(0.005)
            except RuntimeError as e:
                errors.append(e)
    
    threads = [
        threading.Thread(target=update_worker) for _ in range(5)
    ] + [
        threading.Thread(target=flush_worker)
    ]
    
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    
    print(f"Fixed implementation: {len(errors)} RuntimeError(s) occurred")
    return len(errors) == 0  # Should always pass


if __name__ == "__main__":
    print("=" * 60)
    print("Testing SENTRY-5DEZ Fix: Dictionary Concurrent Modification")
    print("=" * 60)
    print()
    
    print("1. Testing buggy implementation (may raise RuntimeError)...")
    buggy_failed = test_buggy_implementation()
    print()
    
    print("2. Testing fixed implementation (should never raise RuntimeError)...")
    fixed_passed = test_fixed_implementation()
    print()
    
    print("=" * 60)
    if fixed_passed:
        print("✓ SUCCESS: Fixed implementation handles concurrency correctly!")
        print()
        print("The fix prevents RuntimeError by:")
        print("  1. Using a lock to protect dictionary access")
        print("  2. Creating a snapshot before iteration")
        print("  3. Clearing the original dict while holding the lock")
        print("  4. Iterating over the snapshot (not the live dict)")
        print()
        print("This ensures that concurrent delivery callbacks can safely")
        print("update metrics while flush operations are in progress.")
        exit(0)
    else:
        print("✗ FAILURE: Fixed implementation still has errors!")
        exit(1)
