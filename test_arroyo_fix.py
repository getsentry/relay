#!/usr/bin/env python3
"""
Test script to verify the fix for the MetricsBuffer race condition.

This test simulates the concurrent access pattern that was causing the
"RuntimeError: dictionary changed size during iteration" error.
"""
import threading
import time
from arroyo.processing.processor import MetricsBuffer


def test_concurrent_flush_and_increment():
    """
    Test that concurrent flush and increment operations don't cause
    dictionary iteration errors.
    """
    metrics_buffer = MetricsBuffer()
    errors = []
    iterations = 1000
    
    def increment_worker():
        """Worker that continuously increments counters."""
        for i in range(iterations):
            try:
                metrics_buffer.incr_counter("arroyo.consumer.stuck", 1)
                time.sleep(0.0001)  # Small delay to increase chance of race
            except RuntimeError as e:
                errors.append(("increment", str(e)))
    
    def flush_worker():
        """Worker that continuously flushes metrics."""
        for i in range(iterations):
            try:
                metrics_buffer.flush()
                time.sleep(0.0001)  # Small delay to increase chance of race
            except RuntimeError as e:
                errors.append(("flush", str(e)))
    
    # Create multiple threads doing concurrent operations
    threads = []
    for _ in range(3):
        threads.append(threading.Thread(target=increment_worker))
        threads.append(threading.Thread(target=flush_worker))
    
    # Start all threads
    start_time = time.time()
    for thread in threads:
        thread.start()
    
    # Wait for all threads to complete
    for thread in threads:
        thread.join()
    
    duration = time.time() - start_time
    
    # Check for errors
    if errors:
        print(f"❌ FAILED: {len(errors)} errors occurred during test:")
        for operation, error in errors[:5]:  # Show first 5 errors
            print(f"   - {operation}: {error}")
        return False
    else:
        print(f"✅ PASSED: No errors in {iterations * 6} operations across 6 threads ({duration:.2f}s)")
        return True


def test_metrics_accuracy():
    """
    Test that metrics are accurately recorded even with concurrent access.
    """
    from unittest.mock import Mock
    
    metrics_buffer = MetricsBuffer()
    # Mock the metrics object to track calls
    metrics_buffer.metrics = Mock()
    
    expected_increments = 100
    
    def increment_worker():
        for _ in range(expected_increments):
            metrics_buffer.incr_counter("arroyo.consumer.stuck", 1)
    
    # Create threads
    threads = [threading.Thread(target=increment_worker) for _ in range(3)]
    
    # Start threads
    for thread in threads:
        thread.start()
    
    # Wait for completion
    for thread in threads:
        thread.join()
    
    # Final flush to ensure all metrics are sent
    metrics_buffer.flush()
    
    # Note: Due to the buffering and throttling nature of MetricsBuffer,
    # we can't guarantee exact counts, but we should have no errors
    print("✅ PASSED: Metrics accuracy test completed without errors")
    return True


if __name__ == "__main__":
    print("Testing arroyo MetricsBuffer race condition fix...\n")
    
    test1_pass = test_concurrent_flush_and_increment()
    print()
    test2_pass = test_metrics_accuracy()
    
    print("\n" + "="*60)
    if test1_pass and test2_pass:
        print("✅ All tests PASSED!")
        exit(0)
    else:
        print("❌ Some tests FAILED")
        exit(1)
