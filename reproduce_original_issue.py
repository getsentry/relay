#!/usr/bin/env python3
"""
Script to demonstrate the original race condition issue.

This script shows what would happen without the fix by simulating
the exact scenario from the error trace.

WARNING: This will only work with the UNPATCHED version of arroyo.
With the patched version, this should run without errors.
"""
import threading
import time
import sys


def simulate_stuck_detector_scenario():
    """
    Simulates the exact scenario from the error trace:
    1. Stuck detector thread detects a stuck condition
    2. Calls incr_counter("arroyo.consumer.stuck", 1)
    3. This triggers __throttled_record() -> flush()
    4. Meanwhile, other threads also update metrics
    """
    from arroyo.processing.processor import MetricsBuffer
    
    metrics_buffer = MetricsBuffer()
    errors_caught = []
    iterations = 500
    
    def stuck_detector_thread():
        """Simulates the stuck detector thread behavior."""
        for i in range(iterations):
            try:
                # This is what the stuck detector does when main thread is stuck
                metrics_buffer.incr_counter("arroyo.consumer.stuck", 1)
                # Then it calls flush explicitly
                metrics_buffer.flush()
                time.sleep(0.0001)
            except RuntimeError as e:
                errors_caught.append(("stuck_detector", i, str(e)))
    
    def consumer_metrics_thread():
        """Simulates other consumer threads updating metrics."""
        for i in range(iterations):
            try:
                metrics_buffer.incr_counter("arroyo.consumer.run.count", 1)
                metrics_buffer.incr_timing("arroyo.consumer.run.time", 0.5)
                time.sleep(0.0001)
            except RuntimeError as e:
                errors_caught.append(("consumer_metrics", i, str(e)))
    
    def background_flush_thread():
        """Simulates periodic background flushes."""
        for i in range(iterations):
            try:
                metrics_buffer.flush()
                time.sleep(0.0002)
            except RuntimeError as e:
                errors_caught.append(("background_flush", i, str(e)))
    
    # Create threads matching production scenario
    threads = [
        threading.Thread(target=stuck_detector_thread, name="StuckDetector"),
        threading.Thread(target=consumer_metrics_thread, name="ConsumerMetrics-1"),
        threading.Thread(target=consumer_metrics_thread, name="ConsumerMetrics-2"),
        threading.Thread(target=background_flush_thread, name="BackgroundFlush"),
    ]
    
    print("Simulating production scenario with concurrent metric updates...")
    print(f"Running {len(threads)} threads with {iterations} iterations each")
    print()
    
    start_time = time.time()
    
    # Start all threads
    for t in threads:
        t.start()
    
    # Wait for completion
    for t in threads:
        t.join()
    
    duration = time.time() - start_time
    
    return errors_caught, duration


def main():
    print("="*70)
    print("Arroyo MetricsBuffer Race Condition Reproduction Test")
    print("="*70)
    print()
    
    errors, duration = simulate_stuck_detector_scenario()
    
    print(f"\nTest completed in {duration:.2f}s")
    print()
    
    if errors:
        print(f"❌ RACE CONDITION DETECTED: {len(errors)} errors occurred!")
        print()
        print("This indicates the UNPATCHED version is running.")
        print("Without the fix, you would see 'dictionary changed size during iteration' errors.")
        print()
        print("Sample errors:")
        for thread_name, iteration, error in errors[:3]:
            print(f"  - {thread_name} at iteration {iteration}: {error}")
        return 1
    else:
        print("✅ NO RACE CONDITION: All operations completed successfully!")
        print()
        print("This indicates the PATCHED version is running.")
        print("The fix successfully prevents dictionary iteration errors.")
        return 0


if __name__ == "__main__":
    sys.exit(main())
