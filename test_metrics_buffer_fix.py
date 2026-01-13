"""
Simplified test to verify the MetricsBuffer.flush() fix works correctly.

This test simulates the exact race condition from the error report without
requiring full arroyo dependencies.
"""
import threading
import time
from collections import defaultdict
from typing import Union, MutableMapping
from unittest.mock import Mock


# Simulate the exact code structure from arroyo
class MetricsBufferOLD:
    """Original buggy version"""
    def __init__(self):
        self.metrics = Mock()
        self.metrics.timing = Mock()
        self.metrics.increment = Mock()
        self.__timers: MutableMapping[str, float] = defaultdict(float)
        self.__counters: MutableMapping[str, int] = defaultdict(int)
        self.__last_record_time = time.time()

    def incr_counter(self, metric: str, delta: int) -> None:
        self.__counters[metric] += delta

    def incr_timing(self, metric: str, duration: float) -> None:
        self.__timers[metric] += duration

    def flush(self) -> None:
        # BUGGY VERSION - iterates directly over dictionaries
        for metric, value in self.__timers.items():
            self.metrics.timing(metric, value)
        for metric, value in self.__counters.items():
            self.metrics.increment(metric, value)
        self.__reset()

    def __reset(self) -> None:
        self.__timers.clear()
        self.__counters.clear()
        self.__last_record_time = time.time()


class MetricsBufferFIXED:
    """Fixed version with snapshots"""
    def __init__(self):
        self.metrics = Mock()
        self.metrics.timing = Mock()
        self.metrics.increment = Mock()
        self.__timers: MutableMapping[str, float] = defaultdict(float)
        self.__counters: MutableMapping[str, int] = defaultdict(int)
        self.__last_record_time = time.time()

    def incr_counter(self, metric: str, delta: int) -> None:
        self.__counters[metric] += delta

    def incr_timing(self, metric: str, duration: float) -> None:
        self.__timers[metric] += duration

    def flush(self) -> None:
        # FIXED VERSION - creates snapshots before iterating
        timers_snapshot = list(self.__timers.items())
        counters_snapshot = list(self.__counters.items())

        for metric, value in timers_snapshot:
            self.metrics.timing(metric, value)
        for metric, value in counters_snapshot:
            self.metrics.increment(metric, value)
        self.__reset()

    def __reset(self) -> None:
        self.__timers.clear()
        self.__counters.clear()
        self.__last_record_time = time.time()


def test_buffer(buffer_class, name: str, iterations: int = 1000):
    """Test a buffer class with concurrent access"""
    buffer = buffer_class()
    errors = []
    
    def flush_loop():
        """Simulates stuck detector thread calling flush"""
        try:
            for _ in range(iterations):
                buffer.flush()
                time.sleep(0.00001)
        except RuntimeError as e:
            if "dictionary changed size during iteration" in str(e):
                errors.append(("flush_loop", e))
            else:
                raise
        except Exception as e:
            errors.append(("flush_loop", e))
    
    def counter_loop():
        """Simulates other threads incrementing counters"""
        try:
            for i in range(iterations * 5):
                buffer.incr_counter("arroyo.consumer.stuck", 1)
                buffer.incr_counter("test.counter", i)
                time.sleep(0.000001)
        except Exception as e:
            errors.append(("counter_loop", e))
    
    def timer_loop():
        """Simulates other threads recording timings"""
        try:
            for i in range(iterations * 5):
                buffer.incr_timing("test.timer", float(i) * 0.001)
                time.sleep(0.000001)
        except Exception as e:
            errors.append(("timer_loop", e))
    
    # Create threads that will race
    threads = [
        threading.Thread(target=flush_loop),
        threading.Thread(target=counter_loop),
        threading.Thread(target=counter_loop),
        threading.Thread(target=timer_loop),
        threading.Thread(target=timer_loop),
    ]
    
    start_time = time.time()
    for t in threads:
        t.start()
    
    for t in threads:
        t.join(timeout=30)
    
    duration = time.time() - start_time
    
    return {
        "name": name,
        "errors": errors,
        "duration": duration,
        "iterations": iterations,
        "timing_calls": buffer.metrics.timing.call_count,
        "increment_calls": buffer.metrics.increment.call_count,
    }


def main():
    print("\n" + "="*80)
    print("MetricsBuffer Thread Safety Test")
    print("="*80 + "\n")
    
    # Test the OLD (buggy) version
    print("Testing ORIGINAL (buggy) version...")
    print("-" * 80)
    old_result = test_buffer(MetricsBufferOLD, "ORIGINAL", iterations=500)
    
    if old_result["errors"]:
        print(f"✗ ORIGINAL version FAILED (as expected)")
        print(f"  Errors detected: {len(old_result['errors'])}")
        for source, error in old_result["errors"][:3]:  # Show first 3
            print(f"    - {source}: {type(error).__name__}: {error}")
        if len(old_result["errors"]) > 3:
            print(f"    ... and {len(old_result['errors']) - 3} more errors")
    else:
        print(f"⚠ ORIGINAL version passed (race condition may not have been triggered)")
        print(f"  Note: This race condition is timing-sensitive and may not always occur")
    
    print(f"  Duration: {old_result['duration']:.2f}s")
    print(f"  Timing calls: {old_result['timing_calls']}")
    print(f"  Increment calls: {old_result['increment_calls']}")
    print()
    
    # Test the FIXED version
    print("Testing FIXED version...")
    print("-" * 80)
    fixed_result = test_buffer(MetricsBufferFIXED, "FIXED", iterations=2000)
    
    if fixed_result["errors"]:
        print(f"✗ FIXED version FAILED")
        print(f"  Errors detected: {len(fixed_result['errors'])}")
        for source, error in fixed_result["errors"]:
            print(f"    - {source}: {type(error).__name__}: {error}")
        print("\n" + "="*80)
        print("TEST FAILED: Fixed version still has errors!")
        print("="*80 + "\n")
        return False
    else:
        print(f"✓ FIXED version PASSED")
        print(f"  No errors detected!")
    
    print(f"  Duration: {fixed_result['duration']:.2f}s")
    print(f"  Timing calls: {fixed_result['timing_calls']}")
    print(f"  Increment calls: {fixed_result['increment_calls']}")
    print()
    
    # Summary
    print("="*80)
    if not fixed_result["errors"]:
        print("SUCCESS: Fixed version prevents RuntimeError!")
        print("="*80)
        print()
        print("The fix successfully prevents the 'dictionary changed size during")
        print("iteration' error by creating snapshots of the dictionaries before")
        print("iterating over them.")
        print()
        print("Key changes:")
        print("  1. timers_snapshot = list(self.__timers.items())")
        print("  2. counters_snapshot = list(self.__counters.items())")
        print("  3. Iterate over snapshots instead of live dictionaries")
        print()
        print("="*80)
        return True
    else:
        print("FAILURE: Fixed version still has errors")
        print("="*80)
        return False


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
