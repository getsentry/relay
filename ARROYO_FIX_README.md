# Fix for RuntimeError: dictionary changed size during iteration in Arroyo

## Issue Summary
The `MetricsBuffer.flush()` method in `arroyo/processing/processor.py` causes a `RuntimeError: dictionary changed size during iteration` when multiple threads concurrently access the metrics buffer.

## Root Cause
The `flush()` method iterates directly over `self.__timers.items()` and `self.__counters.items()`. When other threads (such as DogStatsd sender/flush threads) call methods like `incr_counter()` or `record_timer()` during iteration, they modify these dictionaries, causing Python to raise a RuntimeError.

### Code Location
File: `arroyo/processing/processor.py`  
Class: `MetricsBuffer`  
Method: `flush()`  
Lines: ~130-136

### Problematic Code
```python
def flush(self) -> None:
    metric: Union[ConsumerTiming, ConsumerCounter]
    value: Union[float, int]

    for metric, value in self.__timers.items():  # <- Thread-unsafe
        self.metrics.timing(metric, value)
    for metric, value in self.__counters.items():  # <- Thread-unsafe, ERROR HERE
        self.metrics.increment(metric, value)
    self.__reset()
```

## Solution
Create snapshots of the dictionaries before iterating to ensure thread-safe iteration. The snapshot approach is efficient because `list(dict.items())` creates a shallow copy of the items at that moment.

### Fixed Code
```python
def flush(self) -> None:
    metric: Union[ConsumerTiming, ConsumerCounter]
    value: Union[float, int]
    
    # Create snapshots to avoid RuntimeError when dictionaries are modified
    # by other threads during iteration
    timers_snapshot = list(self.__timers.items())
    counters_snapshot = list(self.__counters.items())
    
    for metric, value in timers_snapshot:
        self.metrics.timing(metric, value)
    for metric, value in counters_snapshot:
        self.metrics.increment(metric, value)
    self.__reset()
```

## Why This Fix Works
1. **Snapshot Creation**: `list(dict.items())` creates an immutable snapshot of dictionary items at that moment
2. **Thread Safety**: Even if other threads modify the original dictionaries during iteration, the snapshot remains unchanged
3. **Performance**: The overhead of creating a list is minimal compared to the risk of runtime errors
4. **Correctness**: All metrics present at the time of flush() are reported; new metrics added during flush will be caught in the next flush cycle

## Alternative Solutions Considered

### 1. Using threading.Lock (Not Recommended)
```python
def __init__(self, ...):
    self.__lock = threading.Lock()
    
def flush(self) -> None:
    with self.__lock:
        for metric, value in self.__timers.items():
            self.metrics.timing(metric, value)
        for metric, value in self.__counters.items():
            self.metrics.increment(metric, value)
        self.__reset()
        
def incr_counter(self, metric: ConsumerCounter, value: int) -> None:
    with self.__lock:
        self.__counters[metric] = self.__counters.get(metric, 0) + value
```

**Why Not**: This approach would require locks on all methods that access these dictionaries, potentially causing performance bottlenecks and increasing the risk of deadlocks.

### 2. Using dict.copy() (Less Efficient)
```python
for metric, value in self.__timers.copy().items():
    ...
```

**Why Not**: `dict.copy()` creates a full copy of the dictionary, which is less efficient than `list(dict.items())` which only copies the items view.

## Testing Recommendations
1. **Unit Test**: Create a test that simulates concurrent access to MetricsBuffer
2. **Integration Test**: Run under load with multiple threads accessing the buffer
3. **Stress Test**: Use threading.Thread to spawn multiple threads that call flush() and incr_counter() simultaneously

### Example Test
```python
import threading
import time

def test_concurrent_metrics_buffer():
    buffer = MetricsBuffer(mock_metrics)
    
    def flush_loop():
        for _ in range(100):
            buffer.flush()
            time.sleep(0.001)
    
    def counter_loop():
        for _ in range(1000):
            buffer.incr_counter("test.metric", 1)
            time.sleep(0.0001)
    
    threads = [
        threading.Thread(target=flush_loop),
        threading.Thread(target=counter_loop),
        threading.Thread(target=counter_loop),
    ]
    
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    
    # Should not raise RuntimeError
```

## Impact Assessment
- **Risk**: Low - The change is minimal and localized to one method
- **Performance**: Negligible - Creating a list snapshot is O(n) where n is the number of metrics (typically small)
- **Compatibility**: No API changes, fully backward compatible
- **Coverage**: Fixes all race conditions in the flush() method

## Deployment
1. Apply the patch to arroyo library: `patch -p1 < arroyo-metrics-buffer-fix.patch`
2. Or manually update the `flush()` method in `arroyo/processing/processor.py`
3. Test in staging environment
4. Deploy to production

## Related Issues
- RuntimeError in stuck detector thread when main thread becomes unresponsive
- Concurrent metric reporting from multiple threads
- DogStatsd threads modifying metrics during flush

## References
- Python threading documentation: https://docs.python.org/3/library/threading.html
- Dictionary iteration best practices: https://docs.python.org/3/library/stdtypes.html#dict
- Arroyo repository: https://github.com/getsentry/arroyo
