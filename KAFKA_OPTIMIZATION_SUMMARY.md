# Kafka Topic and Consumer Reuse Optimization

## Overview

This optimization rewrites the Relays integration test fixtures to reuse Kafka topic names and consumers per pytest worker, creating fresh topics and consumers only when tests fail. This significantly improves test performance by reducing the overhead of creating new Kafka resources for each test.

## Key Changes

### 1. Worker-Specific Topic Naming (`tests/integration/fixtures/processing.py`)

- **Before**: Each test generated a unique topic name using `uuid.uuid4().hex`
- **After**: Topics are named per pytest worker with failure-based versioning

```python
# Before
return lambda topic: f"relay-test-{topic}-{random}"

# After  
def topic_name_generator(topic):
    failure_suffix = f"-f{_worker_state.failure_count}" if _worker_state.failure_count > 0 else ""
    return f"relay-test-{topic}-{_worker_state.topic_suffix}{failure_suffix}"
```

### 2. Worker State Management

- Added thread-local worker state tracking:
  - `topic_suffix`: Unique identifier per worker (`{worker_id}-{short_uuid}`)
  - `failure_count`: Increments when tests fail to ensure fresh topics
  - `consumers`: Cache of reusable Kafka consumers

### 3. Test Failure Tracking

- Automatic failure detection using pytest hooks
- When a test fails, the failure count increments, causing new topics/consumers to be created
- Cached consumers are properly cleaned up on failure

### 4. Consumer Reuse Optimization

- **Before**: New consumer created for every test
- **After**: Consumers are cached and reused based on topic and failure count

```python
# Cache key includes failure count to ensure fresh consumers after failures
cache_key = f"{topic}_{_worker_state.failure_count}"
```

### 5. Proper Cleanup

- Session-scoped cleanup fixture ensures all cached consumers are closed
- Error handling to prevent test failures from affecting cleanup

## Benefits

1. **Performance**: Significant reduction in test setup time by reusing Kafka resources
2. **Reliability**: Fresh topics/consumers on test failure prevent state pollution
3. **Resource Efficiency**: Fewer Kafka topic/consumer creations reduce load
4. **Worker Isolation**: Each pytest-xdist worker has its own resource namespace

## Implementation Details

### Worker ID Detection
```python
def _get_worker_id():
    # Tries pytest.worker_id first, then PYTEST_XDIST_WORKER env var
    worker_id = os.environ.get("PYTEST_XDIST_WORKER", "main")
```

### Topic Name Format
- **Normal**: `relay-test-{topic}-{worker_id}-{short_uuid}`
- **After failure**: `relay-test-{topic}-{worker_id}-{short_uuid}-f{failure_count}`

### Consumer Group Format
- `test-consumer-{worker_id}-{short_uuid}-{topic}-{failure_count}`

## Compatibility

- Fully backward compatible with existing tests
- Works with both single-threaded and pytest-xdist parallel execution
- Maintains test isolation and failure handling

## Testing

The implementation has been validated with comprehensive tests covering:
- Worker ID detection
- State initialization
- Topic name generation consistency
- Failure handling and topic regeneration
- Cache key generation
- Multi-worker isolation

All tests pass successfully, confirming the optimization works as expected.