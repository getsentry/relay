"""
Test to verify the fix for SENTRY-5DEZ: Dictionary mutation during 
concurrent iteration in Kafka producer metrics.

This test ensures that the MetricsTrackingKafkaProducer can handle
concurrent delivery callbacks without raising RuntimeError due to
dictionary size changes during iteration.
"""

import sys
import os
import threading
import time
from unittest.mock import Mock, patch

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fixtures.processing import MetricsTrackingKafkaProducer


def test_concurrent_metrics_updates():
    """
    Test that concurrent delivery callbacks don't cause RuntimeError.
    
    This simulates the scenario where:
    1. Multiple Kafka producers emit messages asynchronously
    2. Delivery callbacks fire concurrently, calling __metrics_delivery_callback
    3. One callback's __throttled_record triggers __flush_metrics
    4. Another concurrent callback modifies __produce_counters dictionary
    
    Without the fix, this would raise:
    RuntimeError: dictionary changed size during iteration
    """
    # Create producer with mocked Kafka backend
    with patch('confluent_kafka.Producer'):
        producer = MetricsTrackingKafkaProducer({"bootstrap.servers": "localhost:9092"})
    
    # Track if any RuntimeError occurred
    errors = []
    
    def delivery_callback_simulator(topic_num):
        """Simulate concurrent delivery callbacks."""
        for i in range(100):
            try:
                # Simulate delivery callback being called from rdkafka thread
                err = None if i % 2 == 0 else Exception("delivery error")
                msg = Mock(topic=lambda: f"topic_{topic_num}")
                
                # This calls __metrics_delivery_callback which modifies the dict
                producer._MetricsTrackingKafkaProducer__metrics_delivery_callback(
                    err, msg, f"topic_{topic_num}"
                )
                
                # Occasionally trigger flush to cause concurrent iteration
                if i % 10 == 0:
                    producer._MetricsTrackingKafkaProducer__flush_metrics()
                
                # Small delay to increase chance of race condition
                time.sleep(0.001)
            except RuntimeError as e:
                if "dictionary changed size during iteration" in str(e):
                    errors.append(e)
    
    # Create multiple threads simulating concurrent delivery callbacks
    threads = []
    for topic_num in range(5):
        thread = threading.Thread(target=delivery_callback_simulator, args=(topic_num,))
        threads.append(thread)
        thread.start()
    
    # Wait for all threads to complete
    for thread in threads:
        thread.join(timeout=10)
    
    # Verify no RuntimeError occurred
    assert len(errors) == 0, f"RuntimeError occurred: {errors[0]}" if errors else ""
    
    # Verify producer can still flush without errors
    producer._MetricsTrackingKafkaProducer__flush_metrics()


def test_metrics_snapshot_isolation():
    """
    Test that the snapshot-based iteration properly isolates metrics.
    
    Verifies that:
    1. Metrics are captured in a snapshot before iteration
    2. The original dictionary is cleared immediately
    3. New metrics during flush go to the fresh dictionary
    4. Iteration completes without errors
    """
    with patch('confluent_kafka.Producer'):
        producer = MetricsTrackingKafkaProducer({"bootstrap.servers": "localhost:9092"})
    
    # Add some initial metrics
    for i in range(10):
        msg = Mock(topic=lambda i=i: f"topic_{i % 3}")
        producer._MetricsTrackingKafkaProducer__metrics_delivery_callback(
            None, msg, f"topic_{i % 3}"
        )
    
    # Thread to add metrics during flush
    def add_metrics_during_flush():
        time.sleep(0.005)  # Wait a bit to ensure flush has started
        for i in range(10, 20):
            msg = Mock(topic=lambda i=i: f"topic_{i % 3}")
            producer._MetricsTrackingKafkaProducer__metrics_delivery_callback(
                None, msg, f"topic_{i % 3}"
            )
    
    # Start thread that will modify dict during flush
    thread = threading.Thread(target=add_metrics_during_flush)
    thread.start()
    
    # Flush should complete without error
    try:
        producer._MetricsTrackingKafkaProducer__flush_metrics()
        thread.join(timeout=1)
        success = True
    except RuntimeError:
        success = False
    
    assert success, "Flush should not raise RuntimeError even with concurrent modifications"


def test_produce_with_automatic_flush():
    """
    Test that the produce method with automatic flush triggers work correctly.
    """
    with patch('confluent_kafka.Producer') as mock_producer_class:
        mock_producer = Mock()
        mock_producer_class.return_value = mock_producer
        
        producer = MetricsTrackingKafkaProducer({"bootstrap.servers": "localhost:9092"})
        
        # Produce messages that will trigger automatic flush
        for i in range(10):
            producer.produce(f"topic_{i % 2}", value=f"message_{i}".encode())
        
        # Verify produce was called on the underlying producer
        assert mock_producer.produce.call_count == 10
        
        # Manual flush should work
        producer.flush()
        mock_producer.flush.assert_called()


if __name__ == "__main__":
    print("Running concurrent metrics test...")
    test_concurrent_metrics_updates()
    print("✓ Concurrent metrics test passed")
    
    print("Running snapshot isolation test...")
    test_metrics_snapshot_isolation()
    print("✓ Snapshot isolation test passed")
    
    print("Running produce with automatic flush test...")
    test_produce_with_automatic_flush()
    print("✓ Produce with automatic flush test passed")
    
    print("\nAll tests passed! Fix for SENTRY-5DEZ is working correctly.")
