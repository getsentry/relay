import json
from pathlib import Path

import pytest

from .fixtures import processing

pytest_plugins = ["pytester"]


@pytest.mark.parametrize("fail", [False, True])
@pytest.mark.parametrize("workers", [0, 2])
def test_topic_cleanup_at_session_end(pytester, monkeypatch, fail, workers):
    """Each worker batches its topics after all tests and producers have stopped."""
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVER", "broker.example:9092")
    # The nested failure is intentional; do not report it as a real CI failure.
    monkeypatch.delenv("PYTEST_SENTRY_DSN", raising=False)
    tests_dir = str(Path(__file__).resolve().parents[1])
    pytester.makeconftest(f"""
import json
import os
import sys
from concurrent.futures import Future
from pathlib import Path

sys.path.insert(0, {tests_dir!r})
from integration.fixtures import processing
from integration.fixtures.processing import kafka_topics, get_topic_name
from integration.conftest import background_process

processes = []
deleted_file = Path("deleted-" + os.environ.get("PYTEST_XDIST_WORKER", "master") + ".json")

class AdminClient:
    def __init__(self, config):
        assert config == {{"bootstrap.servers": "broker.example:9092"}}

    def delete_topics(self, topics, operation_timeout, request_timeout):
        assert processes and all(p.returncode is not None for p in processes)
        assert operation_timeout == request_timeout == 10
        assert not deleted_file.exists()
        deleted_file.write_text(json.dumps(topics))
        futures = {{}}
        for topic in topics:
            future = Future()
            if topic.startswith("relay-test-never-created-"):
                future.set_exception(processing.kafka.KafkaException(
                    processing.kafka.KafkaError(
                        processing.kafka.KafkaError.UNKNOWN_TOPIC_OR_PART
                    )
                ))
            else:
                future.set_result(None)
            futures[topic] = future
        return futures

processing.AdminClient = AdminClient
""")
    pytester.makepyfile(f"""
import sys
import pytest
from conftest import processes, deleted_file

@pytest.mark.parametrize("run", range(4))
def test_producer(background_process, get_topic_name, run):
    assert not deleted_file.exists(), "Topics were deleted before the session ended"
    processes.append(background_process(
        [sys.executable, "-c", "import time; time.sleep(60)"]
    ))
    assert get_topic_name("events") == get_topic_name("events")
    get_topic_name("never-created")
    assert not ({fail!r} and run == 0), "intentional test failure"
""")
    result = pytester.runpytest_subprocess("-q", "-n", str(workers))
    result.assert_outcomes(passed=4 - int(fail), failed=int(fail), errors=0)
    batches = [
        json.loads(file.read_text()) for file in pytester.path.glob("deleted-*.json")
    ]
    assert len(batches) == (workers or 1)
    deleted = [topic for batch in batches for topic in batch]
    assert len(deleted) == len(set(deleted)) == 8
    events = [topic for topic in deleted if topic.startswith("relay-test-events-")]
    missing = [
        topic for topic in deleted if topic.startswith("relay-test-never-created-")
    ]
    assert len(events) == len(missing) == 4
    assert {topic.rsplit("-", 1)[1] for topic in events} == {
        topic.rsplit("-", 1)[1] for topic in missing
    }


def test_unused_topic_fixture_does_not_connect(monkeypatch):
    def unexpected_client(config):
        pytest.fail("No Kafka connection is needed when no topics were requested")

    monkeypatch.setattr(processing, "AdminClient", unexpected_client)
    fixture = processing.kafka_topics.__wrapped__()
    next(fixture)
    with pytest.raises(StopIteration):
        next(fixture)


def test_topic_cleanup_reports_broker_errors(monkeypatch):
    from concurrent.futures import Future

    error = processing.kafka.KafkaException(
        processing.kafka.KafkaError(
            processing.kafka.KafkaError.TOPIC_AUTHORIZATION_FAILED
        )
    )

    class AdminClient:
        def __init__(self, config):
            pass

        def delete_topics(self, topics, **kwargs):
            future = Future()
            future.set_exception(error)
            return {topics[0]: future}

    monkeypatch.setattr(processing, "AdminClient", AdminClient)
    fixture = processing.kafka_topics.__wrapped__()
    next(fixture).add("relay-test-events-owned")
    with pytest.raises(processing.kafka.KafkaException) as exc:
        next(fixture)
    assert exc.value is error
