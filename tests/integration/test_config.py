import tempfile
import pytest
import time

from requests import HTTPError


def test_invalid_kafka_config_should_fail(mini_sentry, relay_with_processing):
    options = {
        "processing": {
            "topics": {
                "__unknown": "foobar",
                "profiles": {
                    "name": "profiles",
                    "config": "does_not_exist",
                },
            }
        }
    }

    relay = relay_with_processing(options=options, wait_health_check=False)
    assert relay.wait_for_exit() != 0

    error = str(mini_sentry.test_failures.get_nowait())
    assert "__unknown" in error
    error = str(mini_sentry.test_failures.get_nowait())
    assert "profiles" in error.lower()


def test_invalid_topics_raise_error(mini_sentry, relay_with_processing):
    options = {"processing": {"kafka_validate_topics": True}}

    relay = relay_with_processing(options=options, wait_health_check=False)
    assert relay.wait_for_exit() != 0

    error = str(mini_sentry.test_failures.get_nowait())
    assert "failed to validate the topic with name" in error


def test_missing_env_var_in_config(mini_sentry, relay, relay_credentials):
    credentials = relay_credentials()
    relay = relay(
        mini_sentry,
        credentials=credentials,
        wait_health_check=False,
        options={
            "http": {
                "encoding": "${THIS_DOES_NOT_EXIST_OTHER_WISE_THE_TEST_WILL_PASS}",
            }
        },
    )

    assert relay.wait_for_exit() != 0


def test_variable_loaded_from_file(mini_sentry, relay):
    tmp = tempfile.NamedTemporaryFile(delete_on_close=False)
    tmp.write(b"1B")
    tmp.close()

    relay = relay(
        mini_sentry,
        options={
            "limits": {"max_event_size": f"${{file:{tmp.name}}}"},
        },
    )

    with pytest.raises(HTTPError, match="413 Client Error"):
        relay.send_event(42)


def test_variable_loaded_from_file_is_reloaded(mini_sentry, relay):
    # Relay logs an error whenever the memory watermark is breached, don't fail the test on it.
    mini_sentry.fail_on_relay_error = False

    tmp = tempfile.NamedTemporaryFile(delete_on_close=False)
    tmp.write(b"1TiB")  # Relay will start as healthy.
    tmp.flush()

    relay = relay(
        mini_sentry,
        options={
            "relay": {"mode": "proxy"},
            "health": {"max_memory_bytes": f"${{file:{tmp.name}}}"},
        },
    )

    response = relay.get("/api/relay/healthcheck/ready/", is_internal=True)
    assert response.status_code == 200

    # Rewrite the water mark, Relay will start reporting unhealthy.
    tmp.truncate(0)
    tmp.seek(0)
    tmp.write(b"42")
    tmp.flush()

    start = time.time()
    while True:
        assert time.time() < start + 10, "timed out while waiting for config reload"

        response = relay.get("/api/relay/healthcheck/ready/", is_internal=True)
        if response.status_code == 503:
            # Unhealthy => config was reloaded.
            return
        time.sleep(0.5)
