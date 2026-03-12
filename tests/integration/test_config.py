import tempfile
import pytest

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
