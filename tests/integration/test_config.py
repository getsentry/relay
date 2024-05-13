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

    error = str(mini_sentry.test_failures.pop(0))
    assert "__unknown" in error
    error = str(mini_sentry.test_failures.pop(0))
    assert "profiles" in error.lower()


def test_invalid_topics_raise_error(mini_sentry, relay_with_processing):
    options = {"processing": {"validate_kafka_topics": True}}

    relay = relay_with_processing(options=options, wait_health_check=False)
    assert relay.wait_for_exit() != 0

    error = str(mini_sentry.test_failures.pop(0))
    assert "failed to validate the topic with name" in error
