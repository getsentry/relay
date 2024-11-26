import uuid

from sentry_sdk.envelope import Envelope, Item, PayloadRef


def test_multi_write_redis_client_with_rate_limiting(
    mini_sentry,
    relay_with_processing,
    events_consumer,
    outcomes_consumer,
):
    outcomes_consumer = outcomes_consumer(timeout=10)
    events_consumer = events_consumer()

    project_cache_redis_prefix = f"relay-test-relayconfig-{uuid.uuid4()}"

    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["quotas"] = [
        {
            "id": f"error_rate_limiting_{uuid.uuid4()}",
            "categories": ["error"],
            "window": 3600,
            "limit": 1,
            "reasonCode": "drop_all",
        }
    ]

    redis_configs = [
        # First, we double-write/-read to/from both Redis instances.
        {
            "configs": [
                {"server": "redis://127.0.0.1:6379"},
                {"server": "redis://127.0.0.1:6380"},
            ]
        },
        # Afterward, we point to the secondary instance to make sure we are
        # correctly reading the quotas written before.
        {"server": "redis://127.0.0.1:6380"},
    ]

    for redis_config, expects_event, event_name in zip(
        redis_configs, [True, False], ["event_1", "event_2"]
    ):
        relay = relay_with_processing(
            {
                "processing": {
                    "redis": redis_config,
                    # We use the same prefix across the instances to make sure they read/write the same keys.
                    "projectconfig_cache_prefix": project_cache_redis_prefix,
                },
            }
        )

        event_id = uuid.uuid1().hex
        error_payload = {
            "event_id": event_id,
            "message": event_name,
            "extra": {"msg_text": event_name},
            "type": "error",
            "environment": "production",
            "release": "foo@1.2.3",
        }

        envelope = Envelope()
        envelope.add_item(Item(PayloadRef(json=error_payload), type="event"))

        relay.send_envelope(project_id, envelope)

        if expects_event:
            # We expect to pass the first rate-limit since we have a limit of 1.
            event, _ = events_consumer.get_event()
            assert event["logentry"]["formatted"] == event_name
        else:
            # Because of the quotas, we should drop error.
            events_consumer.assert_empty()

            # We must have 1 outcome with provided reason.
            outcomes = outcomes_consumer.get_outcomes()
            assert len(outcomes) == 1
            assert outcomes[0]["reason"] == "drop_all"

        relay.shutdown()
