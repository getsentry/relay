from datetime import datetime, timezone

from .test_spansv2 import TEST_CONFIG, envelope_with_spans


def test_spansv2_mobile_attributes(
    mini_sentry,
    relay,
    relay_with_processing,
    spans_consumer,
):
    """
    Tests that a span from a mobile SDK gets sentry.mobile and sentry.main_thread set,
    and device.class derived from device attributes.
    """
    spans_consumer = spans_consumer()

    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"].update(
        {
            "features": [
                "projects:span-v2-experimental-processing",
            ],
            "retentions": {"span": {"standard": 42, "downsampled": 1337}},
        }
    )

    relay = relay(relay_with_processing(options=TEST_CONFIG), options=TEST_CONFIG)

    ts = datetime.now(timezone.utc)

    envelope = envelope_with_spans(
        {
            "start_timestamp": ts.timestamp(),
            "end_timestamp": ts.timestamp() + 0.5,
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "span_id": "eee19b7ec3c1b174",
            "is_segment": True,
            "name": "MainActivity.onCreate",
            "status": "ok",
            "attributes": {
                "sentry.sdk.name": {"value": "sentry.java.android", "type": "string"},
                "thread.name": {"value": "main", "type": "string"},
                "device.family": {"value": "Android", "type": "string"},
                "device.processor_frequency": {"value": 3000.0, "type": "double"},
                "device.processor_count": {"value": 8.0, "type": "double"},
                "device.memory_size": {"value": 8589934592.0, "type": "double"},
            },
        },
        trace_info={
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "public_key": project_config["publicKeys"][0]["publicKey"],
        },
    )

    relay.send_envelope(project_id, envelope)

    span = spans_consumer.get_span()

    attrs = span["attributes"]
    assert attrs["sentry.mobile"] == {"type": "string", "value": "true"}
    assert attrs["sentry.main_thread"] == {"type": "string", "value": "true"}
    assert attrs["device.class"] == {"type": "string", "value": "3"}


def test_spansv2_mobile_outlier_filtering(
    mini_sentry,
    relay,
    relay_with_processing,
    spans_consumer,
):
    """
    Tests that mobile measurements exceeding 180 seconds are removed.
    """
    spans_consumer = spans_consumer()

    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"].update(
        {
            "features": [
                "projects:span-v2-experimental-processing",
            ],
            "retentions": {"span": {"standard": 42, "downsampled": 1337}},
        }
    )

    relay = relay(relay_with_processing(options=TEST_CONFIG), options=TEST_CONFIG)

    ts = datetime.now(timezone.utc)

    envelope = envelope_with_spans(
        {
            "start_timestamp": ts.timestamp(),
            "end_timestamp": ts.timestamp() + 0.5,
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "span_id": "eee19b7ec3c1b175",
            "is_segment": True,
            "name": "app.start",
            "status": "ok",
            "attributes": {
                "app.vitals.start.cold.value": {
                    "value": 200000.0,
                    "type": "double",
                },
                "app.vitals.ttid.value": {"value": 5000.0, "type": "double"},
            },
        },
        trace_info={
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "public_key": project_config["publicKeys"][0]["publicKey"],
        },
    )

    relay.send_envelope(project_id, envelope)

    span = spans_consumer.get_span()

    attrs = span["attributes"]
    assert "app.vitals.start.cold.value" not in attrs
    assert attrs["app.vitals.ttid.value"] == {"type": "double", "value": 5000.0}
