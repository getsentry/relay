import os
import pytest
import queue

from sentry_sdk.envelope import Envelope, Item, PayloadRef
from sentry_sdk.attachments import Attachment

def load_dump_file(base_file_name: str):
    dmp_path = os.path.join(
        os.path.dirname(__file__), "fixtures", "native", base_file_name
    )

    with open(dmp_path, "rb") as f:
        return f.read()


@pytest.mark.parametrize("variant", ["plain", "zstandard"])
def test_nnswitch(
    mini_sentry, relay_with_processing, outcomes_consumer, attachments_consumer, events_consumer, variant
):
    PROJECT_ID = 42
    mini_sentry.add_full_project_config(
        PROJECT_ID,
        extra={"config": {}},
    )
    events_consumer = events_consumer()
    outcomes_consumer = outcomes_consumer()
    attachments_consumer = attachments_consumer()
    relay = relay_with_processing()

    bogus_error = {
        "event_id": "cbf6960622e14a45abc1f03b2055b186",
        "type": "error",
        "exception": {"values": [{"type": "ValueError", "value": "Should not happen"}]},
    }
    envelope = Envelope()
    envelope.add_event(bogus_error)

    envelope.add_item(
        Item(
            type="attachment",
            payload=PayloadRef(bytes=load_dump_file("nnswitch_dying_message_%s.dat" % variant)),
            headers={
                "filename": "dying_message.dat",
                "content_type": "application/octet-stream",
            },
        )
    )
    relay.send_envelope(PROJECT_ID, envelope)

    outcomes = outcomes_consumer.get_outcomes()
    assert len(outcomes) == 0

    event, _ = events_consumer.get_event()
    assert event["sdk"]["name"] == "sentry.native.switch"
    assert event["user"]["id"] == "user-id"
    assert event["contexts"]["os"]["name"] == "Nintendo"
    assert event["breadcrumbs"]["values"][0]["type"] == "bread"
    assert event["breadcrumbs"]["values"][0]["message"] == "crumb"
