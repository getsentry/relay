import pytest

from requests.exceptions import HTTPError


def test_attachments_400(
    mini_sentry, relay_with_processing, attachments_consumer, outcomes_consumer
):
    proj_id = 42
    relay = relay_with_processing()
    relay.wait_relay_healthcheck()
    mini_sentry.project_configs[proj_id] = mini_sentry.full_project_config()
    attachments_consumer = attachments_consumer()

    event_id = "123abc"

    with pytest.raises(HTTPError) as excinfo:
        relay.send_attachments(proj_id, event_id, [])

    assert excinfo.value.response.status_code == 400


def test_attachments_with_processing(
    mini_sentry, relay_with_processing, attachments_consumer, outcomes_consumer
):
    proj_id = 42
    relay = relay_with_processing()
    relay.wait_relay_healthcheck()
    mini_sentry.project_configs[proj_id] = mini_sentry.full_project_config()
    attachments_consumer = attachments_consumer()
    outcomes_consumer = outcomes_consumer()

    event_id = "515539018c9b4260a6f999572f1661ee"

    response = relay.send_attachments(
        proj_id,
        event_id,
        [("att_1", "foo.txt", b"heavens no"), ("att_2", "bar.txt", b"hell yeah"),],
    )

    attachment_contents = {}
    attachment_ids = []
    attachment_num_chunks = {}

    while set(attachment_contents.values()) != {b"heavens no", b"hell yeah"}:
        chunk, v = attachments_consumer.get_attachment_chunk()
        attachment_contents[v["id"]] = attachment_contents.get(v["id"], b"") + chunk
        if v["id"] not in attachment_ids:
            attachment_ids.append(v["id"])
        num_chunks = 1 + attachment_num_chunks.get(v["id"], 0)
        assert v["chunk_index"] == num_chunks - 1
        attachment_num_chunks[v["id"]] = num_chunks

    id1, id2 = attachment_ids

    assert attachment_contents[id1] == b"heavens no"
    assert attachment_contents[id2] == b"hell yeah"

    attachment = attachments_consumer.get_individual_attachment()
    attachment2 = attachments_consumer.get_individual_attachment()

    assert attachment == {
        "type": "attachment",
        "attachment": {
            "attachment_type": "event.attachment",
            "chunks": attachment_num_chunks[id1],
            "content_type": "application/octet-stream",
            "id": id1,
            "name": "foo.txt",
        },
        "event_id": event_id,
        "project_id": 42,
    }
    assert attachment2 == {
        "type": "attachment",
        "attachment": {
            "attachment_type": "event.attachment",
            "chunks": attachment_num_chunks[id2],
            "content_type": "application/octet-stream",
            "id": id2,
            "name": "bar.txt",
        },
        "event_id": event_id,
        "project_id": 42,
    }

    # We want to check that no outcome has been created for the attachment
    # upload.
    #
    # Send an unrelated event in, and assert that it is the first item we can
    # poll from outcomes for. While not 100% correct due to partitioning, it's
    # way faster than waiting n seconds to see if nothing else is in outcomes.
    #
    # We need to send in an invalid event because successful ones do not
    # produce outcomes (not in Relay)
    with pytest.raises(HTTPError):
        relay.send_event(42, b"bogus")

    outcome = outcomes_consumer.get_outcome()
    assert outcome["event_id"] == None, outcome
    assert outcome["outcome"] == 3
    assert outcome["reason"] == "payload"
