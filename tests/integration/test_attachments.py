import pytest
import time
import uuid

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
    project_id = 42
    event_id = "515539018c9b4260a6f999572f1661ee"

    relay = relay_with_processing()
    relay.wait_relay_healthcheck()
    mini_sentry.project_configs[project_id] = mini_sentry.full_project_config()
    attachments_consumer = attachments_consumer()
    outcomes_consumer = outcomes_consumer()

    attachments = [
        ("att_1", "foo.txt", b"heavens no"),
        ("att_2", "bar.txt", b"hell yeah"),
    ]
    response = relay.send_attachments(project_id, event_id, attachments)

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
        "project_id": project_id,
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
        "project_id": project_id,
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
        relay.send_event(project_id, b"bogus")

    outcome = outcomes_consumer.get_outcome()
    assert outcome.get("event_id", None) is None, outcome
    assert outcome["outcome"] == 3
    assert outcome["reason"] == "payload"


def test_empty_attachments_with_processing(
    mini_sentry, relay_with_processing, attachments_consumer, outcomes_consumer
):
    project_id = 42
    event_id = "515539018c9b4260a6f999572f1661ee"

    relay = relay_with_processing()
    relay.wait_relay_healthcheck()
    mini_sentry.project_configs[project_id] = mini_sentry.full_project_config()
    attachments_consumer = attachments_consumer()

    attachments = [("att_1", "foo.txt", b"")]
    response = relay.send_attachments(project_id, event_id, attachments)

    attachment = attachments_consumer.get_individual_attachment()

    # The ID is random. Just assert that it is there and non-zero.
    assert attachment["attachment"].pop("id")

    assert attachment == {
        "type": "attachment",
        "attachment": {
            "attachment_type": "event.attachment",
            "chunks": 0,
            "content_type": "application/octet-stream",
            "name": "foo.txt",
        },
        "event_id": event_id,
        "project_id": project_id,
    }


@pytest.mark.parametrize("rate_limits", [[], ["attachment"]])
def test_attachments_ratelimit(
    mini_sentry, relay_with_processing, outcomes_consumer, rate_limits
):
    event_id = "515539018c9b4260a6f999572f1661ee"

    relay = relay_with_processing()
    relay.wait_relay_healthcheck()

    project_config = mini_sentry.project_configs[42] = mini_sentry.full_project_config()
    project_config["config"]["quotas"] = [
        {"categories": rate_limits, "limit": 0, "reasonCode": "static_disabled_quota"}
    ]

    outcomes_consumer = outcomes_consumer()
    attachments = [("att_1", "foo.txt", b"")]

    # First attachment returns 200 but is rate limited in processing
    relay.send_attachments(42, event_id, attachments)
    # TODO: There are no outcomes emitted for attachments yet. Instead, sleep to allow Relay to
    # process the event and cache the rate limit
    # outcomes_consumer.assert_rate_limited("static_disabled_quota")
    time.sleep(0.2)

    # Second attachment returns 429 in endpoint
    with pytest.raises(HTTPError) as excinfo:
        relay.send_attachments(42, event_id, attachments)
    assert excinfo.value.response.status_code == 429
    # outcomes_consumer.assert_rate_limited("static_disabled_quota")


def test_attachments_quotas(
    mini_sentry, relay_with_processing, attachments_consumer, outcomes_consumer,
):
    event_id = "515539018c9b4260a6f999572f1661ee"
    attachment_body = b"blabla"

    relay = relay_with_processing()
    relay.wait_relay_healthcheck()

    project_config = mini_sentry.project_configs[42] = mini_sentry.full_project_config()
    project_config["config"]["quotas"] = [
        {
            "id": "test_rate_limiting_{}".format(uuid.uuid4().hex),
            "categories": ["attachment"],
            "window": 3600,
            "limit": 5 * len(attachment_body),
            "reasonCode": "attachments_exceeded",
        }
    ]

    attachments_consumer = attachments_consumer()
    outcomes_consumer = outcomes_consumer()
    attachments = [("att_1", "foo.txt", attachment_body)]

    for i in range(5):
        relay.send_attachments(42, event_id, [("att_1", "%s.txt" % i, attachment_body)])
        chunk, _ = attachments_consumer.get_attachment_chunk()
        assert chunk == attachment_body
        attachment = attachments_consumer.get_individual_attachment()
        assert attachment["attachment"]["name"] == "%s.txt" % i

    # First attachment returns 200 but is rate limited in processing
    relay.send_attachments(42, event_id, attachments)
    # TODO: There are no outcomes emitted for attachments yet. Instead, sleep to allow Relay to
    # process the event and cache the rate limit
    # outcomes_consumer.assert_rate_limited("static_disabled_quota")
    time.sleep(0.2)

    # Second attachment returns 429 in endpoint
    with pytest.raises(HTTPError) as excinfo:
        relay.send_attachments(42, event_id, attachments)
    assert excinfo.value.response.status_code == 429
    # outcomes_consumer.assert_rate_limited("static_disabled_quota")
