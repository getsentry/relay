"""
Tests for the TUS upload endpoint (/api/{project_id}/upload/).
"""

import time
import uuid

from flask import Response
import pytest
import urllib
from sentry_relay.auth import PublicKey


@pytest.fixture
def dummy_upload(mini_sentry):
    mini_sentry.allow_chunked = True

    @mini_sentry.app.route("/api/<project>/upload/", methods=["POST"])
    def dummy_upload(**opts):
        return Response("", status=201, headers={"Location": "dummy"})


def test_forward_success(mini_sentry, relay, dummy_upload):
    project_id = 42
    mini_sentry.add_full_project_config(project_id)
    relay = relay(mini_sentry)

    data = b"hello world"
    response = relay.post(
        "/api/%s/upload/?sentry_key=%s"
        % (project_id, mini_sentry.get_dsn_public_key(project_id)),
        headers={
            "Tus-Resumable": "1.0.0",
            "Upload-Length": str(len(data)),
            "Content-Type": "application/offset+octet-stream",
        },
        data=data,
    )

    assert response.status_code == 201


def test_upload_missing_tus_version(mini_sentry, relay, dummy_upload):

    project_id = 42
    mini_sentry.add_full_project_config(project_id)
    relay = relay(mini_sentry)

    response = relay.post(
        "/api/%s/upload/?sentry_key=%s"
        % (project_id, mini_sentry.get_dsn_public_key(project_id)),
        headers={
            "Upload-Length": "5",
            "Content-Type": "application/offset+octet-stream",
        },
        data=b"hello",
    )

    assert response.status_code == 400


def test_upload_unsupported_tus_version(mini_sentry, relay, dummy_upload):

    project_id = 42
    mini_sentry.add_full_project_config(project_id)
    relay = relay(mini_sentry)

    response = relay.post(
        "/api/%s/upload/?sentry_key=%s"
        % (project_id, mini_sentry.get_dsn_public_key(project_id)),
        headers={
            "Tus-Resumable": "0.2.0",
            "Upload-Length": "5",
            "Content-Type": "application/offset+octet-stream",
        },
        data=b"hello",
    )

    assert response.status_code == 400


def test_upload_missing_upload_length(mini_sentry, relay, dummy_upload):

    project_id = 42
    mini_sentry.add_full_project_config(project_id)
    relay = relay(mini_sentry)

    response = relay.post(
        "/api/%s/upload/?sentry_key=%s"
        % (project_id, mini_sentry.get_dsn_public_key(project_id)),
        headers={
            "Tus-Resumable": "1.0.0",
            "Content-Type": "application/offset+octet-stream",
        },
        data=b"hello",
    )

    assert response.status_code == 400


def test_upload_body_too_large(mini_sentry, relay, dummy_upload):

    project_id = 42
    mini_sentry.add_full_project_config(project_id)
    relay = relay(mini_sentry)

    data = b"this is way more data than declared"
    response = relay.post(
        "/api/%s/upload/?sentry_key=%s"
        % (project_id, mini_sentry.get_dsn_public_key(project_id)),
        headers={
            "Tus-Resumable": "1.0.0",
            "Upload-Length": "5",
            "Content-Type": "application/offset+octet-stream",
        },
        data=data,
    )

    assert response.status_code == 413


@pytest.mark.parametrize("data_category", ["attachment", "attachment_item"])
def test_upload_rate_limited(mini_sentry, relay, data_category, dummy_upload):
    """Request is rate limited on the fast path

    NOTE: It would be nice if this also worked for the "error" data category,
    but the `EnvelopeLimiter` does not check the event rate limit when there's only attachments,
    because for classic envelopes it cannot distinguish between event and transaction attachments.
    """
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["quotas"] = [
        {
            "id": f"test_rate_limiting_{uuid.uuid4().hex}",
            "categories": [data_category],
            "limit": 0,
            "reasonCode": "cached_rate_limit",
        }
    ]
    relay = relay(mini_sentry)

    def request():
        return relay.post(
            "/api/%s/upload/?sentry_key=%s"
            % (project_id, mini_sentry.get_dsn_public_key(project_id)),
            headers={
                "Tus-Resumable": "1.0.0",
                "Upload-Length": "5",
                "Content-Type": "application/offset+octet-stream",
            },
            data=b"hello",
        )

    # First request goes through:
    assert request().status_code == 201

    time.sleep(1)  # TODO: wait for log instead.

    assert request().status_code == 429


PROCESSING_OPTIONS = {
    "processing": {"upload": {"objectstore_url": "http://127.0.0.1:8888/"}}
}


def test_upload_processing(mini_sentry, relay_with_processing):
    """Upload via processing relay stores the blob in objectstore."""
    project_id = 42
    mini_sentry.add_full_project_config(project_id)
    relay = relay_with_processing(PROCESSING_OPTIONS)

    data = b"hello world"
    response = relay.post(
        "/api/%s/upload/?sentry_key=%s"
        % (project_id, mini_sentry.get_dsn_public_key(project_id)),
        headers={
            "Tus-Resumable": "1.0.0",
            "Upload-Length": str(len(data)),
            "Content-Type": "application/offset+octet-stream",
        },
        data=data,
    )

    assert response.status_code == 201
    assert response.headers["Tus-Resumable"] == "1.0.0"
    assert response.headers["Upload-Offset"] == str(len(data))

    # Validate location:
    path, query = response.headers["Location"].split("?")
    base_path, attachment_id = path.rstrip("/").rsplit("/", 1)
    assert base_path == "/api/42/upload"
    attachment_id = uuid.UUID(attachment_id).hex
    query_params = urllib.parse.parse_qs(query)
    (length,) = query_params["length"]
    assert length == "11"
    (signature,) = query_params["signature"]

    unsigned_uri = f"{base_path}/{attachment_id}/?length=11"
    assert PublicKey.parse(relay.public_key).verify(unsigned_uri.encode(), signature)
