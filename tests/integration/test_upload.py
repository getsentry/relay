"""
Tests for the TUS upload endpoint (/api/{project_id}/upload/).
"""

import uuid

import pytest


def test_upload_success(mini_sentry, relay):
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
    assert response.headers["Tus-Resumable"] == "1.0.0"
    assert response.headers["Upload-Offset"] == str(len(data))


def test_upload_missing_tus_version(mini_sentry, relay):
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


def test_upload_unsupported_tus_version(mini_sentry, relay):
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


def test_upload_missing_upload_length(mini_sentry, relay):
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


def test_upload_body_too_large(mini_sentry, relay):
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


@pytest.mark.parametrize("data_category", ["error", "attachment", "attachment_item"])
def test_upload_rate_limited(mini_sentry, relay, data_category):
    """Request is rate limited on the fast path"""
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

    import time

    time.sleep(1)  # TODO: wait for log instead.

    assert request().status_code == 429
