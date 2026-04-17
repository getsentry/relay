"""
Tests for the TUS upload endpoint (/api/{project_id}/upload/).
"""

import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed

from flask import Response
import pytest

DUMMY_UPLOAD_PATH = "/api/42/upload/019cdc82ed6c7761ba21fd34b86481c2/"
DUMMY_UPLOAD_LOCATION = f"{DUMMY_UPLOAD_PATH}?length=11&signature=z_fUMhT0EZqJz6OQtwGHqTlOOLPpTVpvPa-rYTg18FVWZM1OGny-LeVJB5H-sSR_5e--I1xt-FlCmRG2bsmcAQ.eyJ0IjoiMjAyNi0wMy0xMVQxMDo0ODoxMy45NDM1ODNaIn0"


@pytest.fixture
def dummy_upload(mini_sentry):
    mini_sentry.allow_chunked = True

    @mini_sentry.app.route("/api/<project>/upload/", methods=["POST"])
    def create(**opts):

        return Response(
            "",
            status=201,
            headers={"Location": DUMMY_UPLOAD_LOCATION},
        )

    @mini_sentry.app.route("/api/<project>/upload/<key>/", methods=["PATCH"])
    def upload(**opts):
        return Response(
            "",
            status=204,
            headers={"Location": DUMMY_UPLOAD_LOCATION},
        )


@pytest.fixture
def project_config(mini_sentry):
    project_id = 42
    config = mini_sentry.add_full_project_config(project_id)["config"]
    config.setdefault("features", []).append("projects:relay-upload-endpoint")
    return config


@pytest.mark.parametrize(
    "feature_enabled,expected_status_code",
    [
        pytest.param(True, 201, id="feature enabled"),
        pytest.param(False, 403, id="feature disabled"),
    ],
)
def test_forward_create(
    mini_sentry, relay, dummy_upload, feature_enabled, expected_status_code
):
    project_id = 42
    config = mini_sentry.add_full_project_config(project_id)
    if feature_enabled:
        config["config"].setdefault("features", []).append(
            "projects:relay-upload-endpoint"
        )
    relay = relay(mini_sentry)

    response = relay.post(
        "/api/%s/upload/?sentry_key=%s"
        % (project_id, mini_sentry.get_dsn_public_key(project_id)),
        headers={
            "Tus-Resumable": "1.0.0",
            "Upload-Length": "11",
        },
    )

    assert response.status_code == expected_status_code, response.text


@pytest.mark.parametrize(
    "feature_enabled,expected_status_code",
    [
        pytest.param(True, 204, id="feature enabled"),
        pytest.param(False, 403, id="feature disabled"),
    ],
)
def test_forward_patch(
    mini_sentry, relay, dummy_upload, feature_enabled, expected_status_code
):
    project_id = 42
    config = mini_sentry.add_full_project_config(project_id)
    if feature_enabled:
        config["config"].setdefault("features", []).append(
            "projects:relay-upload-endpoint"
        )
    relay = relay(mini_sentry)

    data = b"hello world"
    response = relay.patch(
        "%s&sentry_key=%s"
        % (DUMMY_UPLOAD_LOCATION, mini_sentry.get_dsn_public_key(project_id)),
        headers={
            "Tus-Resumable": "1.0.0",
            "Content-Type": "application/offset+octet-stream",
            "Upload-Offset": "0",
        },
        data=data,
    )

    assert response.status_code == expected_status_code, response.text
    if not feature_enabled:
        assert (
            response.json()["detail"]
            == "event submission rejected with_reason: FeatureDisabled(UploadEndpoint)"
        )


def test_upload_missing_tus_version(mini_sentry, relay, dummy_upload, project_config):

    project_id = 42
    relay = relay(mini_sentry)

    response = relay.post(
        "/api/%s/upload/?sentry_key=%s"
        % (project_id, mini_sentry.get_dsn_public_key(project_id)),
        headers={
            "Upload-Length": "5",
        },
        data=b"hello",
    )

    assert response.status_code == 400
    assert response.json() == {
        "detail": "TUS protocol error: expected Tus-Resumable: 1.0.0, got: (missing)",
        "causes": ["expected Tus-Resumable: 1.0.0, got: (missing)"],
    }


def test_upload_unsupported_tus_version(
    mini_sentry, relay, dummy_upload, project_config
):

    project_id = 42
    relay = relay(mini_sentry)

    response = relay.post(
        "/api/%s/upload/?sentry_key=%s"
        % (project_id, mini_sentry.get_dsn_public_key(project_id)),
        headers={
            "Tus-Resumable": "0.2.0",
            "Upload-Length": "5",
        },
        data=b"hello",
    )

    assert response.status_code == 400
    assert response.json() == {
        "detail": "TUS protocol error: expected Tus-Resumable: 1.0.0, got: 0.2.0",
        "causes": ["expected Tus-Resumable: 1.0.0, got: 0.2.0"],
    }


def test_upload_missing_upload_length(mini_sentry, relay, dummy_upload, project_config):

    project_id = 42
    relay = relay(mini_sentry)

    response = relay.post(
        "/api/%s/upload/?sentry_key=%s"
        % (project_id, mini_sentry.get_dsn_public_key(project_id)),
        headers={
            "Tus-Resumable": "1.0.0",
        },
        data=b"hello",
    )

    assert response.status_code == 400
    assert response.json() == {
        "detail": (
            "TUS protocol error: expected Upload-Length or Upload-Defer-Length=1, "
            "got Upload-Length=None, Upload-Defer-Length=None"
        ),
        "causes": [
            "expected Upload-Length or Upload-Defer-Length=1, "
            "got Upload-Length=None, Upload-Defer-Length=None"
        ],
    }


@pytest.mark.parametrize(
    "size,expected_status_code,expected_error",
    [
        pytest.param(
            10,
            400,
            "stream shorter than lower bound: received 10 < 11",
            id="smaller_than_announced",
        ),
        pytest.param(
            12,
            400,
            "stream exceeded upper bound: received 12 > 11",
            id="larger_than_announced",
        ),
        pytest.param(101, 413, "length limit exceeded", id="larger_than_allowed"),
    ],
)
def test_upload_body_size(
    mini_sentry,
    relay,
    size,
    expected_status_code,
    expected_error,
    dummy_upload,
    project_config,
):

    project_id = 42
    relay = relay(
        mini_sentry,
        {
            "limits": {
                "max_upload_size": 100,
            }
        },
    )

    data = "x" * size
    response = relay.patch(
        "%s&sentry_key=%s"
        % (DUMMY_UPLOAD_LOCATION, mini_sentry.get_dsn_public_key(project_id)),
        headers={
            "Tus-Resumable": "1.0.0",
            "Content-Type": "application/offset+octet-stream",
            "Upload-Offset": "0",
        },
        data=data,
    )

    assert response.status_code == expected_status_code
    assert response.text == expected_error or any(
        expected_error in source for source in response.json()["causes"]
    ), response.json()


@pytest.mark.parametrize("data_category", ["attachment", "attachment_item"])
def test_upload_rate_limited(
    mini_sentry, relay, data_category, dummy_upload, project_config
):
    """Request is rate limited on the fast path

    NOTE: It would be nice if this also worked for the "error" data category,
    but the `EnvelopeLimiter` does not check the event rate limit when there's only attachments,
    because for classic envelopes it cannot distinguish between event and transaction attachments.
    """
    project_id = 42
    project_config["quotas"] = [
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
            },
            data=b"hello",
        )

    response = request()
    assert response.status_code == 429
    assert "rate limit" in response.json()["detail"]


@pytest.mark.parametrize(
    "http_timeout, upload_timeout, expected_status_code",
    [
        pytest.param(1, 60, 204, id="http"),
        pytest.param(60, 1, 504, id="upload"),
    ],
)
def test_timeout(
    mini_sentry,
    relay,
    project_config,
    http_timeout,
    upload_timeout,
    expected_status_code,
):
    """Ensure that the general HTTP timeout does not affect the upload endpoint"""
    mini_sentry.allow_chunked = True

    @mini_sentry.app.route(DUMMY_UPLOAD_PATH, methods=["PATCH"])
    def slow_upload(**opts):
        time.sleep(2)
        return Response("", status=204, headers={"Location": DUMMY_UPLOAD_LOCATION})

    project_id = 42
    relay = relay(
        mini_sentry,
        options={
            "http": {"timeout": http_timeout},
            "upload": {"timeout": upload_timeout},
        },
    )

    data = b"hello world"
    response = relay.patch(
        "%s&sentry_key=%s"
        % (DUMMY_UPLOAD_LOCATION, mini_sentry.get_dsn_public_key(project_id)),
        headers={
            "Tus-Resumable": "1.0.0",
            "Upload-Offset": "0",
            "Content-Type": "application/offset+octet-stream",
        },
        data=data,
    )

    assert response.status_code == expected_status_code, response.text
    if expected_status_code == 504:
        assert response.json() == {
            "detail": "upload error: request timeout: deadline has elapsed",
            "causes": [
                "request timeout: deadline has elapsed",
                "deadline has elapsed",
            ],
        }


@pytest.mark.parametrize(
    "chain", [pytest.param(False, id="processing_only"), pytest.param(True, id="chain")]
)
def test_create_processing(
    mini_sentry, relay, relay_with_processing, chain, project_config
):
    """Create and separate upload via processing relay stores the blob in objectstore."""
    project_id = 42
    project_key = mini_sentry.get_dsn_public_key(project_id)

    processing_relay = relay_with_processing()
    if chain:
        relay = relay(processing_relay)
    else:
        relay = processing_relay

    data = b"hello world"
    response = relay.post(
        f"/api/{project_id}/upload/?sentry_key={project_key}",
        headers={
            "Content-Length": "0",
            "Tus-Resumable": "1.0.0",
            "Upload-Length": str(len(data)),
        },
    )

    assert response.status_code == 201
    assert response.headers["Tus-Resumable"] == "1.0.0"
    assert "Upload-Offset" not in response.headers

    # Use the location to send a PATCH request:
    data = b"hello world"
    response = relay.patch(
        f"{response.headers['Location']}&sentry_key={project_key}",
        headers={
            "Content-Length": str(len(data)),
            "Content-Type": "application/offset+octet-stream",
            "Tus-Resumable": "1.0.0",
            "Upload-Offset": "0",
        },
        data=data,
    )

    assert response.status_code == 204
    assert response.headers["Tus-Resumable"] == "1.0.0"
    assert response.headers["Upload-Offset"] == str(len(data)), response.headers


@pytest.mark.parametrize("length", [9, 11])
def test_processing_invalid_length(
    mini_sentry, relay, relay_with_processing, project_config, length
):
    mini_sentry.fail_on_relay_error = False
    project_id = 42
    project_key = mini_sentry.get_dsn_public_key(project_id)

    relay = relay_with_processing()

    response = relay.post(
        f"/api/{project_id}/upload/?sentry_key={project_key}",
        headers={
            "Content-Length": "0",
            "Tus-Resumable": "1.0.0",
            "Upload-Length": "10",
        },
    )

    assert response.status_code == 201
    assert response.headers["Tus-Resumable"] == "1.0.0"
    assert "Upload-Offset" not in response.headers

    # Use the location to send a PATCH request that is too long // too short
    data = length * b"X"
    response = relay.patch(
        f"{response.headers['Location']}&sentry_key={project_key}",
        headers={
            "Content-Length": str(len(data)),
            "Content-Type": "application/offset+octet-stream",
            "Tus-Resumable": "1.0.0",
            "Upload-Offset": "0",
        },
        data=data,
    )

    assert response.status_code == 400


@pytest.mark.parametrize("defer_length_value", ["1", "2"])
def test_upload_with_deferred_length(
    mini_sentry, relay, relay_with_processing, project_config, defer_length_value
):
    project_id = 42
    processing_relay = relay_with_processing()
    relay = relay(processing_relay)

    response = relay.post(
        "/api/%s/upload/?sentry_key=%s"
        % (project_id, mini_sentry.get_dsn_public_key(project_id)),
        headers={
            "Tus-Resumable": "1.0.0",
            "Upload-Defer-Length": defer_length_value,
        },
    )

    expected_status_code = 403 if defer_length_value == "1" else 400
    assert response.status_code == expected_status_code
    if defer_length_value == "1":
        assert response.json() == {
            "detail": "TUS protocol error: Upload-Defer-Length not allowed",
            "causes": ["Upload-Defer-Length not allowed"],
        }
    else:
        assert response.json() == {
            "detail": (
                "TUS protocol error: expected Upload-Length or Upload-Defer-Length=1, "
                "got Upload-Length=None, Upload-Defer-Length=Some(2)"
            ),
            "causes": [
                "expected Upload-Length or Upload-Defer-Length=1, "
                "got Upload-Length=None, Upload-Defer-Length=Some(2)"
            ],
        }


def test_concurrency_limit(mini_sentry, relay, project_config):
    """Exceeding upload.max_concurrent_requests results in 503 Service Unavailable."""

    project_id = 42
    project_key = mini_sentry.get_dsn_public_key(project_id)
    timeout = 2

    mini_sentry.allow_chunked = True
    relay.capture_logs = True

    @mini_sentry.app.route("/api/<project>/upload/<key>/", methods=["PATCH"])
    def slow_upstream(**opts):
        time.sleep(timeout + 1)

    relay = relay(
        mini_sentry,
        {"upload": {"max_concurrent_requests": 1, "timeout": 1}},
    )

    data = "hello world"

    def do_upload():
        return relay.patch(
            f"{DUMMY_UPLOAD_LOCATION}&sentry_key={project_key}",
            headers={
                "Content-Length": str(len(data)),
                "Content-Type": "application/offset+octet-stream",
                "Tus-Resumable": "1.0.0",
                "Upload-Offset": "0",
            },
            data=data,
        )

    with ThreadPoolExecutor(max_workers=10) as pool:
        futures = [pool.submit(do_upload) for _ in range(10)]
        results = [f.result() for f in as_completed(futures)]

    status_codes = {r.status_code for r in results}

    # Some requests hit a timeout, the others are loadshed:
    assert status_codes == {503, 504}
    for r in results:
        if r.status_code == 503:
            assert r.json() == {
                "detail": "upload error: loadshed",
                "causes": ["loadshed"],
            }, r.text
        else:
            assert r.json() == {
                "detail": "upload error: request timeout: deadline has elapsed",
                "causes": [
                    "request timeout: deadline has elapsed",
                    "deadline has elapsed",
                ],
            }, r.text


def test_objectstore_retries(mini_sentry, relay_with_processing, project_config):
    """Upload succeeds after a transient connection failure thanks to stream retries.

    The first objectstore connection attempt fails (nothing listening yet).
    The retry delay gives time for the mock objectstore to start, and the
    second attempt succeeds because the stream has not been consumed yet.
    """
    mini_sentry.fail_on_relay_errors = False
    project_id = 42
    project_key = mini_sentry.get_dsn_public_key(project_id)

    relay = relay_with_processing(
        options={
            "processing": {
                "objectstore": {
                    "objectstore_url": "http://127.0.0.1:8889/",  # wrong port
                    "retry_delay": 1.0,
                    "max_attempts": 3,
                }
            }
        }
    )

    # Create the upload (this does NOT contact objectstore).
    data = b"hello world"
    response = relay.post(
        f"/api/{project_id}/upload/?sentry_key={project_key}",
        headers={
            "Content-Length": "0",
            "Tus-Resumable": "1.0.0",
            "Upload-Length": str(len(data)),
        },
    )
    assert response.status_code == 201

    response = relay.patch(
        f"{response.headers['Location']}&sentry_key={project_key}",
        headers={
            "Content-Length": str(len(data)),
            "Content-Type": "application/offset+octet-stream",
            "Tus-Resumable": "1.0.0",
            "Upload-Offset": "0",
        },
        data=data,
    )

    failure = mini_sentry.test_failures.get(timeout=10)
    assert "failed to upload 1 attachment(s) to objectstore in 3 attempt(s)" in str(
        failure
    )
    assert response.status_code == 500
