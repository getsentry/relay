"""
Tests for the TUS upload endpoint (/api/{project_id}/upload/).
"""

import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import timedelta
from urllib.parse import urlparse

from flask import Response, request
import pytest

from sentry_relay.auth import SecretKey
from sentry_sdk.envelope import Envelope
from objectstore_client.metadata import TimeToLive

from .consts import (
    DUMMY_UPLOAD_ONESHOT_LOCATION,
    DUMMY_UPLOAD_PATH,
    DUMMY_UPLOAD_LOCATION,
)
from .consts import Outcome
from .test_attachment_ref import make_ref_item
from .tus import (
    FIRST_CHUNK,
    RESUMABLE_DATA,
    SECOND_CHUNK,
    head_upload,
    location_parts,
    patch_chunk,
    rebuild_location,
)


@pytest.fixture
def project_config(mini_sentry):
    project_id = 42
    config = mini_sentry.add_full_project_config(project_id)["config"]
    config.setdefault("features", []).extend(
        ["projects:relay-minidump-uploads", "projects:resumable-uploads"]
    )
    return config


@pytest.mark.parametrize(
    "killswitched,expected_status_code",
    [
        pytest.param(False, 201, id="killswitch off"),
        pytest.param(True, 503, id="killswitch on"),
    ],
)
def test_forward_create(
    mini_sentry, relay, dummy_upload, killswitched, expected_status_code
):
    project_id = 42
    mini_sentry.add_full_project_config(project_id)
    if killswitched:
        mini_sentry.global_config["options"][
            "relay.endpoint-fetch-config.enabled"
        ] = False
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


@pytest.mark.parametrize("upload_max_chunk_size", [0, 1_000_000_000])
def test_header(mini_sentry, relay, dummy_upload, upload_max_chunk_size):
    project_id = 42
    mini_sentry.add_full_project_config(project_id)
    mini_sentry.global_config["options"][
        "relay.upload-max-chunk.size"
    ] = upload_max_chunk_size
    relay = relay(mini_sentry)

    response = relay.post(
        "/api/%s/upload/?sentry_key=%s"
        % (project_id, mini_sentry.get_dsn_public_key(project_id)),
        headers={
            "Tus-Resumable": "1.0.0",
            "Upload-Length": "11",
        },
    )

    assert response.status_code == 201, response.text
    if upload_max_chunk_size != 0:
        assert int(response.headers["Upload-Max-Chunk-Size"]) == upload_max_chunk_size
    else:
        assert "Upload-Max-Chunk-Size" not in response.headers


@pytest.mark.parametrize(
    "killswitched,expected_status_code",
    [
        pytest.param(False, 204, id="killswitch off"),
        pytest.param(True, 503, id="killswitch on"),
    ],
)
def test_forward_patch(
    mini_sentry, relay, dummy_upload, killswitched, expected_status_code
):
    project_id = 42
    mini_sentry.add_full_project_config(project_id)
    if killswitched:
        mini_sentry.global_config["options"][
            "relay.endpoint-fetch-config.enabled"
        ] = False
    relay = relay(mini_sentry)

    data = b"hello world"
    response = relay.patch(
        "%s&sentry_key=%s"
        % (
            DUMMY_UPLOAD_LOCATION,
            mini_sentry.get_dsn_public_key(project_id),
        ),
        headers={
            "Tus-Resumable": "1.0.0",
            "Content-Type": "application/offset+octet-stream",
            "Upload-Offset": "0",
            "X-Decoded-Content-Length": str(len(data)),
        },
        data=data,
    )

    assert response.status_code == expected_status_code, response.text


@pytest.mark.parametrize(
    "header,value,expected_status_code,expected_detail",
    [
        pytest.param(
            "Upload-Offset",
            None,
            400,
            "expected Upload-Offset >= 0",
            id="offset missing",
        ),
        pytest.param(
            "Content-Type",
            "application/octet-stream",
            415,
            "expected Content-Type: application/offset+octet-stream, "
            "got: application/octet-stream",
            id="wrong content type",
        ),
        pytest.param(
            "Content-Type",
            None,
            415,
            "expected Content-Type: application/offset+octet-stream, got: ",
            id="missing content type",
        ),
    ],
)
def test_invalid_headers(
    mini_sentry,
    relay,
    dummy_upload,
    header,
    value,
    expected_status_code,
    expected_detail,
):
    project_id = 42
    mini_sentry.add_full_project_config(project_id)
    relay = relay(mini_sentry)

    headers = {
        "Tus-Resumable": "1.0.0",
        "Content-Type": "application/offset+octet-stream",
        "Upload-Offset": "0",
    }
    if value is None:
        del headers[header]
    else:
        headers[header] = value

    response = relay.patch(
        "%s&sentry_key=%s"
        % (
            DUMMY_UPLOAD_LOCATION,
            mini_sentry.get_dsn_public_key(project_id),
        ),
        headers=headers,
        data=b"hello world",
    )

    assert response.status_code == expected_status_code, response.text
    assert response.headers["Tus-Resumable"] == "1.0.0"
    assert response.json() == {"detail": expected_detail}


def test_post_retries(mini_sentry, relay, project_config):
    """POST (create) requests forwarded to the upstream are retried.

    The upstream returns 503 on the first attempt and succeeds on the second.
    """
    mini_sentry.allow_chunked = True

    create_attempts = 0

    @mini_sentry.app.route("/api/<project>/upload/", methods=["POST"])
    def create(**opts):
        nonlocal create_attempts
        create_attempts += 1
        if create_attempts == 1:
            return Response("", status=503)
        return Response("", status=201, headers={"Location": DUMMY_UPLOAD_LOCATION})

    project_id = 42
    project_key = mini_sentry.get_dsn_public_key(project_id)
    relay = relay(mini_sentry)

    response = relay.post(
        f"/api/{project_id}/upload/?sentry_key={project_key}",
        headers={
            "Tus-Resumable": "1.0.0",
            "Upload-Length": "11",
        },
    )
    assert response.status_code == 201, response.text
    assert create_attempts == 2


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

    assert response.status_code == 412
    assert response.headers["Tus-Version"] == "1.0.0"
    assert response.json() == {
        "detail": "expected Tus-Resumable: 1.0.0, got: (missing)",
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

    assert response.status_code == 412
    assert response.headers["Tus-Version"] == "1.0.0"
    assert response.json() == {
        "detail": "expected Tus-Resumable: 1.0.0, got: 0.2.0",
    }


def test_upload_with_metadata(
    mini_sentry,
    relay,
    dummy_upload,
    project_config,
):
    project_id = 42
    relay = relay(mini_sentry)

    response = relay.post(
        "/api/%s/upload/?sentry_key=%s"
        % (project_id, mini_sentry.get_dsn_public_key(project_id)),
        headers={
            "Tus-Resumable": "1.0.0",
            "Upload-Defer-Length": "1",
            "Upload-Metadata": "sentry eyJhdHRhY2htZW50X3R5cGUiOiAiZXZlbnQubWluaWR1bXAifQ==",
        },
    )

    assert response.status_code == 201


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
        "detail": "expected Upload-Length or Upload-Defer-Length=1, got Upload-Length=None, Upload-Defer-Length=None",
    }


@pytest.mark.parametrize(
    "size,expected_status_code,expected_error",
    [
        pytest.param(
            10,
            204,
            None,
            id="smaller_than_announced",
        ),
        pytest.param(
            12,
            413,
            "Chunk of 12 bytes exceeds the remaining 11 bytes",
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
        % (
            DUMMY_UPLOAD_LOCATION,
            mini_sentry.get_dsn_public_key(project_id),
        ),
        headers={
            "Tus-Resumable": "1.0.0",
            "Content-Type": "application/offset+octet-stream",
            "Upload-Offset": "0",
            "X-Decoded-Content-Length": str(len(data)),
        },
        data=data,
    )

    assert response.status_code == expected_status_code
    if expected_error:
        assert expected_error in response.text, response.text


@pytest.mark.parametrize(
    "content_encoding,expected_error",
    [
        pytest.param(None, None, id="no_encoding"),
        pytest.param("utf-8", None, id="identity_encoding"),
        pytest.param("zstd", "Missing X-Decoded-Content-Length header", id="zstd"),
    ],
)
def test_missing_decoded_length(
    mini_sentry,
    relay,
    dummy_upload,
    project_config,
    content_encoding,
    expected_error,
):
    """X-Decoded-Content-Length can be omitted when Content-Length is set"""
    project_id = 42
    relay = relay(mini_sentry)

    data = "foo"
    response = relay.patch(
        f"{DUMMY_UPLOAD_LOCATION}&sentry_key={mini_sentry.get_dsn_public_key(project_id)}",
        headers={
            "Tus-Resumable": "1.0.0",
            "Content-Type": "application/offset+octet-stream",
            "Content-Encoding": content_encoding,
            # NOTE: `Content-Length` is automatically set by the client library.
            "Upload-Offset": "0",
        },
        data=data,
    )

    if expected_error:
        assert response.status_code == 400
        assert expected_error in response.text, response.text
    else:
        assert response.status_code == 204


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
        return Response(
            "",
            status=204,
            headers={
                "Location": DUMMY_UPLOAD_LOCATION,
                "Upload-Offset": "0",
            },
        )

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
        % (
            DUMMY_UPLOAD_LOCATION,
            mini_sentry.get_dsn_public_key(project_id),
        ),
        headers={
            "Tus-Resumable": "1.0.0",
            "Upload-Offset": "0",
            "Content-Type": "application/offset+octet-stream",
            "X-Decoded-Content-Length": str(len(data)),
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
    "chain",
    [pytest.param(False, id="processing_only"), pytest.param(True, id="chain")],
)
@pytest.mark.parametrize(
    "feature_flag",
    [pytest.param(False, id="legacy"), pytest.param(True, id="resumable")],
)
def test_create_processing(
    mini_sentry,
    relay,
    relay_with_processing,
    chain,
    feature_flag,
    project_config,
    events_consumer,
):
    """Create and separate upload via processing relay stores the blob in objectstore."""
    project_id = 42
    project_key = mini_sentry.get_dsn_public_key(project_id)
    if not feature_flag:
        project_config.get("features", []).remove("projects:resumable-uploads")

    processing_relay = relay_with_processing()
    if chain:
        relay = relay(processing_relay)
    else:
        relay = processing_relay

    # Do some busy work until the global config is loaded
    events_consumer = events_consumer()
    relay.send_event(project_id)
    events_consumer.get_event()

    data = b"hello world"
    response = relay.post(
        f"/api/{project_id}/upload/?sentry_key={project_key}",
        headers={
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
            **({"X-Decoded-Content-Length": str(len(data))} if feature_flag else {}),
            "Content-Type": "application/offset+octet-stream",
            "Tus-Resumable": "1.0.0",
            "Upload-Offset": "0",
        },
        data=data,
    )

    assert response.status_code == 204, response.text
    assert response.headers["Tus-Resumable"] == "1.0.0"
    assert response.headers["Upload-Offset"] == str(len(data)), response.headers


def test_processing_invalid_length(
    mini_sentry,
    relay,
    relay_with_processing,
    project_config,
):
    mini_sentry.fail_on_relay_error = False
    project_id = 42
    project_key = mini_sentry.get_dsn_public_key(project_id)

    relay = relay_with_processing()

    response = relay.post(
        f"/api/{project_id}/upload/?sentry_key={project_key}",
        headers={
            "Tus-Resumable": "1.0.0",
            "Upload-Length": "10",
        },
    )

    assert response.status_code == 201
    assert response.headers["Tus-Resumable"] == "1.0.0"
    assert "Upload-Offset" not in response.headers

    # Use the location to send a PATCH request that is too long // too short
    data = 11 * b"X"
    response = relay.patch(
        f"{response.headers['Location']}&sentry_key={project_key}",
        headers={
            "X-Decoded-Content-Length": str(len(data)),
            "Content-Type": "application/offset+octet-stream",
            "Tus-Resumable": "1.0.0",
            "Upload-Offset": "0",
        },
        data=data,
    )

    assert response.status_code == 413, response.text


@pytest.mark.parametrize("defer_length_value", ["1", "2"])
def test_upload_with_deferred_length(
    mini_sentry,
    relay,
    relay_with_processing,
    project_config,
    events_consumer,
    defer_length_value,
):
    project_id = 42
    processing_relay = relay_with_processing()
    relay = relay(processing_relay)

    # Do some busy work until the global config is loaded
    events_consumer = events_consumer()
    relay.send_event(project_id)
    events_consumer.get_event()

    response = relay.post(
        "/api/%s/upload/?sentry_key=%s"
        % (project_id, mini_sentry.get_dsn_public_key(project_id)),
        headers={
            "Tus-Resumable": "1.0.0",
            "Upload-Defer-Length": defer_length_value,
        },
    )

    if defer_length_value == "1":
        assert response.status_code == 201
    else:
        assert response.status_code == 400
        assert response.json() == {
            "detail": "expected Upload-Length or Upload-Defer-Length=1, got Upload-Length=None, Upload-Defer-Length=Some(2)",
        }


@pytest.mark.parametrize(
    "feature_flag",
    [pytest.param(False, id="legacy"), pytest.param(True, id="resumable")],
)
def test_concurrency_limit(mini_sentry, relay, project_config, feature_flag):
    """Exceeding upload.max_concurrent_requests results in 503 Service Unavailable."""

    project_id = 42
    project_key = mini_sentry.get_dsn_public_key(project_id)
    if not feature_flag:
        project_config.get("features", []).remove("projects:resumable-uploads")

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
        location = (
            DUMMY_UPLOAD_LOCATION if feature_flag else DUMMY_UPLOAD_ONESHOT_LOCATION
        )
        return relay.patch(
            f"{location}&sentry_key={project_key}",
            headers={
                **(
                    {"X-Decoded-Content-Length": str(len(data))} if feature_flag else {}
                ),
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


@pytest.mark.parametrize(
    "feature_flag",
    [pytest.param(False, id="legacy"), pytest.param(True, id="resumable")],
)
def test_objectstore_retries(
    mini_sentry, relay_with_processing, project_config, feature_flag
):
    project_id = 42
    project_key = mini_sentry.get_dsn_public_key(project_id)
    if not feature_flag:
        project_config.get("features", []).remove("projects:resumable-uploads")

    relay = relay_with_processing(
        options={
            "processing": {
                "objectstore": {
                    "objectstore_url": "http://localhost:1337",  # invalid port
                    "retry_delay": 1.0,
                    "max_attempts": 3,
                }
            }
        }
    )

    location = f"/api/{project_id}/upload/019cdc82ed6c7761ba21fd34b86481c2/"
    sep = "?"
    signature = SecretKey.parse(relay.secret_key).sign(location.encode())
    signed_location = (
        f"{location}{sep}sentry_key={project_key}&upload_signature={signature}"
    )

    data = b"hello world"
    response = relay.patch(
        signed_location,
        headers={
            **({"X-Decoded-Content-Length": str(len(data))} if feature_flag else {}),
            "Content-Type": "application/offset+octet-stream",
            "Tus-Resumable": "1.0.0",
            "Upload-Offset": "0",
        },
        data=data,
    )
    print(response.text)

    failure = mini_sentry.test_failures.get(timeout=10)
    assert "failed to upload 1 attachment(s) to objectstore in 3 attempt(s)" in str(
        failure
    )
    assert response.status_code == 500


def test_objectstore_upload_uncompressed(
    mini_sentry, relay_with_processing, project_config
):
    mini_sentry.allow_chunked = True
    project_id = 42
    project_key = mini_sentry.get_dsn_public_key(project_id)
    uploads = []

    @mini_sentry.app.route("/v1/objects/attachments/<scope>/<key>", methods=["PUT"])
    def decline_resumable(scope, key):
        return "", 501

    @mini_sentry.app.route("/v1/objects/attachments/<scope>/", methods=["POST"])
    def upload(scope):
        uploads.append((request.headers.get("Content-Encoding"), request.get_data()))
        return {"key": "some_key"}

    relay = relay_with_processing(
        options={
            "processing": {
                "objectstore": {
                    "objectstore_url": mini_sentry.url,
                }
            }
        }
    )

    response = upload_something(
        relay,
        project_id,
        project_key,
        legacy=True,
    )

    assert response.status_code == 204, response.text
    assert uploads == [(None, b"hello world")]


def test_objectstore_timeout(
    mini_sentry, relay_with_processing, project_config, dummy_upload
):
    mini_sentry.allow_chunked = True
    mini_sentry.fail_on_relay_error = False
    project_id = 42
    project_key = mini_sentry.get_dsn_public_key(project_id)

    @mini_sentry.app.route("/v1/objects/attachments/<scope>/<key>", methods=["PUT"])
    def decline_resumable(scope, key):
        return "", 501

    @mini_sentry.app.route("/v1/objects/attachments/<scope>/", methods=["POST"])
    def slow_upload(scope):
        time.sleep(2)
        raise NotImplementedError

    relay = relay_with_processing(
        options={
            "processing": {
                "objectstore": {
                    "objectstore_url": mini_sentry.url,
                    "stream_timeout": 1,
                }
            }
        }
    )

    response = upload_something(relay, project_id, project_key, legacy=True)

    assert response.status_code == 504


def upload_something(relay, project_id, project_key, legacy):
    data = b"hello world"
    response = relay.post(
        f"/api/{project_id}/upload/?sentry_key={project_key}",
        headers={
            "Tus-Resumable": "1.0.0",
            **(
                {"Upload-Defer-Length": "1"}
                if legacy
                else {"Upload-Length": str(len(data))}
            ),
        },
    )
    assert response.status_code == 201, response.json()

    return relay.patch(
        f"{response.headers['Location']}&sentry_key={project_key}",
        headers={
            "X-Decoded-Content-Length": str(len(data)),
            "Content-Type": "application/offset+octet-stream",
            "Tus-Resumable": "1.0.0",
            "Upload-Offset": "0",
        },
        data=data,
    )


@pytest.mark.parametrize(
    "feature_flag",
    [pytest.param(False, id="legacy"), pytest.param(True, id="resumable")],
)
def test_objectstore_retention(
    mini_sentry, relay_with_processing, objectstore, feature_flag, project_config
):
    project_id = 42
    project_config["eventRetention"] = 20
    project_key = mini_sentry.get_dsn_public_key(project_id)
    if not feature_flag:
        project_config.get("features", []).remove("projects:resumable-uploads")

    relay = relay_with_processing()

    data = b"hello world"
    create = relay.post(
        f"/api/{project_id}/upload/?sentry_key={project_key}",
        headers={
            "Tus-Resumable": "1.0.0",
            "Upload-Length": str(len(data)),
        },
    )
    assert create.status_code == 201, create.text
    location = create.headers["Location"]
    key = urlparse(location).path.rstrip("/").split("/")[-1]

    patch = relay.patch(
        f"{location}&sentry_key={project_key}",
        headers={
            **({"X-Decoded-Content-Length": str(len(data))} if feature_flag else {}),
            "Content-Type": "application/offset+octet-stream",
            "Tus-Resumable": "1.0.0",
            "Upload-Offset": "0",
        },
        data=data,
    )
    assert patch.status_code == 204, patch.text
    key = urlparse(patch.headers["Location"]).path.rstrip("/").split("/")[-1]

    meta = objectstore("attachments", project_id).head(key)
    assert meta.expiration_policy == TimeToLive(timedelta(days=20))


@pytest.mark.parametrize(
    "opted_in,metadata,expected_status_code",
    [
        pytest.param(False, False, 201, id="default_type_allowed"),
        pytest.param(True, True, 201, id="minidump_opted_in"),
        pytest.param(False, True, 403, id="minidump_not_opted_in"),
    ],
)
def test_upload_minidump_opt_in(
    mini_sentry,
    relay,
    dummy_upload,
    project_config,
    opted_in,
    metadata,
    expected_status_code,
):
    project_id = 42
    if not opted_in:
        project_config["features"].remove("projects:relay-minidump-uploads")

    relay = relay(mini_sentry, options={"outcomes": {"emit_outcomes": True}})

    headers = {
        "Tus-Resumable": "1.0.0",
        "Upload-Length": "11",
    }
    if metadata:
        headers["Upload-Metadata"] = (
            "sentry eyJhdHRhY2htZW50X3R5cGUiOiAiZXZlbnQubWluaWR1bXAifQ=="
        )

    response = relay.post(
        "/api/%s/upload/?sentry_key=%s"
        % (project_id, mini_sentry.get_dsn_public_key(project_id)),
        headers=headers,
    )

    assert response.status_code == expected_status_code

    if expected_status_code == 403:
        assert (
            response.json()["detail"]
            == "event submission rejected with_reason: FeatureDisabled(MinidumpUploads)"
        )
        outcomes = mini_sentry.get_outcomes(n=1)
        assert any(
            o["outcome"] == Outcome.INVALID and o["reason"] == "feature_disabled"
            for o in outcomes
        )
    else:
        assert mini_sentry.captured_outcomes.empty()


@pytest.mark.parametrize(
    "overrides,expected_status_code,expected_error",
    [
        pytest.param(
            {"offset": 0, "chunk": FIRST_CHUNK},
            409,
            f"upload offset mismatch (server holds {len(FIRST_CHUNK)} bytes)",
            id="retry_first_chunk",
        ),
        pytest.param(
            {"offset": len(RESUMABLE_DATA) + 1},
            409,
            f"Invalid Upload-Offset {len(RESUMABLE_DATA) + 1} for Upload-Length {len(RESUMABLE_DATA)}",
            id="offset_beyond_length",
        ),
        pytest.param(
            {"chunk": SECOND_CHUNK + b"!"},
            413,
            f"Chunk of {len(SECOND_CHUNK) + 1} bytes exceeds the remaining {len(SECOND_CHUNK)} bytes",
            id="chunk_exceeds_remaining",
        ),
        pytest.param(
            {"params": {"upload_length": str(len(RESUMABLE_DATA) + 100)}},
            400,
            "invalid signature",
            id="tampered_upload_length",
        ),
        pytest.param(
            {"params": {"upload_id": "tampered"}},
            400,
            "invalid signature",
            id="tampered_upload_id",
        ),
    ],
)
def test_resumable_upload_errors(
    mini_sentry,
    relay_with_processing,
    project_config,
    objectstore,
    overrides,
    expected_status_code,
    expected_error,
):
    mini_sentry.fail_on_relay_error = False
    project_id = 42
    project_key = mini_sentry.get_dsn_public_key(project_id)
    relay = relay_with_processing()

    # Create the resumable upload
    create = relay.post(
        f"/api/{project_id}/upload/?sentry_key={project_key}",
        headers={
            "Tus-Resumable": "1.0.0",
            "Upload-Length": str(len(RESUMABLE_DATA)),
        },
    )
    assert create.status_code == 201, create.text

    # First upload to get into the resumable state
    first = patch_chunk(relay, create.headers["Location"], project_key, FIRST_CHUNK, 0)
    assert first.status_code == 204, first.text
    location = first.headers["Location"]
    path, params = location_parts(location)
    key = path.rstrip("/").split("/")[-1]
    assert "upload_id" in params

    offset = overrides.get("offset", len(FIRST_CHUNK))
    chunk = overrides.get("chunk", SECOND_CHUNK)
    headers = overrides.get("headers", {})
    if "params" in overrides:
        params.update(overrides["params"])
        location = rebuild_location(path, params)

    # Second (broken) upload
    response = patch_chunk(relay, location, project_key, chunk, offset, headers)
    assert response.status_code == expected_status_code, response.text
    assert expected_error in response.text, response.text

    # Resuming correctly must still work after the failed attempt.
    second = patch_chunk(
        relay,
        first.headers["Location"],
        project_key,
        SECOND_CHUNK,
        len(FIRST_CHUNK),
    )
    assert second.status_code == 204, second.text
    assert second.headers["Upload-Offset"] == str(len(RESUMABLE_DATA))

    objectstore_session = objectstore("attachments", project_id)
    assert objectstore_session.get(key).payload.read() == RESUMABLE_DATA


def test_patch_completed_upload(
    mini_sentry, relay_with_processing, project_config, objectstore
):
    mini_sentry.fail_on_relay_error = False
    project_id = 42
    project_key = mini_sentry.get_dsn_public_key(project_id)
    relay = relay_with_processing()

    create = relay.post(
        f"/api/{project_id}/upload/?sentry_key={project_key}",
        headers={
            "Tus-Resumable": "1.0.0",
            "Upload-Length": str(len(RESUMABLE_DATA)),
        },
    )
    assert create.status_code == 201, create.text

    first = patch_chunk(relay, create.headers["Location"], project_key, FIRST_CHUNK, 0)
    assert first.status_code == 204, first.text

    second = patch_chunk(
        relay, first.headers["Location"], project_key, SECOND_CHUNK, len(FIRST_CHUNK)
    )
    assert second.status_code == 204, second.text
    final_location = second.headers["Location"]
    final_path, final_params = location_parts(final_location)
    assert "upload_id" not in final_params

    # For legacy reasons you could do another patch here, but not with more data than `Upload-Length`.
    response = patch_chunk(
        relay,
        final_location,
        project_key,
        FIRST_CHUNK + SECOND_CHUNK + FIRST_CHUNK,
        len(FIRST_CHUNK),
    )
    assert response.status_code == 400, response.text
    assert "stream exceeded upper bound" in response.text

    key = final_path.rstrip("/").split("/")[-1]
    objectstore_session = objectstore("attachments", project_id)
    assert objectstore_session.get(key).payload.read() == RESUMABLE_DATA


@pytest.mark.parametrize(
    "resumable_feature,defer_length",
    [
        pytest.param(True, True, id="feature_on_deferred_length"),
        pytest.param(False, False, id="feature_off_known_length"),
        pytest.param(False, True, id="feature_off_deferred_length"),
    ],
)
def test_oneshot_fallback(
    mini_sentry,
    relay_with_processing,
    objectstore,
    resumable_feature,
    defer_length,
    project_config,
):

    project_id = 42
    if not resumable_feature:
        project_config["features"].remove("projects:resumable-uploads")
    project_key = mini_sentry.get_dsn_public_key(project_id)
    relay = relay_with_processing()

    data = b"oneshot payload"
    headers = {"Tus-Resumable": "1.0.0"}
    if defer_length:
        headers["Upload-Defer-Length"] = "1"
    else:
        headers["Upload-Length"] = str(len(data))

    create = relay.post(
        f"/api/{project_id}/upload/?sentry_key={project_key}", headers=headers
    )
    assert create.status_code == 201, create.text
    _, params = location_parts(create.headers["Location"])
    assert "upload_id" not in params
    assert "upload_signature" in params

    if defer_length:
        assert "upload_length" not in params
    else:
        # Legacy clients declare the length up front, the location keeps it.
        assert params["upload_length"] == str(len(data))

    patch = patch_chunk(relay, create.headers["Location"], project_key, data, 0)
    assert patch.status_code == 204, patch.text
    assert patch.headers["Upload-Offset"] == str(len(data))
    final_path, final_params = location_parts(patch.headers["Location"])
    assert final_path.startswith(f"/api/{project_id}/upload/")
    assert final_params["upload_length"] == str(len(data))
    assert "upload_id" not in final_params

    key = final_path.rstrip("/").split("/")[-1]
    assert objectstore("attachments", project_id).get(key).payload.read() == data


@pytest.mark.parametrize(
    "granularity",
    [pytest.param(0), pytest.param(1024)],
)
def test_create_granularity_header(
    mini_sentry,
    relay,
    relay_with_processing,
    project_config,
    events_consumer,
    granularity,
):
    project_id = 42
    project_key = mini_sentry.get_dsn_public_key(project_id)

    @mini_sentry.app.route("/v1/objects/attachments/<scope>/<key>", methods=["PUT"])
    def create_session(scope, key):
        assert request.args["upload_type"] == "resumable"
        return {"key": key, "session": "c2Vzc2lvbg", "granularity": granularity}

    processing_relay = relay_with_processing(
        options={
            "processing": {
                "objectstore": {
                    "objectstore_url": mini_sentry.url,
                }
            }
        }
    )
    pop_relay = relay(processing_relay)

    # Do some busy work until the global config is loaded
    events_consumer = events_consumer()
    pop_relay.send_event(project_id)
    events_consumer.get_event()

    for target in (processing_relay, pop_relay):
        response = target.post(
            f"/api/{project_id}/upload/?sentry_key={project_key}",
            headers={
                "Tus-Resumable": "1.0.0",
                "Upload-Length": "11",
            },
        )

        assert response.status_code == 201, response.text
        assert "upload_id=" in response.headers["Location"]
        if granularity > 0:
            assert int(response.headers["Upload-Chunk-Granularity"]) == granularity
            # The minimum chunk size is currently derived from the granularity.
            assert int(response.headers["Upload-Min-Chunk-Size"]) == granularity
        else:
            assert "Upload-Chunk-Granularity" not in response.headers, response.headers
            assert "Upload-Min-Chunk-Size" not in response.headers, response.headers


@pytest.mark.parametrize(
    "chain",
    [pytest.param(False, id="processing_only"), pytest.param(True, id="chain")],
)
def test_resumable_upload_declined(
    mini_sentry, relay, relay_with_processing, project_config, events_consumer, chain
):
    project_id = 42
    project_key = mini_sentry.get_dsn_public_key(project_id)

    @mini_sentry.app.route("/v1/objects/attachments/<scope>/<key>", methods=["PUT"])
    def decline_resumable(scope, key):
        assert request.args["upload_type"] == "resumable"
        return "", 501

    mini_sentry.fail_on_relay_error = False
    processing_relay = relay_with_processing(
        options={
            "processing": {
                "objectstore": {
                    "objectstore_url": mini_sentry.url,
                }
            }
        }
    )
    relay = relay(processing_relay) if chain else processing_relay

    # Do some busy work until the global config is loaded
    events_consumer = events_consumer()
    relay.send_event(project_id)
    events_consumer.get_event()

    response = relay.post(
        f"/api/{project_id}/upload/?sentry_key={project_key}",
        headers={
            "Tus-Resumable": "1.0.0",
            "Upload-Length": "11",
        },
    )

    assert response.status_code == 501, response.text
    assert "resumable upload declined" in response.text
    assert "Location" not in response.headers


def test_head_resumable_progress(
    mini_sentry, relay, relay_with_processing, project_config, events_consumer
):
    project_id = 42
    project_key = mini_sentry.get_dsn_public_key(project_id)
    total = len(RESUMABLE_DATA)

    relay = relay(relay_with_processing())
    # Do some busy work until the global config is loaded
    events_consumer = events_consumer()
    relay.send_event(project_id)
    events_consumer.get_event()

    create = relay.post(
        f"/api/{project_id}/upload/?sentry_key={project_key}",
        headers={"Tus-Resumable": "1.0.0", "Upload-Length": str(total)},
    )
    assert create.status_code == 201, create.text
    location = create.headers["Location"]
    _, params = location_parts(location)
    assert "upload_id" in params

    response = head_upload(relay, location, project_key)
    assert response.status_code == 204, response.text
    assert response.headers["Tus-Resumable"] == "1.0.0"
    assert response.headers["Upload-Offset"] == "0"
    assert response.headers["Upload-Length"] == str(total)

    patch = patch_chunk(relay, location, project_key, FIRST_CHUNK, 0)
    assert patch.status_code == 204, patch.text
    assert patch.headers["Upload-Offset"] == str(len(FIRST_CHUNK))

    response = head_upload(relay, patch.headers["Location"], project_key)
    assert response.status_code == 204, response.text
    assert response.headers["Upload-Offset"] == patch.headers["Upload-Offset"]
    assert response.headers["Upload-Length"] == str(total)

    patch = patch_chunk(
        relay,
        location,
        project_key,
        SECOND_CHUNK,
        int(response.headers["Upload-Offset"]),
    )
    assert patch.status_code == 204, patch.text
    assert patch.headers["Upload-Offset"] == str(total)

    response = head_upload(relay, location, project_key)
    assert response.status_code == 204, response.text
    assert response.headers["Upload-Offset"] == str(total)
    assert response.headers["Upload-Length"] == str(total)


@pytest.mark.parametrize(
    "resumable_feature,defer_length",
    [
        pytest.param(True, True, id="feature_on_deferred_length"),
        pytest.param(False, False, id="feature_off_known_length"),
        pytest.param(False, True, id="feature_off_deferred_length"),
    ],
)
def test_head_oneshot(
    mini_sentry, relay_with_processing, project_config, resumable_feature, defer_length
):
    project_id = 42
    if not resumable_feature:
        project_config["features"].remove("projects:resumable-uploads")
    project_key = mini_sentry.get_dsn_public_key(project_id)
    relay = relay_with_processing()

    data = b"oneshot payload"
    headers = {"Tus-Resumable": "1.0.0"}
    if defer_length:
        headers["Upload-Defer-Length"] = "1"
    else:
        headers["Upload-Length"] = str(len(data))

    create = relay.post(
        f"/api/{project_id}/upload/?sentry_key={project_key}", headers=headers
    )
    assert create.status_code == 201, create.text
    _, params = location_parts(create.headers["Location"])
    assert "upload_id" not in params

    response = head_upload(relay, create.headers["Location"], project_key)

    assert response.status_code == 204, response.text
    assert response.headers["Tus-Resumable"] == "1.0.0"
    assert response.headers["Upload-Offset"] == "0"
    if defer_length:
        assert "Upload-Length" not in response.headers
        assert response.headers["Upload-Defer-Length"] == "1"
    else:
        assert response.headers["Upload-Length"] == str(len(data))
        assert "Upload-Defer-Length" not in response.headers

    patch = patch_chunk(relay, create.headers["Location"], project_key, data, 0)
    assert patch.status_code == 204, patch.text
    _, params = location_parts(patch.headers["Location"])
    assert params["upload_length"] == str(len(data))

    response = head_upload(relay, patch.headers["Location"], project_key)

    assert response.status_code == 204, response.text
    assert response.headers["Upload-Offset"] == str(len(data))
    assert response.headers["Upload-Length"] == str(len(data))
    assert "Upload-Defer-Length" not in response.headers


def test_head_session_unavailable(mini_sentry, relay_with_processing, project_config):
    project_id = 42
    project_key = mini_sentry.get_dsn_public_key(project_id)

    @mini_sentry.app.route(
        "/v1/objects/attachments/<scope>/<key>", methods=["PUT", "HEAD"]
    )
    def objectstore(scope, key):
        if request.headers.get("Upload-Offset") == "*" or request.method == "HEAD":
            return Response("", status=404)
        return {"key": key, "session": "c2Vzc2lvbg", "granularity": 0}

    relay = relay_with_processing(
        options={"processing": {"objectstore": {"objectstore_url": mini_sentry.url}}}
    )

    create = relay.post(
        f"/api/{project_id}/upload/?sentry_key={project_key}",
        headers={"Tus-Resumable": "1.0.0", "Upload-Length": "11"},
    )
    assert create.status_code == 201, create.text

    response = head_upload(relay, create.headers["Location"], project_key)

    assert response.status_code == 404
    assert response.headers["Tus-Resumable"] == "1.0.0"
    assert "Upload-Offset" not in response.headers


@dataclass(frozen=True, kw_only=True)
class EncodingRequestCase:
    id: str
    location: str
    max_upload_size: int


@pytest.mark.parametrize("encoding", ["unknown", "x-gzip", "zstd, gzip"])
@pytest.mark.parametrize(
    "case",
    [
        EncodingRequestCase(
            id="valid-request",
            location=f"{DUMMY_UPLOAD_LOCATION}&sentry_key={{project_key}}",
            max_upload_size=1024,
        ),
        EncodingRequestCase(
            id="invalid-request",
            location="/api/not-a-project/upload/key/",
            max_upload_size=1,
        ),
    ],
    ids=lambda case: case.id,
)
def test_unsupported_upload_encoding_contract(
    mini_sentry, relay, project_config, encoding, case: EncodingRequestCase
):
    edge = relay(
        mini_sentry,
        options={"limits": {"max_upload_size": case.max_upload_size}},
    )
    # Encoding rejection must precede auth, path extraction, and the body limit.
    response = edge.patch(
        case.location.format(project_key=mini_sentry.get_dsn_public_key(42)),
        headers={
            "Tus-Resumable": "1.0.0",
            "Content-Type": "application/offset+octet-stream",
            "Upload-Offset": "0",
            "X-Decoded-Content-Length": "11",
            "Content-Encoding": encoding,
            "Origin": "https://example.com",
        },
        data=b"hello world",
    )
    assert response.status_code == 415
    assert response.headers["Accept-Encoding"] == "zstd,gzip,deflate,br"
    assert response.content == b""
    assert "Tus-Resumable" not in response.headers
    assert "Access-Control-Allow-Origin" not in response.headers


@pytest.mark.parametrize("encoding", ["zstd", "gzip", "deflate"])
@pytest.mark.parametrize("chunked", [False, True])
@pytest.mark.parametrize("processing", [False, True])
def test_compressed_upload_passthrough(
    mini_sentry,
    relay,
    processing_config,
    project_config,
    events_consumer,
    encoding,
    chunked,
    processing,
):
    """Forwarding preserves the encoded bytes even when they exceed the decoded length."""
    mini_sentry.allow_chunked = True
    data = b"hello world"
    compressed = compress_upload(data, encoding)
    assert len(compressed) > len(data)
    received = []

    @mini_sentry.app.route("/api/<project>/upload/<key>/", methods=["PATCH"])
    def upload(**opts):
        received.append((dict(request.headers), request.get_data()))
        return Response(
            "",
            status=204,
            headers={
                "Location": DUMMY_UPLOAD_LOCATION,
                "Upload-Offset": str(len(data)),
            },
        )

    # Neither hop's configured output encoding may replace the incoming encoding.
    options = {
        "http": {"encoding": "zstd"},
        "limits": {"max_upload_size": len(data)},
    }
    if processing:
        # Processing alone must not trigger decoding: this Relay still forwards uploads.
        options["processing"] = {"objectstore": {"objectstore_url": None}}
        options = processing_config(options)
        receive_event = events_consumer().get_event
    else:
        receive_event = mini_sentry.get_captured_envelope
    upstream = relay(mini_sentry, options=options)
    edge = relay(
        upstream,
        options={
            "http": {"encoding": "gzip"},
            "limits": {"max_upload_size": len(data)},
        },
    )
    # An accepted event confirms that both hops have loaded global/project config.
    edge.send_event(42)
    receive_event()

    response = edge.patch(
        f"{DUMMY_UPLOAD_LOCATION}&sentry_key={mini_sentry.get_dsn_public_key(42)}",
        headers={
            "Tus-Resumable": "1.0.0",
            "Content-Type": "application/offset+octet-stream",
            "Content-Encoding": encoding,
            "X-Decoded-Content-Length": str(len(data)),
            "Upload-Offset": "0",
        },
        data=iter([compressed[:5], compressed[5:]]) if chunked else compressed,
    )
    assert response.status_code == 204, response.text
    assert response.headers["Upload-Offset"] == str(len(data))
    assert len(received) == 1
    headers, body = received[0]
    assert body == compressed
    assert headers["Content-Encoding"] == encoding
    assert headers["X-Decoded-Content-Length"] == str(len(data))
    assert headers["Upload-Offset"] == "0"


def compress_upload(data, encoding):
    import gzip
    import zlib
    import zstandard

    return {
        "zstd": zstandard.ZstdCompressor(write_checksum=True).compress,
        "gzip": gzip.compress,
        "deflate": zlib.compress,
    }[encoding](data)


@dataclass(frozen=True, kw_only=True)
class UploadCase:
    id: str
    upload_features: list[str]
    creation_headers: dict[str, str]
    chunks: list[tuple[bytes, int]]


@pytest.mark.parametrize("encoding", ["zstd", "gzip", "deflate"])
@pytest.mark.parametrize(
    "case",
    [
        UploadCase(
            id="resumable",
            upload_features=["projects:resumable-uploads"],
            creation_headers={"Upload-Length": str(len(RESUMABLE_DATA))},
            chunks=[
                (FIRST_CHUNK, len(FIRST_CHUNK)),
                (SECOND_CHUNK, len(RESUMABLE_DATA)),
            ],
        ),
        UploadCase(
            id="oneshot",
            upload_features=[],
            creation_headers={"Upload-Length": str(len(RESUMABLE_DATA))},
            chunks=[(RESUMABLE_DATA, len(RESUMABLE_DATA))],
        ),
        UploadCase(
            id="deferred",
            upload_features=["projects:resumable-uploads"],
            creation_headers={"Upload-Defer-Length": "1"},
            chunks=[(RESUMABLE_DATA, len(RESUMABLE_DATA))],
        ),
    ],
    ids=lambda case: case.id,
)
def test_compressed_upload_processing_chain(
    mini_sentry,
    relay,
    relay_with_processing,
    project_config,
    events_consumer,
    attachments_consumer,
    outcomes_consumer,
    objectstore,
    encoding,
    case: UploadCase,
):
    """Forwarding, decoded offsets, stored content, and attachment-byte accounting."""
    project_key = mini_sentry.get_dsn_public_key(42)
    project_config["quotas"] = [
        {
            "id": uuid.uuid4().hex,
            "categories": ["attachment"],
            "window": 3600,
            "limit": len(RESUMABLE_DATA),
            "reasonCode": "compressed_upload_bytes",
        }
    ]
    project_config["features"] = [
        "projects:relay-minidump-uploads",
        *case.upload_features,
    ]
    edge = relay(relay(relay_with_processing()))
    consumer = events_consumer()
    attachments = attachments_consumer()
    outcomes = outcomes_consumer()
    edge.send_event(42)
    consumer.get_event()

    create = edge.post(
        f"/api/42/upload/?sentry_key={project_key}",
        headers={"Tus-Resumable": "1.0.0", **case.creation_headers},
    )
    assert create.status_code == 201, create.text
    location = create.headers["Location"]
    offset = 0
    for chunk, expected_offset in case.chunks:
        response = patch_chunk(
            edge,
            location,
            project_key,
            compress_upload(chunk, encoding),
            offset,
            headers={
                "Content-Encoding": encoding,
                "X-Decoded-Content-Length": str(len(chunk)),
            },
        )
        assert response.status_code == 204, response.text
        assert response.headers["Upload-Offset"] == str(expected_offset)
        offset = expected_offset
        location = response.headers["Location"]

    key = urlparse(location).path.rstrip("/").split("/")[-1]
    assert objectstore("attachments", 42).get(key).payload.read() == RESUMABLE_DATA
    progress = head_upload(edge, location, project_key)
    assert progress.status_code == 204, progress.text
    assert progress.headers["Upload-Offset"] == str(len(RESUMABLE_DATA))

    # The signed location must validate against the decoded attachment length.
    envelope = Envelope(headers={"event_id": uuid.uuid4().hex})
    envelope.add_item(make_ref_item(location, len(RESUMABLE_DATA)))
    edge.send_envelope(42, envelope)
    attachment = attachments.get_individual_attachment()["attachment"]
    assert attachment["size"] == len(RESUMABLE_DATA)

    # One decoded attachment exactly fills the byte quota, regardless of wire size.
    edge.send_envelope(42, envelope)
    outcomes.assert_rate_limited(
        "compressed_upload_bytes",
        categories={"attachment": len(RESUMABLE_DATA), "attachment_item": 1},
    )


@dataclass(frozen=True, kw_only=True)
class InvalidUploadCase:
    id: str
    data: bytes


@pytest.mark.parametrize(
    "case",
    [
        InvalidUploadCase(id="malformed", data=b"not a zstd stream"),
        InvalidUploadCase(
            id="truncated", data=compress_upload(b"hello world", "zstd")[:8]
        ),
        InvalidUploadCase(id="short", data=compress_upload(b"hello", "zstd")),
        InvalidUploadCase(id="long", data=compress_upload(b"hello world!", "zstd")),
    ],
    ids=lambda case: case.id,
)
def test_invalid_compressed_upload_processing_chain(
    mini_sentry,
    relay,
    relay_with_processing,
    project_config,
    events_consumer,
    case: InvalidUploadCase,
):
    mini_sentry.fail_on_relay_error = False
    project_key = mini_sentry.get_dsn_public_key(42)
    edge = relay(relay(relay_with_processing()))
    consumer = events_consumer()
    edge.send_event(42)
    consumer.get_event()
    create = edge.post(
        f"/api/42/upload/?sentry_key={project_key}",
        headers={"Tus-Resumable": "1.0.0", "Upload-Length": "11"},
    )
    assert create.status_code == 201, create.text
    response = patch_chunk(
        edge,
        create.headers["Location"],
        project_key,
        case.data,
        0,
        headers={"Content-Encoding": "zstd", "X-Decoded-Content-Length": "11"},
    )
    assert response.status_code == 400, response.text
