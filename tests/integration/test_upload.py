"""
Tests for the TUS upload endpoint (/api/{project_id}/upload/).
"""

import re
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed

from flask import Response
from objectstore_client.multipart import MultipartUpload
import pytest
import zstandard

from sentry_relay.auth import SecretKey

from .asserts import matches_any
from .consts import (
    DUMMY_UPLOAD_PATH,
    DUMMY_UPLOAD_LOCATION,
)

LOCATION_REGEX = re.compile(r"/api/\d+/upload/(\w+)/?.*upload_id=([\w-]+)")


@pytest.fixture
def project_config(mini_sentry):
    project_id = 42
    config = mini_sentry.add_full_project_config(project_id)["config"]
    config.setdefault("features", []).append("projects:relay-minidump-uploads")
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
        },
        data=data,
    )

    assert response.status_code == expected_status_code, response.text


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
        % (
            DUMMY_UPLOAD_LOCATION,
            mini_sentry.get_dsn_public_key(project_id),
        ),
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
        % (
            DUMMY_UPLOAD_LOCATION,
            mini_sentry.get_dsn_public_key(project_id),
        ),
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
    mini_sentry, relay, relay_with_processing, chain, project_config, events_consumer
):
    """Create and separate upload via processing relay stores the blob in objectstore."""
    project_id = 42
    project_key = mini_sentry.get_dsn_public_key(project_id)

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


def test_processing_invalid_length(
    mini_sentry, relay, relay_with_processing, project_config
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

    # Use the location to send a PATCH request that is too long
    data = 11 * b"X"
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


@pytest.mark.parametrize(
    "with_multipart",
    [pytest.param(False, id="no multipart"), pytest.param(True, id="with multipart")],
)
def test_objectstore_retries(
    mini_sentry, relay_with_processing, project_config, with_multipart
):
    project_id = 42
    project_key = mini_sentry.get_dsn_public_key(project_id)

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
    if with_multipart:
        location += "?upload_id=my_upload_id"
        sep = "&"
    signature = SecretKey.parse(relay.secret_key).sign(location.encode())
    signed_location = (
        f"{location}{sep}sentry_key={project_key}&upload_signature={signature}"
    )

    data = b"hello world"
    response = relay.patch(
        signed_location,
        headers={
            "Content-Length": str(len(data)),
            "Content-Type": "application/offset+octet-stream",
            "Tus-Resumable": "1.0.0",
            "Upload-Offset": "0",
        },
        data=data,
    )
    print(response.text)

    failure = mini_sentry.test_failures.get(timeout=10)
    expected_attempts = 1 if with_multipart else 3  # multipart cannot be retried
    assert (
        f"failed to upload 1 attachment(s) to objectstore in {expected_attempts} attempt(s)"
        in str(failure)
    )
    assert response.status_code == 500


def test_objectstore_timeout(mini_sentry, relay_with_processing, project_config):
    mini_sentry.allow_chunked = True
    mini_sentry.fail_on_relay_error = False
    project_id = 42
    project_key = mini_sentry.get_dsn_public_key(project_id)

    @mini_sentry.app.route(
        "/v1/objects:multipart/attachments/<scope>/<key>", methods=["PUT"]
    )
    def multipart_create(**params):
        return {"key": params["key"], "upload_id": "foo"}, 201

    @mini_sentry.app.route(
        "/v1/objects:multipart:parts/attachments/<scope>/<key>", methods=["PUT"]
    )
    def multipart_upload(**opts):
        time.sleep(2)
        return 204

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

    response = upload_something(relay, project_id, project_key)

    assert response.status_code == 504


def test_upload_offset(mini_sentry, relay_with_processing, project_config, objectstore):
    project_id = 42
    project_key = mini_sentry.get_dsn_public_key(project_id)
    relay = relay_with_processing()
    objectstore = objectstore("attachments", project_id)

    data = 3 * 1024 * 1024 * b"X"
    response = relay.post(
        f"/api/{project_id}/upload/?sentry_key={project_key}",
        headers={
            "Content-Length": "0",
            "Tus-Resumable": "1.0.0",
            "Upload-Length": str(len(data)),
        },
    )
    assert response.status_code == 201, response.json()

    # Upload only part of the bytes
    split = len(data) // 3
    data1 = data[:split]
    data2 = data[split:]

    response = relay.patch(
        f"{response.headers['Location']}&sentry_key={project_key}",
        headers={
            "Content-Length": str(len(data1)),
            "Content-Type": "application/offset+octet-stream",
            "Tus-Resumable": "1.0.0",
            "Upload-Offset": "0",
        },
        data=data1,
    )
    assert response.status_code == 204

    key, upload_id = LOCATION_REGEX.match(response.headers["Location"]).groups()
    (part1,) = MultipartUpload(objectstore, key, upload_id).list_parts()

    assert vars(part1) == {
        "part_number": 1,
        "etag": matches_any(),
        "last_modified": matches_any(),
        "size": len(zstandard.compress(data1)),
    }

    response = relay.patch(
        f"{response.headers['Location']}&sentry_key={project_key}",
        headers={
            "Content-Length": str(len(data2)),
            "Content-Type": "application/offset+octet-stream",
            "Tus-Resumable": "1.0.0",
            "Upload-Offset": str(len(data1)),
            # "Upload-Length": str(len(data)),  # TODO: can we even make this required?
        },
        data=data2,
    )
    assert response.status_code == 204

    key, upload_id = LOCATION_REGEX.match(response.headers["Location"]).groups()

    # TODO: remove upload_id

    assert objectstore.get(key).payload.read() == data


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
    config = mini_sentry.add_full_project_config(project_id)["config"]
    features = config.setdefault("features", [])
    if opted_in:
        features.append("projects:relay-minidump-uploads")

    relay = relay(
        mini_sentry,
        options={
            "outcomes": {
                "emit_outcomes": True,
                "batch_size": 1,
                "batch_interval": 1,
            }
        },
    )

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
            o["outcome"] == 3 and o["reason"] == "feature_disabled" for o in outcomes
        )
    else:
        assert mini_sentry.captured_outcomes.empty()


def upload_something(relay, project_id, project_key):
    data = b"hello world"
    response = relay.post(
        f"/api/{project_id}/upload/?sentry_key={project_key}",
        headers={
            "Content-Length": "0",
            "Tus-Resumable": "1.0.0",
            "Upload-Length": str(len(data)),
        },
    )
    assert response.status_code == 201, response.json()

    return relay.patch(
        f"{response.headers['Location']}&sentry_key={project_key}",
        headers={
            "Content-Length": str(len(data)),
            "Content-Type": "application/offset+octet-stream",
            "Tus-Resumable": "1.0.0",
            "Upload-Offset": "0",
        },
        data=data,
    )
