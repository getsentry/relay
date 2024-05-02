import pytest
import struct
import base64
import zstandard as zstd


from .test_metrics import TEST_CONFIG, metrics_by_name


def b64_set(*values):
    return (
        base64.b64encode(b"".join(struct.pack("<I", value) for value in values))
        .decode("ascii")
        .rstrip("=")
    )


def b64_dist(*values):
    return (
        base64.b64encode(b"".join(struct.pack("<d", value) for value in values))
        .decode("ascii")
        .rstrip("=")
    )


def assert_zstd_set(value, *expected):
    data = zstd_decompress(value.pop("data"))
    assert value == {"format": "zstd"}

    values = list(value[0] for value in struct.iter_unpack("<I", data))
    assert values == list(expected)


def assert_zstd_dist(value, *expected):
    data = zstd_decompress(value.pop("data"))
    assert value == {"format": "zstd"}

    values = list(value[0] for value in struct.iter_unpack("<d", data))
    assert values == list(expected)


def zstd_decompress(data):
    with zstd.ZstdDecompressor().stream_reader(
        base64.b64decode(data + "==")
    ) as decompressor:
        return decompressor.read()


@pytest.mark.parametrize("mode", [None, "legacy"])
def test_metric_bucket_encoding_legacy(
    mini_sentry, relay_with_processing, metrics_consumer, mode
):
    if mode is not None:
        mini_sentry.global_config["options"]["relay.metric-bucket-set-encodings"] = mode
        mini_sentry.global_config["options"][
            "relay.metric-bucket-distribution-encodings"
        ] = mode

    metrics_consumer = metrics_consumer()
    relay = relay_with_processing(options=TEST_CONFIG)

    project_id = 42
    mini_sentry.add_basic_project_config(project_id)

    relay.send_metrics(project_id, "transactions/foo:1337|d\ntransactions/bar:42|s")

    metrics = metrics_by_name(metrics_consumer, 2)
    assert metrics["d:transactions/foo@none"]["value"] == [1337.0]
    assert metrics["s:transactions/bar@none"]["value"] == [42.0]


@pytest.mark.parametrize("namespace", [None, "spans", "custom"])
@pytest.mark.parametrize("ty", ["set", "distribution"])
def test_metric_bucket_encoding_dynamic_global_config_option(
    mini_sentry, relay_with_processing, metrics_consumer, namespace, ty
):
    if namespace is not None:
        mini_sentry.global_config["options"][f"relay.metric-bucket-{ty}-encodings"] = {
            namespace: "array"
        }

    metrics_consumer = metrics_consumer()
    relay = relay_with_processing(options=TEST_CONFIG)

    project_id = 42
    project_config = mini_sentry.add_basic_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:custom-metrics",
        "projects:span-metrics-extraction",
    ]

    metrics_payload = (
        f"{namespace or 'custom'}/foo:1337|d\n{namespace or 'custom'}/bar:42|s"
    )
    relay.send_metrics(project_id, metrics_payload)

    metrics = metrics_by_name(metrics_consumer, 2)

    dname = f"d:{namespace or 'custom'}/foo@none"
    sname = f"s:{namespace or 'custom'}/bar@none"
    assert dname in metrics
    assert sname in metrics

    if namespace is not None and ty == "distribution":
        assert metrics[dname]["value"] == {"format": "array", "data": [1337.0]}
    else:
        assert metrics[dname]["value"] == [1337.0]

    if namespace is not None and ty == "set":
        assert metrics[sname]["value"] == {"format": "array", "data": [42.0]}
    else:
        assert metrics[sname]["value"] == [42.0]


def test_metric_bucket_encoding_base64(
    mini_sentry, relay_with_processing, metrics_consumer
):
    mini_sentry.global_config["options"].update(
        {
            "relay.metric-bucket-set-encodings": "base64",
            "relay.metric-bucket-distribution-encodings": "base64",
        }
    )

    metrics_consumer = metrics_consumer()
    relay = relay_with_processing(options=TEST_CONFIG)

    project_id = 42
    mini_sentry.add_basic_project_config(project_id)

    relay.send_metrics(project_id, "transactions/foo:3:1:2|d\ntransactions/bar:7:1|s")

    metrics = metrics_by_name(metrics_consumer, 2)
    assert metrics["d:transactions/foo@none"]["value"] == {
        "format": "base64",
        "data": b64_dist(3, 1, 2),  # values are in order
    }
    assert metrics["s:transactions/bar@none"]["value"] == {
        "format": "base64",
        "data": b64_set(1, 7),
    }


def test_metric_bucket_encoding_zstd(
    mini_sentry, relay_with_processing, metrics_consumer
):
    mini_sentry.global_config["options"].update(
        {
            "relay.metric-bucket-set-encodings": "zstd",
            "relay.metric-bucket-distribution-encodings": "zstd",
        }
    )

    metrics_consumer = metrics_consumer()
    relay = relay_with_processing(options=TEST_CONFIG)

    project_id = 42
    mini_sentry.add_basic_project_config(project_id)

    relay.send_metrics(project_id, "transactions/foo:3:1:2|d\ntransactions/bar:7:1|s")

    metrics = metrics_by_name(metrics_consumer, 2)
    assert_zstd_dist(
        metrics["d:transactions/foo@none"]["value"], 1, 2, 3
    )  # values are sorted
    assert_zstd_set(metrics["s:transactions/bar@none"]["value"], 1, 7)
