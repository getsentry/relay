import socket


def test_metrics_prefix(mini_sentry, relay_with_processing, processing_config):
    mini_sentry.add_full_project_config(42)
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as statsd:
        statsd.bind(("127.0.0.1", 0))
        statsd.settimeout(30)
        host, port = statsd.getsockname()
        options = processing_config(
            {
                "metrics": {"statsd": f"{host}:{port}", "prefix": "relay"},
            }
        )
        options["processing"]["kafka_config"].append(
            {"name": "statistics.interval.ms", "value": "1000"}
        )
        relay = relay_with_processing(options=options)
        relay.send_event(42)

        metrics = {
            "arroyo.producer.librdkafka.message_count",
            "datadog.dogstatsd.client.metrics",
            "relay.processing.event.enqueued",
        }
        while metrics:
            for metric in statsd.recv(65535).decode().splitlines():
                name = metric.partition(":")[0]
                assert not name.startswith("relay.arroyo.")
                assert not name.startswith("relay.datadog.dogstatsd.client.")
                metrics.discard(name)


def test_arroyo_application_tag(mini_sentry, relay_with_processing, processing_config):
    mini_sentry.add_full_project_config(42)
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as statsd:
        statsd.bind(("127.0.0.1", 0))
        statsd.settimeout(30)
        host, port = statsd.getsockname()
        options = processing_config(
            {
                "metrics": {"statsd": f"{host}:{port}", "prefix": "relay"},
            }
        )
        options["processing"]["kafka_config"].append(
            {"name": "statistics.interval.ms", "value": "1000"}
        )
        relay = relay_with_processing(options=options)
        relay.send_event(42)

        metrics = {
            "arroyo.producer.librdkafka.message_count",
            "relay.processing.event.enqueued",
        }
        while metrics:
            for metric in statsd.recv(65535).decode().splitlines():
                name = metric.partition(":")[0]
                tags = metric.partition("|#")[2].split("|", 1)[0].split(",")
                if name == "arroyo.producer.librdkafka.message_count":
                    assert "application:relay" in tags
                elif name == "relay.processing.event.enqueued":
                    assert not any(tag.startswith("application:") for tag in tags)
                metrics.discard(name)
