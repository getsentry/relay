import socket


def test_arroyo_application_tag(relay_with_processing, processing_config):
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as statsd:
        statsd.bind(("127.0.0.1", 0))
        statsd.settimeout(30)
        host, port = statsd.getsockname()
        options = processing_config(
            {
                "processing": {"use_arroyo": True},
                "metrics": {"statsd": f"{host}:{port}"},
            }
        )
        options["processing"]["kafka_config"].append(
            {"name": "statistics.interval.ms", "value": "1000"}
        )
        relay_with_processing(options=options)

        seen_arroyo = False
        seen_relay = False
        while not (seen_arroyo and seen_relay):
            for metric in statsd.recv(65535).decode().splitlines():
                tags = metric.partition("|#")[2].split("|", 1)[0].split(",")
                if metric.startswith("sentry.relay.arroyo."):
                    assert "application:relay" in tags
                    seen_arroyo = True
                elif metric.startswith("sentry.relay.server.starting:"):
                    assert not any(tag.startswith("application:") for tag in tags)
                    seen_relay = True
