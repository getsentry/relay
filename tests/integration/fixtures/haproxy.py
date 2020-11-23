import os
import shutil

import pytest

from . import SentryLike


HAPROXY_BIN = [os.environ.get("HAPROXY_BIN") or "haproxy"]


class HAProxy(SentryLike):
    def __init__(self, server_address, process, upstream):
        super(HAProxy, self).__init__(server_address, upstream)
        self.process = process


@pytest.fixture
def haproxy(background_process, random_port, config_dir):
    if shutil.which(HAPROXY_BIN[0]) is None:
        pytest.skip("HAProxy not installed")

    def inner(*upstreams):
        host = "127.0.0.1"
        port = random_port()

        config = config_dir("haproxy").join("config")

        config_lines = [
            f"defaults",
            f"    mode http",
            f"    timeout connect 25000ms",
            f"    timeout client 25000ms",
            f"    timeout server 25000ms",
            f"    timeout queue 25000ms",
            f"    timeout http-request 25000ms",
            f"    timeout http-keep-alive 25000ms",
            f"    option forwardfor",
            f"    option redispatch",
            f"frontend defaultFront",
            f"    bind {host}:{port}",
            f"    default_backend defaultBack",
            f"backend defaultBack",
            f"    balance roundrobin",
        ]

        for i, upstream in enumerate(upstreams):
            upstream_host, upstream_port = upstream.server_address
            config_lines.append(
                f"    server sentryUpstream{i} {upstream_host}:{upstream_port} no-check"
            )

        config.write("\n".join(config_lines))
        config.write("\n")

        process = background_process(HAPROXY_BIN + ["-f", str(config)])

        return HAProxy((host, port), process, upstreams[0])

    return inner
