import os
import shutil

import pytest

from . import SentryLike


HAPROXY_BIN = [os.environ.get("HAPROXY_BIN") or "haproxy"]


class HAProxy(SentryLike):
    def __init__(self, server_address, process, upstream):
        super().__init__(server_address, upstream)
        self.process = process


@pytest.fixture
def haproxy(background_process, random_port, config_dir):
    def inner(*upstreams):
        if shutil.which(HAPROXY_BIN[0]) is None:
            pytest.skip("HAProxy not installed")

        host = "127.0.0.1"
        port = random_port()

        config = config_dir("haproxy").join("config")

        config_lines = [
            "defaults",
            "    mode http",
            "    timeout connect 25000ms",
            "    timeout client 25000ms",
            "    timeout server 25000ms",
            "    timeout queue 25000ms",
            "    timeout http-request 25000ms",
            "    timeout http-keep-alive 25000ms",
            "    option forwardfor",
            "    option redispatch",
            "frontend defaultFront",
            f"    bind {host}:{port}",
            "    default_backend defaultBack",
            "backend defaultBack",
            "    balance roundrobin",
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
