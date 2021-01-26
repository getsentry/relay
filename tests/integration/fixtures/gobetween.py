import json
import os
import shutil

import pytest

from . import SentryLike

GOBETWEEN_BIN = [os.environ.get("GOBETWEEN_BIN") or "gobetween"]


class Gobetween(SentryLike):
    def __init__(self, server_address, process, upstream):
        super(Gobetween, self).__init__(server_address, upstream)
        self.process = process


@pytest.fixture
def gobetween(background_process, random_port, config_dir):
    def inner(*upstreams):
        if shutil.which(GOBETWEEN_BIN[0]) is None:
            pytest.skip("Gobetween not installed")

        host = "127.0.0.1"
        port = random_port()

        config = config_dir("gobetween").join("config.json")
        config.write(
            json.dumps(
                {
                    "logging": {"level": "debug", "output": "stdout"},
                    "api": {
                        "enabled": True,
                        "bind": f"{host}:{random_port()}",
                        "cors": False,
                    },
                    "defaults": {
                        "max_connections": 0,
                        "client_idle_timeout": "0",
                        "backend_idle_timeout": "0",
                        "backend_connection_timeout": "0",
                    },
                    "servers": {
                        "sample": {
                            "protocol": "tcp",
                            "bind": f"{host}:{port}",
                            "discovery": {
                                "kind": "static",
                                "static_list": [
                                    f"{u.server_address[0]}:{u.server_address[1]}"
                                    for u in upstreams
                                ],
                            },
                        }
                    },
                }
            )
        )

        process = background_process(
            GOBETWEEN_BIN + ["from-file", "-fjson", str(config)]
        )

        return Gobetween((host, port), process, upstreams[0])

    return inner
