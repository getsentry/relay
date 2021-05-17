import json
import os
import signal
import subprocess

import pytest

from . import SentryLike

RELAY_BIN = [os.environ.get("RELAY_BIN") or "target/debug/relay"]

if os.environ.get("RELAY_AS_CARGO", "false") == "true":
    RELAY_BIN = ["cargo", "run", "--"]


class Relay(SentryLike):
    def __init__(
        self,
        server_address,
        process,
        upstream,
        public_key,
        secret_key,
        relay_id,
        config_dir,
        options,
    ):
        super(Relay, self).__init__(server_address, upstream, public_key)

        self.process = process
        self.relay_id = relay_id
        self.secret_key = secret_key
        self.config_dir = config_dir
        self.options = options

    def shutdown(self, sig=signal.SIGKILL):
        self.process.send_signal(sig)

        try:
            self.process.wait(19)
        except subprocess.TimeoutExpired:
            self.process.kill()
            raise


@pytest.fixture
def relay(mini_sentry, random_port, background_process, config_dir):
    def inner(
        upstream,
        options=None,
        prepare=None,
        external=None,
        wait_healthcheck=True,
        static_relays_config=None,
    ):
        host = "127.0.0.1"
        port = random_port()

        default_opts = {
            "relay": {
                "upstream": upstream.url,
                "host": host,
                "port": port,
                "tls_port": None,
                "tls_private_key": None,
                "tls_cert": None,
            },
            "sentry": {"dsn": mini_sentry.internal_error_dsn, "enabled": True},
            "limits": {"max_api_file_upload_size": "1MiB"},
            "cache": {"batch_interval": 0},
            "logging": {"level": "trace"},
            "http": {"timeout": 2},
            "processing": {"enabled": False, "kafka_config": [], "redis": ""},
        }

        if options is not None:
            for key in options:
                default_opts.setdefault(key, {}).update(options[key])

        dir = config_dir("relay")
        dir.join("config.yml").write(json.dumps(default_opts))

        if static_relays_config is not None:
            # since json is yml write it with json (so we don't add another dependency)
            dir.join("static_relays.yml").write(json.dumps(static_relays_config))

        output = subprocess.check_output(
            RELAY_BIN + ["-c", str(dir), "credentials", "generate"]
        )

        # now that we have generated a credentials file get the details
        with open(dir.join("credentials.json"), "r") as f:
            credentials = json.load(f)
        public_key = credentials.get("public_key")
        assert public_key is not None
        secret_key = credentials.get("secret_key")
        assert secret_key is not None
        relay_id = credentials.get("id")
        assert relay_id is not None

        if prepare is not None:
            prepare(dir)

        mini_sentry.known_relays[relay_id] = {
            "publicKey": public_key,
            "internal": not external,
        }

        process = background_process(RELAY_BIN + ["-c", str(dir), "run"])

        relay = Relay(
            (host, port),
            process,
            upstream,
            public_key,
            secret_key,
            relay_id,
            dir,
            default_opts,
        )

        if wait_healthcheck:
            relay.wait_relay_healthcheck()

        return relay

    return inner
