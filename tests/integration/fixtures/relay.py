import json
import os
import sys
import signal
import stat
import requests
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
        version,
    ):
        super(Relay, self).__init__(server_address, upstream, public_key)

        self.process = process
        self.relay_id = relay_id
        self.secret_key = secret_key
        self.config_dir = config_dir
        self.options = options
        self.version = version

    def shutdown(self, sig=signal.SIGKILL):
        self.process.send_signal(sig)

        try:
            self.process.wait(19)
        except subprocess.TimeoutExpired:
            self.process.kill()
            raise


@pytest.fixture
def get_relay_binary():
    def inner(version="latest"):
        if version == "latest":
            return RELAY_BIN

        if sys.platform == "linux" or sys.platform == "linux2":
            filename = "relay-Linux-x86_64"
        elif sys.platform == "darwin":
            filename = "relay-Darwin-x86_64"
        elif sys.platform == "win32":
            filename = "relay-Windows-x86_64.exe"

        download_path = f"target/relay_releases_cache/{filename}_{version}"

        if not os.path.exists(download_path):
            download_url = (
                f"https://github.com/getsentry/relay/releases/download/"
                f"{version}/{filename}"
            )

            headers = {}
            if "GITHUB_TOKEN" in os.environ:
                headers["Authorization"] = f"Bearer {os.environ['GITHUB_TOKEN']}"

            os.makedirs(os.path.dirname(download_path), exist_ok=True)

            with requests.get(download_url, headers=headers) as r:
                r.raise_for_status()

                with open(download_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)

        os.chmod(download_path, 0o700 | stat.S_IEXEC)

        return [download_path]

    return inner


@pytest.fixture
def relay(mini_sentry, random_port, background_process, config_dir, get_relay_binary):
    def inner(
        upstream,
        options=None,
        prepare=None,
        external=None,
        wait_healthcheck=True,
        static_relays=None,
        version="latest",
    ):
        relay_bin = get_relay_binary(version)
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

        if static_relays is not None:
            default_opts["auth"] = {"static_relays": static_relays}

        if options is not None:
            for key in options:
                default_opts.setdefault(key, {}).update(options[key])

        dir = config_dir("relay")
        dir.join("config.yml").write(json.dumps(default_opts))

        output = subprocess.check_output(
            relay_bin + ["-c", str(dir), "credentials", "generate"]
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
            "version": version,
        }

        process = background_process(relay_bin + ["-c", str(dir), "run"])

        relay = Relay(
            (host, port),
            process,
            upstream,
            public_key,
            secret_key,
            relay_id,
            dir,
            default_opts,
            version,
        )

        if wait_healthcheck:
            relay.wait_relay_healthcheck()

        return relay

    return inner
