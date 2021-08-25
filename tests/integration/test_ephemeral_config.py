import json
import pytest
import subprocess

from .fixtures.relay import Relay


@pytest.mark.parametrize("mode", ["managed", "proxy"])
@pytest.mark.parametrize("config_method", ["arg", "env"])
def test_store_via_ephemeral_relay(
    tmpdir,
    mini_sentry,
    get_relay_binary,
    background_process,
    random_port,
    mode,
    config_method,
):
    project_id = 42
    version = "latest"
    project_config = mini_sentry.add_basic_project_config(project_id)

    relay_bin = get_relay_binary()
    port = random_port()
    upstream = mini_sentry.url
    public_key = ""
    secret_key = ""
    relay_id = ""
    if mode == "managed":
        # TODO(michal): should not need config dir
        credentials = json.loads(
            subprocess.check_output(
                relay_bin + ["credentials", "generate", "--stdout"],
                universal_newlines=True,
                cwd=str(tmpdir),
            )
        )
        public_key = credentials["public_key"]
        secret_key = credentials["secret_key"]
        relay_id = credentials["id"]
        mini_sentry.known_relays[relay_id] = {
            "publicKey": public_key,
            "internal": False,
            "version": version,
        }

    args = relay_bin + ["run"]
    env = {}
    if config_method == "arg":
        args += [
            "--no-processing",
            "--upstream={}".format(upstream),
            "--port={}".format(port),
            "--mode={}".format(mode),
        ]
        if mode == "managed":
            args += [
                "--id={}".format(relay_id),
                "--public-key={}".format(public_key),
                "--secret-key={}".format(secret_key),
            ]
    if config_method == "env":
        env["RELAY_PROCESSING_ENABLED"] = "false"
        env["RELAY_UPSTREAM_URL"] = upstream
        env["RELAY_PORT"] = str(port)
        env["RELAY_MODE"] = mode
        if mode == "managed":
            env["RELAY_ID"] = relay_id
            env["RELAY_PUBLIC_KEY"] = public_key
            env["RELAY_SECRET_KEY"] = secret_key
    process = background_process(args, env=env, cwd=str(tmpdir))

    relay = Relay(
        ("127.0.0.1", port),
        process,
        mini_sentry,
        public_key,
        secret_key,
        relay_id,
        None,
        {},
        version,
    )
    relay.wait_relay_healthcheck()
    print(mode)
    if mode == "managed":
        project_config["config"]["trustedRelays"] = list(relay.iter_public_keys())
        print(project_config["config"]["trustedRelays"])

    relay.send_event(project_id)
    event = mini_sentry.captured_events.get(timeout=1).get_event()
    assert event["logentry"]["formatted"] == "Hello, World!"
