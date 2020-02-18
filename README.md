<p align="center">
  <a href="https://sentry.io" target="_blank" align="center">
    <img src="https://sentry-brand.storage.googleapis.com/sentry-logo-black.png" width="280">
  </a>
  <br />
</p>

# Official Sentry Relay

[![Travis](https://travis-ci.com/getsentry/relay.svg?branch=master)](https://travis-ci.com/getsentry/relay)
[![AppVeyor](https://img.shields.io/appveyor/ci/sentry/relay.svg)](https://ci.appveyor.com/project/sentry/relay)
[![GitHub release](https://img.shields.io/github/release/getsentry/relay.svg)](https://github.com/getsentry/relay/releases/latest)
[![PyPI](https://img.shields.io/pypi/v/sentry-relay.svg)](https://pypi.python.org/pypi/sentry-relay)

<p align="center">
  <p align="center">
    <img src="https://github.com/getsentry/relay/blob/master/artwork/semaphore.jpg?raw=true" alt="semaphore tower" width="480">
  </p>
</p>

The Sentry Relay is a work in progress service that pushes some functionality
from the Sentry SDKs as well as the Sentry server into a proxy process.

> **NOTE:** Relay version _0.5_ and newer can only be used with sentry.io
> (SaaS). Support will be shipped to on-premise in an upcoming release. To use
> Relay with your self-hosted Sentry instance, remain on version 0.4 for the
> time being.

## Documentation
The project documentation can be found at: [https://getsentry.github.io/relay/](https://getsentry.github.io/relay/).

## Quickstart

Relay needs Sentry 10 or later to connect to. It stores all of its settings in a
`.relay` folder in the current working directory by default. The initial config
can be created through a wizard:

    relay config init

This will guide you through the setup experience and create a `config.yml` and
`credentials.json` file in the `.relay` folder.

To run Relay in the foreground and connect to the upstream sentry installation
`run` can be used:

    relay run

To see the entire config (including defaults) the following command can be used:

    relay config show

To change the location of the config folder the `--config` parameter can be
passed to the `relay` command with the path to an alternative location.

## Upstream Registration

When Relay runs, it registers with the upstream configured Sentry instance. Each
Relay is identified by the `(relay_id, public_key)` tuple upstream. Multiple
Relays can share the same public key if they run with different Relay IDs.

At present, Sentry requires Relays to be explicitly whitelisted by their public
key. This is done through the `SENTRY_RELAY_WHITELIST_PK` config key which is a
list of permitted public keys.

## Metrics and Crash Reporting

By default, Relay currently reports directly to sentry.io. This can be disabled
by setting the `sentry.enabled` key to `false`. Additionally a different DSN can
be supplied with `sentry.dsn`. Crash reporting will become opt-in before the
initial public release.

Stats can be submitted to a statsd server by configuring `metrics.statsd` key.
It can be put to a `ip:port` tuple. Additionally `metrics.prefix` can be
configured to have a different prefix (the default is `sentry.relay`). This
prefix is added in front of all metrics.

## License

Like Sentry, Relay is licensed under the BSL. See the `LICENSE` file and [this
forum post](https://forum.sentry.io/t/re-licensing-sentry-faq-discussion/8044)
for more information.

## Development

We're going to settle on using vscode for this project for now. We're targeting
stable rust at the moment and the repo is appropriately configured.

If you have `systemfd` and `cargo-watch` installed, the `make devserver` command can auto-reload the
relay:

    $ cargo install systemfd cargo-watch
    $ make devserver


### SSL

The repository contains a SSL-certificate + private key for development
purposes. It comes in two formats: Once as a `(.pem, .cert)`-pair, once as
`.pfx` (PKCS #12) file.

The password for the `.pfx` file is `password`.

### Running in Docker

Docker image for `relay` can be found at `us.gcr.io/sentryio/relay`.

For example, you can start the latest version of `relay` as follows:

```sh
docker run -v $(pwd)/configs/:/etc/relay/ us.gcr.io/sentryio/relay run --config /etc/relay
```

The command assumes that Relay's configuration (`config.yml` and
`credentials.json`) are stored in `./configs/` directory on the host machine.

### Release Management

We use [craft](https://github.com/getsentry/craft) to release new versions.
