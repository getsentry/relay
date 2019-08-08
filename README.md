<p align="center">
  <a href="https://sentry.io" target="_blank" align="center">
    <img src="https://sentry-brand.storage.googleapis.com/sentry-logo-black.png" width="280">
  </a>
  <br />
</p>

# Semaphore - Official Sentry Relay

[![Travis](https://travis-ci.com/getsentry/semaphore.svg?branch=master)](https://travis-ci.com/getsentry/semaphore)
[![AppVeyor](https://img.shields.io/appveyor/ci/sentry/sentry-agent.svg)](https://ci.appveyor.com/project/sentry/sentry-agent)
[![codecov without integration tests](https://codecov.io/gh/getsentry/semaphore/branch/master/graph/badge.svg)](https://codecov.io/gh/getsentry/semaphore)
[![GitHub release](https://img.shields.io/github/release/getsentry/semaphore.svg)](https://github.com/getsentry/semaphore/releases/latest)
[![PyPI](https://img.shields.io/pypi/v/semaphore.svg)](https://pypi.python.org/pypi/Semaphore)
[![license](https://img.shields.io/github/license/getsentry/semaphore.svg)](https://github.com/getsentry/semaphore/blob/master/LICENSE)

<p align="center">
  <p align="center">
    <img src="https://github.com/getsentry/semaphore/blob/master/artwork/semaphore.jpg?raw=true" alt="semaphore tower" width="480">
  </p>
</p>

The Sentry Relay (aka Semaphore) is a work in progress service that pushes some
functionality from the Sentry SDKs as well as the Sentry server into a proxy process.

## Quickstart

Semaphore needs a relay ready sentry installation to connect to. It stores all
of its settings in a `.semaphore` folder in the current working directory by
default. The initial config can be created through a wizard:

    semaphore config init

This will guide you through the setup experience and create a `config.yml` and
`credentials.json` file in the `.semaphore` folder.

To run semaphore in the foreground and connect to the upstream sentry installation
`run` can be used:

    semaphore run

To see the entire config (including defaults) the following command can be used:

    semaphore config show

To change the location of the config folder the `--config` parameter can be passed
to the `semaphore` command with the path to an alternative location.

## Upstream Registration

When the semaphore runs it registers itself with the upstream configured sentry
instance as a relay. Right now relays can only directly connect to Sentry as
they do not yet proxy through non store requests. Each relay is identified by
the `(relay_id, public_key)` tuple upstream. Multiple relays can share the same
public key if they run with different relay IDs.

At present Sentry requires the relays to be explicitly whitelisted by their public
key. This is done through the `SENTRY_RELAY_WHITELIST_PK` config key which is
a list of permitted public keys.

## Metrics and Crash Reporting

By default the relay currently reports directly to sentry.io. This can be disabled
by setting the `sentry.enabled` key to `false`. Additionally a different DSN can
be supplied with `sentry.dsn`. Crash reporting will become opt-in before the initial
public release.

Stats can be submitted to a statsd server by configuring `metrics.statsd` key. It
can be put to a `ip:port` tuple. Additionally `metrics.prefix` can be configured
to have a different prefix (the default is `sentry.relay`). This prefix is added
in front of all metrics.

## License

Semaphore is licensed under the MIT license.

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

### Release management

We use [craft](https://github.com/getsentry/semaphore) to release new versions.
