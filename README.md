<p align="center">
  <a href="https://sentry.io" target="_blank" align="center">
    <img src="https://sentry-brand.storage.googleapis.com/sentry-logo-black.png" width="280">
  </a>
  <br />
</p>

# Semaphore - Official Sentry Relay

[![Travis](https://img.shields.io/travis/getsentry/semaphore.svg)](https://travis-ci.org/getsentry/semaphore)
[![AppVeyor](https://img.shields.io/appveyor/ci/sentry/sentry-agent.svg)](https://ci.appveyor.com/project/sentry/sentry-agent)
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

## Usage

Semaphore needs a relay ready sentry installation to connect to.  It stores all
of its settings in a `.semaphore` folder in the current working directory by
default.  The initial config can be created through a wizard:

    semaphore config init

This will guide you through the setup experience.

To run semaphore in the foreground and connect to the upstream sentry installation
`run` can be used:

    semaphore run

## License

Semaphore is licensed under the MIT license.

## Development

We're going to settle on using vscode for this project for now. We're targeting
stable rust at the moment and the repo is appropriately configured.

If you have `catflap` and `cargo-watch` installed, the `make devserver` command can auto-reload the
relay:

    $ cargo install catflap cargo-watch
    $ make devserver
