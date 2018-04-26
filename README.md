# Sentry Relay

<a href="https://travis-ci.org/getsentry/sentry-relay"><img src="https://travis-ci.org/getsentry/sentry-relay.svg?branch=master" alt=""></a>

The Sentry Relay (aka Smith) is a work in progress service that pushes some
functionality from the Sentry SDKs as well as the Sentry server into a proxy process.

## License

Sentry Relay is licensed under the MIT license.

## Development

We're going to settle on using vscode for this project for now.  We're targeting
stable rust at the moment and the repo is appropriately configured.

If you have `catflap` installed the `make devserver` command can auto-reload the
relay:

    $ cargo install catflap
    $ make devserver
