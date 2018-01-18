# Sentry Agent

<a href="https://travis-ci.org/getsentry/sentry-agent"><img src="https://travis-ci.org/getsentry/sentry-agent.svg?branch=master" alt=""></a>

The Sentry Agent (aka Smith) is a work in progress service that pushes some
functionality from the Sentry SDKs as well as the Sentry server into a proxy process.

## License

Sentry Agent is licensed under the MIT license.

## Development

We're going to settle on using vscode for this project for now.  Because RLS is not
in the best state at the moment the config for vscode targets a specific nightly
build to get RLS support.  To use RLS please run this once:

    rustup toolchain add nightly-2018-01-10

Afterwards vscode will automatically sue this nightly toolchain for the development
tools.  For actually compiling use stable or beta (but we won't shoot you if you use
nightly as long as you don't use nightly features).
