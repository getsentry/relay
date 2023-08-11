<p align="center">
  <a href="https://sentry.io/?utm_source=github&utm_medium=logo" target="_blank">
    <picture>
      <source srcset="https://sentry-brand.storage.googleapis.com/sentry-logo-white.png" media="(prefers-color-scheme: dark)" />
      <source srcset="https://sentry-brand.storage.googleapis.com/sentry-logo-black.png" media="(prefers-color-scheme: light), (prefers-color-scheme: no-preference)" />
      <img src="https://sentry-brand.storage.googleapis.com/sentry-logo-black.png" alt="Sentry" width="280">
    </picture>
  </a>
</p>

# Official Sentry Relay

[![CI](https://github.com/getsentry/relay/workflows/CI/badge.svg?branch=master)](https://github.com/getsentry/relay/actions?query=workflow%3ACI+branch%3Amaster)
[![GitHub Release](https://img.shields.io/github/release/getsentry/relay.svg)](https://github.com/getsentry/relay/releases/latest)
[![PyPI](https://img.shields.io/pypi/v/sentry-relay.svg)](https://pypi.python.org/pypi/sentry-relay)

<p align="center">
  <p align="center">
    <img src="https://github.com/getsentry/relay/blob/master/artwork/relay-logo.png?raw=true" alt="Relay" width="480">
  </p>
</p>

The Sentry Relay is a service that pushes some functionality from the Sentry
SDKs as well as the Sentry server into a proxy process.

## Documentation

- Product documentation can be found at: [https://docs.sentry.io/meta/relay/](https://docs.sentry.io/meta/relay).
- Code and development documentation can be found at:
  [https://getsentry.github.io/relay/](https://getsentry.github.io/relay/).

## License

Like Sentry, Relay is licensed under the BUSL. See the `LICENSE` file and [this
forum post](https://forum.sentry.io/t/re-licensing-sentry-faq-discussion/8044)
for more information.

## Development

To build Relay, we require the **latest stable Rust**. The crate is split into a
workspace with multiple features, so when running building or running tests
always make sure to pass the `--all` and `--all-features` flags.
The `processing` feature additionally requires a C compiler and CMake.

To install the development environment, librdkafka must be installed and on the
path. On macOS, we require to install it with `brew install librdkafka`, as the installation script uses `brew --prefix` to determine the correct location.

We use VSCode for development. This repository contains settings files
configuring code style, linters, and useful features. When opening the project
for the first time, make sure to _install the Recommended Extensions_, as they
will allow editor assist during coding.

The root of the repository contains a `Makefile` with useful commands for
development:

- `make check`: Runs code formatting checks and linters. This is useful before
  opening a pull request.
- `make test`: Runs unit tests, integration tests and Python package tests (see
  below for more information).
- `make all`: Runs all checks and tests. This runs most of the tasks that are
  also performed in CI.
- `make clean`: Removes all build artifacts, the virtualenv and cached files.

For more avalibale make targets, please, run `make help`.

Integration tests require Redis and Kafka running in their default
configuration. The most convenient way to get all required services is via
[`sentry devservices`](https://develop.sentry.dev/services/devservices/), which
requires an up-to-date Sentry development environment.

### Building and Running

The easiest way to rebuild and run Relay for development is using `cargo`.
Depending on the configuration, you may need to have a local instance of Sentry
running.

```bash
# Initialize Relay for the first time
cargo run --all-features -- config init

# Rebuild and run with all features
cargo run --all-features -- run
```

The standard build commands are also available as `make` targets. Note that
release builds still generate debug information.

```bash
# Build without optimizations in debug mode.
make build

# Build with release optimizations and debug information.
make release
```

For quickly verifying that Relay compiles after making some changes, you can
also use `cargo check`:

```bash
cargo check --all --all-features
```

### Features

By default, Relay compiles without _processing_ mode. This is the configuration
used for Relays operating as proxys. There are two optional features:

- **`processing`**: Enables event processing and ingestion functionality. This
  allows to enable processing in config. When enabled, Relay will produce events
  into a Kafka topic instead of forwarding to the configured upstream. Also, it
  will perform full event normalization, filtering, and rate limiting.

- **`crash-handler`**: Allows native crash reporting for segfaults and
  out-of-memory situations when internal error reporting to Sentry is enabled.

To enable a feature, pass it to the cargo invocation. For example, to run tests
across all workspace crates with the `processing` feature enabled, run:

```bash
cargo run --features=processing
```

### Tests

The test suite comprises unit tests, an integration test suite and a separate
test suite for the Python package. Unit tests are implemented as part of the
Rust crates and can be run via:

```bash
# Tests for default features
make test-rust

# Run Rust tests for all features
make test-rust-all
```

The integration test suite requires `python`. By default, the integration test
suite will create a virtualenv, build the Relay binary with processing enabled,
and run a set of integration tests:

```bash
# Create a new virtualenv, build Relay and run integration tests
make test-integration

# Build and run a single test manually
make build
.venv/bin/pytest tests/integration -k <test_name>
```

#### Snapshot tests

We use `insta` for snapshot testing. It will run as part of the `make test` command 
to validate schema/protocol changes. To install the `insta` tool for reviewing snapshots run:
```bash
cargo install cargo-insta
```

After that you'll be able to review and automatically update snapshot files by running:
```bash
cargo insta review
```

Make sure to run the command if you've made any changed to the event schema/protocol.
For more information see https://insta.rs/docs/.

### Linting

We use `rustfmt` and `clippy` from the latest stable channel for code formatting
and linting. To make sure that these tools are set up correctly and running with
the right configuration, use the following make targets:

```bash
# Format the entire codebase
make format

# Run clippy on the entire codebase
make lint
```

### Python and C-ABI

Potentially, new functionality also needs to be added to the Python package.
This first requires to expose new functions in the C ABI. For this, refer to the
[Relay C-ABI readme](https://getsentry.github.io/relay/relay_cabi/).

We highly recommend to develop and test the python package in a **virtual
environment**. Once the ABI has been updated and tested, ensure the virtualenv
is active and install the package, which builds the native library. There are
two ways to install this:

```bash
# Install the release build, recommended:
pip install --editable ./py

# Install the debug build, faster installation but much slower runtime:
RELAY_DEBUG=1 pip install --editable ./py
```

For testing, we use ubiquitous `pytest`. Again, ensure that your virtualenv is
active and the latest version of the native library has been installed. Then,
run:

```bash
# Create a new virtualenv, install the release build and run tests
make test-python

# Run a single test manually
.venv/bin/pytest py/tests -k <test_name>
```

### Usage with Sentry

To develop Relay with an existing Sentry devserver, self-hosted Sentry
installation, or Sentry SaaS, configure the upstream to the URL of the Sentry
server in `.relay/config.yml` in the project root directory. For example, in local development set
`relay.upstream` to `http://localhost:8000/`.

To test processing mode with a local development Sentry, use this configuration:

```yaml
relay:
  # Point to your Sentry devserver URL:
  upstream: http://localhost:8000/
  # Listen to a port other than 3000:
  port: 3001
logging:
  # Enable full logging and backraces:
  level: trace
  enable_backtraces: true
limits:
  # Speed up shutdown on ^C
  shutdown_timeout: 0
processing:
  # Enable processing mode with store normalization and post data to Kafka:
  enabled: true
  kafka_config:
    - { name: "bootstrap.servers", value: "127.0.0.1:9092" }
    - { name: "message.max.bytes", value: 2097176 }
  redis: "redis://127.0.0.1"
```

Note that the Sentry devserver also starts a Relay in processing mode on port
`3000` with similar configuration. That Relay does not interfere with your
development build. To ensure SDKs send to your development instance, update the
port in the DSN:

```
http://<key>@localhost:3001/<id>
```

### Release Management

We use GitHub actions to release new versions.
There are two separate projects to publish:

- **Relay binary** is automatically released using [Calendar Versioning](https://calver.org/) on a monthly basis together with `sentry` (see https://develop.sentry.dev/self-hosted/releases/), so there should be no reason to create a release manually. That said, manual releases are possible with the ["Release" action](https://github.com/getsentry/relay/actions/workflows/release_binary.yml). Make sure that [`CHANGELOG.md`](./CHANGELOG.md) is up-to-date before running the action.

- **Relay Python library** along with the C-ABI are released with the ["Release Library" action](https://github.com/getsentry/relay/actions/workflows/release_library.yml). Make sure that [`py/CHANGELOG.md`](./py/CHANGELOG.md) is up-to-date before running the action. Press "Run workflow" and choose a new version. We use [Semantic Versioning](https://semver.org/) and release during the development cycle.

### Instructions for changelogs

For changes exposed to the _Python package_, add an entry to `py/CHANGELOG.md`. This includes, but is not limited to, event normalization, PII scrubbing, and the protocol.
For changes to the _Relay server_, please add an entry to `CHANGELOG.md` under the following heading:

1. `Features`: for new user-visible functionality.
2. `Bug Fixes`: for user-visible bug fixes.
3. `Internal`: for features and bug fixes in internal operation, especially processing mode.

To the changelog entry, please add a link to this PR (consider a more descriptive message):

```md
- ${getCleanTitle()}. (${PR_LINK})
```

If none of the above apply, you can opt out by adding `#skip-changelog` to the PR description.
