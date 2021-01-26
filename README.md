<p align="center">
  <a href="https://sentry.io" target="_blank" align="center">
    <img src="https://sentry-brand.storage.googleapis.com/sentry-logo-black.png" width="280">
  </a>
  <br />
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

Like Sentry, Relay is licensed under the BSL. See the `LICENSE` file and [this
forum post](https://forum.sentry.io/t/re-licensing-sentry-faq-discussion/8044)
for more information.

## Development

To build Relay, we require the **latest stable Rust**. The crate is split into a
workspace with multiple features, so when running building or running tests
always make sure to pass the `--all` and `--all-features` flags.
The `processing` feature additionally requires a C compiler and CMake.

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

For a lot of tests you will need Redis and Kafka running in their respective
default configuration. `sentry devservices` from
[sentry](https://github.com/getsentry/sentry) does this for you.

### Building and Running

The easiest way to rebuild and run Relay is using `cargo`. Depending on the
configuration, you may need to have a local instance of Sentry running.

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

- **`ssl`**: Enables SSL support in the Server.

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

### Development Server

If you have `systemfd` and `cargo-watch` installed, the `make devserver` command
can auto-reload Relay:

```bash
cargo install systemfd cargo-watch
make devserver
```

### SSL

The repository contains a SSL-certificate + private key for development
purposes. It comes in two formats: Once as a `(.pem, .cert)`-pair, once as
`.pfx` (PKCS #12) file.

The password for the `.pfx` file is `password`.

### Release Management

We use [craft](https://github.com/getsentry/craft) to release new versions.
There are two separate projects to publish:

- **Relay binary** is released from the root folder. Run `craft prepare` and
  `craft publish` in that directory to create a release build and publish it,
  respectively. We use [Calendar Versioning](https://calver.org/) and coordinate
  releases with Sentry.

- **Relay Python library** along with the C-ABI are released from the `py/`
  subfolder. Change into that directory and run `craft prepare` and `craft publish`. We use [Semantic Versioning](https://semver.org/) and release during
  the development cycle.
