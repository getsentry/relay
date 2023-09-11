# Relay Dashboard

The main purpose of this dashboard is to help to debug the current state of the Relay and help with the local development.

The dashboard is still in development and should be used only on your own risk. Running it in production can also cause some performance issues.

Right now you can:
* view the logs of the running Relay
* all the available statsd metrics will be exposed as graphs


# Development

Run Relay:

```sh
cargo run --all-features run
```

Run frontend server for development:

```sh
make web
```

# Release

Build the WASM app first in release mode:

```sh
make dashboard-release
```

or run inside of `relay-dashboard` folder:

```sh
trunk build --release --public-url /dashboard/
```

And then build the relay with `dashboard` feature enabled to pick up the built assets and embed them into the resulting binary:

```sh
cargo b --all-features --all-targets --workspace --release
```
