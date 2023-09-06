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
