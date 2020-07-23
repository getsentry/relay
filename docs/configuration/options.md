# Configuration Options

The base configuration for Relay lives in the file `.relay/config.yml`. To change this location, pass the `--config` option to any Relay command:

```bash
❯ ./relay run --config /path/to/folder
```

All configuration keys are `snake_case`.

## Relay

General relay settings for Relay's operation.

### `relay.mode`

*String, default: `managed`*, possible values: `managed`, `static`, `proxy` and `capture`

Controls how Relay obtains the project configuration for events. For detailed explanation of the modes, see [Relay Modes].

### `relay.upstream`

*String, default: `https://sentry.io`*

Fully qualified URL of the upstream Relay or Sentry instance.

**Important**: Relay does not check for cycles. Ensure this option is not set
to an endpoint that will cause events to be cycled back here.

### `relay.host`

*String, default: `0.0.0.0` in Docker, otherwise `127.0.0.1`*

The host, the Relay should bind to (network interface).  Example: `0.0.0.0`

### `relay.port`

*Integer, default: `3000`*

The port to bind for the unencrypted Relay HTTP server.  Example: `3000`

### `relay.tls_port`

*Integer, optional*

Optional port to bind for the encrypted Relay HTTPS server.  Example: `3001`

This is in addition to the `port` option: If you set up a HTTPS server at
`tls_port`, the HTTP server at `port` still exists.

### `relay.tls_identity_path`

*String, optional*

The filesystem path to the identity (DER-encoded PKCS12) to use for the HTTPS
server. Relative paths are evaluated in the current working directory.  Example:
`relay_dev.pfx`

### `relay.tls_identity_password`

*String, optional*

Password for the PKCS12 archive in `relay.tls_identity_path`.

## HTTP

Set various network-related settings.

### `http.timeout`

*Integer, default: `5`*

Timeout for upstream requests in seconds.

### `http.max_retry_interval`

*Integer, default: `60`*

Maximum interval between failed request retries in seconds.

### `host.header`

string, default: `null`

The custom HTTP Host header to be sent to the upstream.

## Caching

Fine-tune caching of project state.

### `cache.project_expiry`

*Integer, default: `300` (5 minutes)*

The cache timeout for project configurations in seconds.  Irrelevant if you use
the "simple proxy mode", where your project config is stored in local files.

### `cache.project_grace_period`

*Integer, default: `0` (seconds)*

Number of seconds to continue using this project configuration after cache
expiry while a new state is being fetched. This is added on top of `cache.project_expiry`
and `cache.miss_expiry`.

### `cache.relay_expiry`

*Integer, default: `3600` (1 hour)*

The cache timeout for downstream Relay info (public keys) in seconds. This is
only relevant, if you plan to connect further Relays to this one.

### `cache.event_expiry`

*Integer, default: `600` (10 minutes)*

The cache timeout for events (store) before dropping them.

### `cache.miss_expiry`

*Integer, default: `60` (1 minute)*

The cache timeout for non-existing entries.

### `cache.batch_interval`

*Integer, default: `100` (100 milliseconds)*

The buffer timeout for batched queries before sending them upstream **in
milliseconds**.

### `cache.batch_size`
*Integer, default: `500`*

The maximum number of project configs to fetch from Sentry at once.

### `cache.file_interval`

*Integer, default: `10` (10 seconds)*

  Interval for watching local cache override files in seconds.

### `cache.event_buffer_size`

*Integer, default: `1000`*

The maximum number of events that are buffered in case of network issues or high
rates of incoming events.

### `cache.eviction_interval`

*Integer, default: `60` (seconds)*

Interval for evicting outdated project configs from memory.

## Size Limits

Controls various HTTP-related limits. All values are either integers or are
human-readable strings of a number and a human-readable unit, such as:

- `500B`
- `1kB` (*1,000* bytes)
- `1KB` or `1KiB` (*1,024* bytes)
- `1MB` (*1,000,000* bytes)
- `1MiB` (*1,048,576* bytes)

### `limits.max_concurrent_requests`

*Integer, default: `100`*

The maximum number of concurrent connections to the upstream. If supported by the upstream, Relay supports connection keepalive.

### `limits.max_concurrent_queries`

*Integer, default: `5`*

The maximum number of queries that can be sent concurrently from Relay to the
upstream before Relay starts buffering requests. Queries are all requests made
to the upstream for obtaining information and explicitly exclude event
submission.

The concurrency of queries is additionally constrained by `max_concurrent_requests`.

### `limits.max_event_size`

*String, default: `1MiB`*

The maximum payload size for events.

### `limits.max_attachment_size`

*String, default: `50MiB`*

The maximum size for each attachment.

### `limits.max_attachments_size`

*String, default: `50MiB`*

The maximum combined size for all attachments in an envelope or request.

### `limits.max_envelope_size`

*String, default: `50MiB`*

The maximum payload size for an entire envelopes. Individual limits still apply.

### `limits.max_session_count`

*Integer, default: `100`*

The maximum number of session items per envelope.

### `limits.max_api_payload_size`

*String, default: `20MiB`*

The maximum payload size for general API requests.

### `limits.max_api_file_upload_size`

*String, default: `40MiB`*

The maximum payload size for file uploads and chunks.

### `limits.max_api_chunk_upload_size`

*String, default: `100MiB`*

The maximum payload size for chunks

### `limits.max_thread_count`

*Integer, default: number of cpus*

The maximum number of threads to spawn for CPU and web worker, each.

The total number of threads spawned will roughly be `2 * limits.max_thread_count + N`, where `N` is a fixed set of management threads.

### `limits.query_timeout`

*Integer, default: `30` (seconds)*

The maximum number of seconds a query is allowed to take across retries.
Individual requests have lower timeouts.


### `limits.max_connection_rate`

*Integer, default:  `256`*

The maximum number of connections to Relay that can be created at once.


### `limits.max_pending_connections`

*Integer, default: `2048`*

The maximum number of pending connects to Relay. This corresponds to the backlog param of
`listen(2)` in POSIX.


### `limits.max_connections`

*Integer: default: `25_000`*

The maximum number of open incoming connections to Relay.

### `limits.shutdown_timeout`

*Integer, default:L `10` (seconds)*

The maximum number of seconds to wait for pending events after receiving a
shutdown signal.

## Logging

### `logging.level`

*String, default: `info`*

  The log level for the relay. One of:

  - `off`
  - `error`
  - `warn`
  - `info`
  - `debug`
  - `trace`

**Warning**: On `debug` and `trace` levels, Relay emits extremely verbose
messages which can have severe impact on application performance.

### `logging.log_failed_payloads`

*boolean, default: `false`*

Logs full event payloads of failed events to the log stream.

### `logging.format`

*String, default: `auto`*

Controls the log format. One of:

  - `auto`: Auto detect (pretty for TTY, simplified for other)
  - `pretty`: Human readable format with colors
  - `simplified`: Simplified human readable log output
  - `json`: JSON records, suitable for logging software

### `logging.enable_backtraces`

*boolean, default: `true`*

Writes back traces for all internal errors to the log stream and includes them in Sentry errors, if enabled.

## Statsd Metrics

### `metrics.statsd`

*String, optional*

If set to a host/port string then metrics will be reported to this statsd
instance.

### `metrics.prefix`

*String, default: `sentry.relay`*

The prefix that should be added to all metrics.

### `metrics.default_tags`

*Map of strings to strings, default empty*

A set of default tags that should be attached to all outgoing statsd metrics.

### `metrics.hostname_tag`

*String, optional*

If set, reports the current hostname under the given tag name for all metrics.

## Internal Error Reporting

Configures error reporting for errors happening within Relay. Disabled by
default.

### `sentry.enabled`

*boolean, default: `false`*

Whether to report internal errors to a separate DSN. `false` means no internal
errors are sent, but still logged.

### `sentry.dsn`

*String, optional*

DSN to report internal Relay failures to.

It is not a good idea to set this to a value that will make the Relay send
errors to itself. Ideally this should just send errors to Sentry directly, not
another Relay.

[relay modes]: ../modes/
