# Configuration Options

The base configuration for Relay lives in the file `.relay/config.yml`.  All keys are `snake_case`.

## Relay

General relay settings.

`relay.mode`

: *string, default: `managed`* , 

: possible values: `managed`, `static`, `proxy` and `capture`  

  Controls mainly how Relay obtains the project configuration for events. For detailed explanation of the modes see: [Advanced Configuration](../advanced_config/) 

`relay.upstream`

: *string, default: `https://sentry.io`*

  The upstream relay or sentry instance.

  **Important**: Relay does not check for cycles. Ensure this option is not set
  to an endpoint that will cause events to be cycled back here.

`relay.host`

: *string, default: `127.0.0.1`*

  The host the relay should bind to (network interface).  Example: `0.0.0.0`

`relay.port`

: *integer, default: `3000`*

  The port to bind for the unencrypted relay HTTP server.  Example: `3000`

`relay.tls_port`

: *integer, optional*

  Optional port to bind for the encrypted relay HTTPS server.  Example: `3001`

  This is in addition to the `port` option: If you set up a HTTPS server at
  `tls_port`, the HTTP server at `port` still exists.

`relay.tls_identity_path`

: *string, optional*

  The filesystem path to the identity (DER-encoded PKCS12) to use for the HTTPS
  server.  Example: `relay_dev.pfx`

`relay.tls_identity_password`

: *string, optional*

  Password for the PKCS12 archive in `tls_identity_path`.

## HTTP

Set various network-related settings.

`http.timeout`

: *integer, default: `5`*

  Timeout for upstream requests in seconds.

`http.max_retry_interval`

: *integer, default: `60`*

  Maximum interval between failed request retries in seconds.

`host.header`

: string, default: `null`

​	The custom HTTP  Host header to be sent to the upstream.	

## Caching

Fine-tune caching of project state.

`cache.project_expiry`

: *integer, default: `300` (5 minutes)*

  The cache timeout for project configurations in seconds.  Irrelevant if you
  use the "simple proxy mode", where your project config is stored in a local
  file.

`cache.project_grace_period`

:*integer, default: `0` (seconds)*

  Number of seconds to continue using this project configuration after cache 
  expiry while a new state is being fetched. This is added on top of `project_expiry` 
  and `miss_expiry`.

`cache.relay_expiry`

: *integer, default: `3600` (1 hour)*

  The cache timeout for downstream relay info (public keys) in seconds.

`cache.event_expiry`

: *integer, default: `600` (10 minutes)*

  The cache timeout for events (store) before dropping them.

`cache.miss_expiry`

: *integer, default: `60` (1 minute)*

  The cache timeout for non-existing entries.

`cache.batch_interval`

: *integer, default: `100` (100 milliseconds)*

  The buffer timeout for batched queries before sending them upstream **in
  milliseconds**.

`cache.batch_size`
: *integer, default: `500`*

  The maximum number of project configs to fetch from Sentry at once.

`cache.file_interval`

: *integer, default: `10` (10 seconds)*

  Interval for watching local cache override files in seconds.

`cache.event_buffer_size`

: *integer, default: `1000`*

  The maximum number of events that are buffered in case of network issues or
  high rates of incoming events.

`cache.eviction_interval`

: *integer, default: `60` (seconds)*

  Interval for evicting outdated project configs from memory.

## Size Limits

Controls various HTTP-related limits.  All values are either integers or are human-readable strings of a number 
and a human-readable unit, such as:

- `1KiB`
- `1MB`
- `1MiB`
- `1MiB`
- `1025B`

`limits.max_concurrent_requests`

: *integer, default: `100`*

  The maximum number of concurrent connections to the upstream.

`limits.max_concurrent_queries`

: *integer, default: `5`*

  The maximum number of  queries that can be sent concurrently from Relay to the upstream 
  before Relay starts buffering.

  The concurrency of queries is additionally constrained by `max_concurrent_requests`.

`limits.max_event_size`

: *string, default: `1MB`*

  The maximum payload size for events.

`limits.max_attachment_size`

: *string, default: `50Mb`*

  The maximum size for each attachment.

`limits.max_attachments_size`

: *string, default: `50Mb`*

  The maximum combined size for all attachments in an envelope or request. 

`limits.max_envelope_size`

: *string, default: `50Mb`*

  The maximum payload size for an entire envelopes. Individual limits still apply.

`limits.max_session_count`

: *integer, default: `100`*

  The maximum number of session items per envelope.

`limits.max_api_payload_size`

: *string, default: `20MB`*

  The maximum payload size for general API requests.

`limits.max_api_file_upload_size`

: *string, default: `40MB`*

  The maximum payload size for file uploads and chunks.

`limits.max_api_chunk_upload_size`

: *string, default: `100MB`*

  The maximum payload size for chunks

`limits.max_thread_count`

: *integer, default: number of cpus*
  
  The maximum number of threads to spawn for CPU and web worker, each.
    
  The total number of threads spawned will roughly be `2 * max_thread_count + 1`.

`limits.query_timeout`

: *integer, default: `30` (seconds)* 
  
  The maximum number of seconds a query is allowed to take across retries. Individual requests
  have lower timeouts. Defaults to 30 seconds.


`limits.max_connection_rate`
  
: *integer, default:  `256`*
  
  The maximum number of connections to Relay that can be created at once.


`limits.max_pending_connections`

: *integer, default: `2048`*
  
  The maximum number of pending connects to Relay. This corresponds to the backlog param of
  `listen(2)` in POSIX.


`limits.max_connections`

: *integer: default: `25_000`*
  
  The maximum number of open connections to Relay.


`limits.shutdown_timeout`

: *integer, default:L `10` (seconds)*
  
  The maximum number of seconds to wait for pending events after receiving a shutdown signal.

## Logging

`logging.level`

: *string, default: `info`*

  The log level for the relay. One of:

  - `off`
  - `error`
  - `warn`
  - `info`
  - `debug`
  - `trace`

`logging.log_failed_payloads`

: *boolean, default: `false`*

  If set to true this emits log messages for failed event payloads.

`logging.format`

: *string, default: `auto`*

  Controls the log format. One of:

  - `auto`: Auto detect (pretty for tty, simplified for other)
  - `pretty`: With colors
  - `simplified`: Simplified log output
  - `json`: Dump out JSON lines

`logging.enable_backtraces`

: *boolean, default: `true`*

  When set to true, backtraces are forced on.

## Statsd Metrics

`metrics.statsd`

: *string, optional*

  If set to a host/port string then metrics will be reported to this statsd
  instance.

`metrics.prefix`

: *string, default: `sentry.relay`*

  The prefix that should be added to all metrics.

`metrics.default_tags`

: *object of strings, default empty*

  A set of default tags that should be attached to all outgoing statsd metrics.

`metrics.hostname_tag`

: *string, optional*

  If set, report the current hostname under the given tag name for all metrics.

## Internal Error Reporting

Configures error reporting for errors happening within Sentry. Disabled by
default.

`sentry.enabled`

: *boolean, default: `false`*

  Whether to report internal errors to a separate DSN. `false` means no internal
  errors are sent (just logged).

`sentry.dsn`

: *string, optional*

  DSN to report internal relay failures to.

  It is not a good idea to set this to a value that will make the relay send
  errors to itself. Ideally this should just send errors to Sentry directly, not
  another relay.
