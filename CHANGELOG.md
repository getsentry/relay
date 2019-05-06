# Changelog

## 0.4.33

- Smaller protocol adjustments related to rolling out re-normalization in Rust.
- Plugin-provided context types should now work properly again.

## 0.4.32

- Removed `function_name` field from frame and added `raw_function`.

## 0.4.31

- Add trace context type.

## 0.4.30

- Make exception messages/values larger to allow for foreign stacktrace data to be attached.

## 0.4.29

- Added `function_name` field to frame.

## 0.4.28

- Add missing context type for sessionstack.

## 0.4.27

- Increase frame vars size again! Byte size was fine, but max depth
  was way too small.

## 0.4.26

- Reduce frame vars size.

## 0.4.25

- Add missing trimming to frame vars.

## 0.4.24

- Reject non-http/https `help_urls` in exception mechanisms.

## 0.4.23

- Add basic truncation to event meta to prevent payload size from spiralling out of control.

## 0.4.22

- Added grouping enhancements to protocol.

## 0.4.21

- Updated debug image interface with more attributes.

## 0.4.20

- Added support for `lang` frame and stacktrace attribute.

## 0.4.19

- Slight changes to allow replacing more normalization code in Sentry with Rust.

## 0.4.18

- Allow much larger payloads in the extra attribute.

## 0.4.17

- Added support for protocol changes related to upcoming sentry SDK features.
  In particular the `none` event type was added.

## 0.4.16

For users of relay, nothing changed at all. This is a release to test embedding
some Rust code in Sentry itself.

## 0.4.15

For users of relay, nothing changed at all. This is a release to test embedding
some Rust code in Sentry itself.

## 0.4.14

For users of relay, nothing changed at all. This is a release to test embedding
some Rust code in Sentry itself.

## 0.4.13

For users of relay, nothing changed at all. This is a release to test embedding
some Rust code in Sentry itself.

## 0.4.12

For users of relay, nothing changed at all. This is a release to test embedding
some Rust code in Sentry itself.

## 0.4.11

For users of relay, nothing changed at all. This is a release to test embedding
some Rust code in Sentry itself.

## 0.4.10

For users of relay, nothing changed at all. This is a release to test embedding
some Rust code in Sentry itself.

## 0.4.9

For users of relay, nothing changed at all. This is a release to test embedding
some Rust code in Sentry itself.

## 0.4.8

For users of relay, nothing changed at all. This is a release to test embedding
some Rust code in Sentry itself.

## 0.4.7

For users of relay, nothing changed at all. This is a release to test embedding
some Rust code in Sentry itself.

## 0.4.6

For users of relay, nothing changed at all. This is a release to test embedding
some Rust code in Sentry itself.

## 0.4.5

For users of relay, nothing changed at all. This is a release to test embedding
some Rust code in Sentry itself.

## 0.4.4

For users of relay, nothing changed at all. This is a release to test embedding
some Rust code in Sentry itself.

## 0.4.3

For users of relay, nothing changed at all. This is a release to test embedding
some Rust code in Sentry itself.

## 0.4.2

For users of relay, nothing changed at all. This is a release to test embedding
some Rust code in Sentry itself.

## 0.4.1

For users of relay, nothing changed at all. This is a release to test embedding
some Rust code in Sentry itself.

## 0.4.0

Introducing new Relay modes:

- `proxy`: A proxy for all requests and events.
- `static`: Static configuration for known projects in the file system.
- `managed`: Fetch configurations dynamically from Sentry and update them.

The default Relay mode is `managed`. Users upgrading from previous versions will
automatically activate the `managed` mode. To change this setting, add
`relay.mode` to `config.yml` or run `semaphore config init` from the command
line.

**Breaking Change**: If Relay was used without credentials, the mode needs to be
set to `proxy`. The default `managed` mode requires credentials.

For more information on Relay modes, see the [documentation
page](https://docs.sentry.io/data-management/relay/options/).

### Configuration Changes

- Added `cache.event_buffer_size` to control the maximum number of events that
  are buffered in case of network issues or high rates of incoming events.
- Added `limits.max_concurrent_requests` to limit the number of connections that
  this Relay will use to communicate with the upstream.
- Internal error reporting is now disabled by default. To opt in, set
  `sentry.enabled`.

### Bugfixes

- Fix a bug that caused events to get unconditionally dropped after five
  seconds, regardless of the `cache.event_expiry` configuration.
- Fix a memory leak in Relay's internal error reporting.

## 0.3.0

- Changed PII stripping rule format to permit path selectors when applying
  rules. This means that now `$string` refers to strings for instance and
  `user.id` refers to the `id` field in the `user` attribute of the event.
  Temporarily support for old rules is retained.

## 0.2.7

- store: Minor fixes to be closer to Python. Ability to disable trimming of
  objects, arrays and strings.

## 0.2.6

- Fix bug where PII stripping would remove containers without leaving any
  metadata about the retraction.
- Fix bug where old `redactPair` rules would stop working.

## 0.2.5

- Rewrite of PII stripping logic. This brings potentially breaking changes to
  the semantics of PII configs. Most importantly field types such as
  `"freeform"` and `"databag"` are gone, right now there is only `"container"`
  and `"text"`. All old field types should have become an alias for `"text"`,
  but take extra care in ensuring your PII rules still work.

- store: Minor fixes to be closer to Python.

## 0.2.4

For users of relay, nothing changed at all. This is a release to test embedding
some Rust code in Sentry itself.

- store: Remove stray print statement.

## 0.2.3

For users of relay, nothing changed at all. This is a release to test embedding
some Rust code in Sentry itself.

- store: Fix main performance issues.

## 0.2.2

For users of relay, nothing changed at all. This is a release to test embedding
some Rust code in Sentry itself.

- store: Fix segfault when trying to process contexts.
- store: Fix trimming state "leaking" between interfaces, leading to excessive trimming.
- store: Don't serialize empty arrays and objects (with a few exceptions).

## 0.2.1

For users of relay, nothing changed at all. This is a release to test embedding
some Rust code in Sentry itself.

- `libsemaphore`: Expose CABI for normalizing event data.

## 0.2.0

Our first major iteration on Relay has landed!

- User documentation is now hosted at <https://docs.sentry.io/relay/>.
- SSL support is now included by default. Just configure a [TLS
  identity](https://docs.sentry.io/relay/options/#relaytls_identity_path) and
  you're set.
- Updated event processing: Events from older SDKs are now supported. Also,
  we've fixed some bugs along the line.
- Introduced full support for PII stripping. See [PII
  Configuration](https://docs.sentry.io/relay/pii-config/) for instructions.
- Configure with static project settings. Relay will skip querying project
  states from Sentry and use your provided values instead. See [Project
  Configuration](https://docs.sentry.io/relay/project-config/) for a full guide.
- Relay now also acts as a proxy for certain API requests. This allows it to
  receive CSP reports and Minidump crash reports, among others. It also sets
  `X-Forwarded-For` and includes a Relay signature header.

Besides that, there are many technical changes, including:

- Major rewrite of the internals. Relay no longer requires a special endpoint
  for sending events to upstream Sentry and processes events individually with
  less delay than before.
- The executable will exit with a non-zero exit code on startup errors. This
  makes it easier to catch configuration errors.
- Removed `libsodium` as a production dependency, greatly simplifying
  requirements for the runtime environment.
- Greatly improved logging and metrics. Be careful with the `DEBUG` and `TRACE`
  levels, as they are **very** verbose now.
- Improved docker containers.

## 0.1.3

- Added support for metadata format

## 0.1.2

- JSON logging (#32)
- Update dependencies

## 0.1.1

- Rename "sentry-relay" to "semaphore"
- Use new features from Rust 1.26
- Prepare binary and Python builds (#20)
- Add Dockerfile (#23)

## 0.1.0

An initial release of the tool.
