# Changelog

## Unreleased

**Features**:

- Sessions and attachments can be rate limited now. These rate limits apply separately from error events, which means that you can continue to send Release Health sessions while you're out of quota with errors. ([#636](https://github.com/getsentry/relay/pull/636))

**Bug Fixes**:

- Outcomes from downstream relays were not forwarded upstream. ([#632](https://github.com/getsentry/relay/pull/632))
- Apply clock drift correction to Release Health sessions and validate timestamps. ([#633](https://github.com/getsentry/relay/pull/633))
- Apply clock drift correction for timestamps that are too far in the past or future. This fixes a bug where broken transaction timestamps would lead to negative durations. ([#634](https://github.com/getsentry/relay/pull/634))
- Respond with status code `200 OK` to rate limited minidump and UE4 requests. Third party clients otherwise retry those requests, leading to even more load. ([#646](https://github.com/getsentry/relay/pull/646), [#647](https://github.com/getsentry/relay/pull/647))
- Ingested unreal crash reports no longer have a `misc_primary_cpu_brand` key with GPU information set in the Unreal context. ([#650](https://github.com/getsentry/relay/pull/650))

**Internal**:

- Restructure the envelope and event ingestion paths into a pipeline and apply rate limits to all envelopes. ([#635](https://github.com/getsentry/relay/pull/635), [#636](https://github.com/getsentry/relay/pull/636))
- Pass the combined size of all attachments in an envelope to the Redis rate limiter as quantity to enforce attachment quotas. ([#639](https://github.com/getsentry/relay/pull/639))
- Emit flags for rate limited processing attachments and add a `size` field. ([#640](https://github.com/getsentry/relay/pull/640), [#644](https://github.com/getsentry/relay/pull/644))

## 20.6.0

We have switched to [CalVer](https://calver.org/)! Relay's version is always in line with the latest version of [Sentry](https://github.com/getsentry/sentry).

**Features**:

- Proxy and managed Relays now apply clock drift correction based on the `sent_at` header emitted by SDKs. ([#581](https://github.com/getsentry/relay/pull/581))
- Apply cached rate limits to attachments and sessions in the fast-path when parsing incoming requests. ([#618](https://github.com/getsentry/relay/pull/618))
- New config options `metrics.default_tags` and `metrics.hostname_tag`. ([#625](https://github.com/getsentry/relay/pull/625))

**Bug Fixes**:

- Clock drift correction no longer considers the transaction timestamp as baseline for SDKs using Envelopes. Instead, only the dedicated `sent_at` Envelope header is used. ([#580](https://github.com/getsentry/relay/pull/580))
- The `http.timeout` setting is now applied to all requests, including event submission. Previously, events were exempt. ([#588](https://github.com/getsentry/relay/pull/588))
- All endpoint metrics now report their proper `route` tag. This applies to `requests`, `requests.duration`, and `responses.status_codes`. Previously, some some endpoints reported an empty route. ([#595](https://github.com/getsentry/relay/pull/595))
- Properly refresh cached project states based on the configured intervals. Previously, Relay may have gone into an endless refresh cycle if the system clock not accurate, or the state had not been updated in the upstream. ([#596](https://github.com/getsentry/relay/pull/596))
- Respond with `403 Forbidden` when multiple authentication payloads are sent by the SDK. Previously, Relay would authenticate using one of the payloads and silently ignore the rest. ([#602](https://github.com/getsentry/relay/pull/602))
- Improve metrics documentation. ([#614](https://github.com/getsentry/relay/pull/614))
- Do not scrub event processing errors by default. ([#619](https://github.com/getsentry/relay/pull/619))

**Internal**:

- Add source (who emitted the outcome) to Outcome payload. ([#604](https://github.com/getsentry/relay/pull/604))
- Ignore non-Rust folders for faster rebuilding and testing. ([#578](https://github.com/getsentry/relay/pull/578))
- Invalid session payloads are now logged for SDK debugging. ([#584](https://github.com/getsentry/relay/pull/584), [#591](https://github.com/getsentry/relay/pull/591))
- Add support for Outcomes generation in external Relays. ([#592](https://github.com/getsentry/relay/pull/592))
- Remove unused `rev` from project state. ([#586](https://github.com/getsentry/relay/pull/586))
- Add an outcome endpoint for trusted Relays. ([#589](https://github.com/getsentry/relay/pull/589))
- Emit outcomes for event payloads submitted in attachment files. ([#609](https://github.com/getsentry/relay/pull/609))
- Split envelopes that contain sessions and other items and ingest them independently. ([#610](https://github.com/getsentry/relay/pull/610))
- Removed support for legacy per-key quotas. ([#616](https://github.com/getsentry/relay/pull/615))
- Security events (CSP, Expect-CT, Expect-Staple, and HPKP) are now placed into a dedicated `security` item in envelopes, rather than the generic event item. This allows for quick detection of the event type for rate limiting. ([#617](https://github.com/getsentry/relay/pull/617))

## 0.5.9

- Relay has a logo now!
- New explicit `envelope/` endpoint. Envelopes no longer need to be sent with the right `content-type` header (to cater to browser JS).
- Introduce an Envelope item type for transactions.
- Support environment variables and CLI arguments instead of command line parameters.
- Return status `415` on wrong content types.
- Normalize double-slashes in request URLs more aggressively.
- Add an option to generate credentials on stdout.

**Internal**:

- Serve project configs to downstream Relays with proper permission checking.
- PII: Make and/or selectors specific.
- Add a browser filter for IE 11.
- Changes to release parsing.
- PII: Expose event values as part of generated selector suggestions.

## 0.5.8

**Internal**:

- Fix a bug where exception values and the device name were not PII-strippable.

## 0.5.7

- Docker images are now also pushed to Docker Hub.
- New helper function to generate PII selectors from event data.

**Internal**:

- Release is now a required attribute for session data.
- `unknown` can now be used in place of `unknown_error` for span statuses. A future release will change the canonical format from `unknown_error` to `unknown`.

## 0.5.6

- Fix a bug where Relay would stop processing events if Sentry is down for only a short time.
- Improvements to architecture documentation.
- Initial support for rate limiting by event type ("scoped quotas")
- Fix a bug where `key_id` was omitted from outcomes created by Relay.
- Fix a bug where it was not permitted to send content-encoding as part of a CORS request to store.

**Internal**:

- PII processing: Aliases for value types (`$error` instead of `$exception` to be in sync with Discover column naming) and adding a default for replace-redactions.
- It is now valid to send transactions and spans without `op` set, in which case a default value will be inserted.

## 0.5.5

- Suppress verbose DNS lookup logs.

**Internal**:

- Small performance improvements in datascrubbing config converter.
- New, C-style selector syntax (old one still works)

## 0.5.4

**Internal**:

- Add event contexts to `pii=maybe`.
- Fix parsing of msgpack breadcrumbs in Rust store.
- Envelopes sent to Rust store can omit the DSN in headers.
- Ability to quote/escape special characters in selectors in PII configs.

## 0.5.3

- Properly strip the linux binary to reduce its size
- Allow base64 encoded JSON event payloads ([#466](https://github.com/getsentry/relay/pull/466))
- Fix multipart requests without trailing newline ([#465](https://github.com/getsentry/relay/pull/465))
- Support for ingesting session updates ([#449](https://github.com/getsentry/relay/pull/449))

**Internal**:

- Validate release names during event ingestion ([#479](https://github.com/getsentry/relay/pull/479))
- Add browser extension filter ([#470](https://github.com/getsentry/relay/pull/470))
- Add `pii=maybe`, a new kind of event schema field that can only be scrubbed if explicitly addressed.
- Add way to scrub filepaths in a way that does not break processing.

## 0.5.2

- Fix trivial Redis-related crash when running in non-processing mode.
- Limit the maximum retry-after of a rate limit. This is necessary because of the "Delete and ignore future events" feature in Sentry.
- Project caches are now evicted after `project_grace_period` has passed. If you have that parameter set to a high number you may see increased memory consumption.

**Internal**:

- Misc bugfixes in PII processor. Those bugs do not affect the legacy data scrubber exposed in Python.
- Polishing documentation around PII configuration format.
- Signal codes in mach mechanism are no longer required.

## 0.5.1

- Ability to fetch project configuration from Redis as additional caching layer.
- Fix a few bugs in release filters.
- Fix a few bugs in minidumps endpoint with processing enabled.

**Internal**:

- Fix a bug in the PII processor that would always remove the entire string on `pattern` rules.
- Ability to correct some clock drift and wrong system time in transaction events.

## 0.5.0

- The binary has been renamed to `relay`.
- Updated documentation for metrics.

**Internal**:

- The package is now called `sentry-relay`.
- Renamed all `Semaphore*` types to `Relay*`.
- Fixed memory leaks in processing functions.

## 0.4.65

- Implement the Minidump endpoint.
- Implement the Attachment endpoint.
- Implement the legacy Store endpoint.
- Support a plain `Authorization` header in addition to `X-Sentry-Auth`.
- Simplify the shutdown logic. Relay now always takes a fixed configurable time to shut down.
- Fix healthchecks in _Static_ mode.
- Fix internal handling of event attachments.
- Fix partial reads of request bodies resulting in a broken connection.
- Fix a crash when parsing User-Agent headers.
- Fix handling of events sent with `sentry_version=2.0` (Clojure SDK).
- Use _mmap_ to load the GeoIP database to improve the memory footprint.
- Revert back to the system memory allocator.

**Internal**:

- Preserve microsecond precision in all time stamps.
- Record event ids in all outcomes.
- Updates to event processing metrics.
- Add span status mapping from open telemetry.

## 0.4.64

- Switched to `jemalloc` as global allocator.
- Introduce separate outcome reason for invalid events.
- Always consume request bodies to the end.
- Implemented minidump ingestion.
- Increas precisions of timestamps in protocol.

## 0.4.63

- Refactor healthchecks into two: Liveness and readiness (see code comments for explanation for now).
- Allow multiple trailing slashes on store endpoint, e.g. `/api/42/store///`.
- Internal refactor to prepare for envelopes format.

**Internal**:

- Fix a bug where glob-matching in filters did not behave correctly when the to-be-matched string contained newlines.
- Add `moz-extension:` as scheme for browser extensions (filtering out Firefox addons).
- Raise a dedicated Python exception type for invalid transaction events. Also do not report that error to Sentry from Relay.

## 0.4.62

- Various performance improvements.

## 0.4.61

**Internal**:

- Add `thread.errored` attribute ([#306](https://github.com/getsentry/relay/pull/306)).

## 0.4.60

- License is now BSL instead of MIT ([#301](https://github.com/getsentry/relay/pull/301)).
- Improve internal metrics and logging ([#296](https://github.com/getsentry/relay/pull/296), [#297](https://github.com/getsentry/relay/pull/297), [#298](https://github.com/getsentry/relay/pull/298)).
- Fix unbounded requests to Sentry for project configs ([#295](https://github.com/getsentry/relay/pull/295), [#300](https://github.com/getsentry/relay/pull/300)).
- Fix rejected responses from Sentry due to size limit ([#303](https://github.com/getsentry/relay/pull/303)).
- Expose more options for configuring request concurrency limits ([#311](https://github.com/getsentry/relay/pull/311)).

**Internal**:

- Transaction events with negative duration are now rejected ([#291](https://github.com/getsentry/relay/pull/291)).
- Fix a panic when normalizing certain dates.

## 0.4.59

**Internal**:

- Fix: Normalize legacy stacktrace attributes ([#292](https://github.com/getsentry/relay/pull/292))
- Fix: Validate platform attributes in Relay ([#294](https://github.com/getsentry/relay/pull/294))
- Flip the flag that indicates Relay processing ([#293](https://github.com/getsentry/relay/pull/293))

## 0.4.58

- Evict project caches after some time ([#287](https://github.com/getsentry/relay/pull/287))
- Selectively log internal errors to stderr ([#285](https://github.com/getsentry/relay/pull/285))
- Add an error boundary to parsing project states ([#281](https://github.com/getsentry/relay/pull/281))

**Internal**:

- Add event size metrics ([#286](https://github.com/getsentry/relay/pull/286))
- Normalize before datascrubbing ([#290](https://github.com/getsentry/relay/pull/290))
- Add a config value for thread counts ([#283](https://github.com/getsentry/relay/pull/283))
- Refactor outcomes for parity with Sentry ([#282](https://github.com/getsentry/relay/pull/282))
- Add flag that relay processed an event ([#279](https://github.com/getsentry/relay/pull/279))

## 0.4.57

**Internal**:

- Stricter validation of transaction events.

## 0.4.56

**Internal**:

- Fix a panic in trimming.

## 0.4.55

**Internal**:

- Fix more bugs in datascrubbing converter.

## 0.4.54

**Internal**:

- Fix more bugs in datascrubbing converter.

## 0.4.53

**Internal**:

- Fix more bugs in datascrubbing converter.

## 0.4.52

**Internal**:

- Fix more bugs in datascrubbing converter.

## 0.4.51

**Internal**:

- Fix a few bugs in datascrubbing converter.
- Accept trailing slashes.

**Normalization**:

- Fix a panic on overflowing timestamps.

## 0.4.50

**Internal**:

- Fix bug where IP scrubbers were applied even when not enabled.

## 0.4.49

- Internal changes.

## 0.4.48

**Internal**:

- Fix various bugs in the datascrubber and PII processing code to get closer to behavior of the Python implementation.

## 0.4.47

**Internal**:

- Various work on re-implementing Sentry's `/api/X/store` endpoint in Relay. Relay can now apply rate limits based on Redis and emit the correct outcomes.

## 0.4.46

**Internal**:

- Resolved a regression in IP address normalization. The new behavior is closer to a line-by-line port of the old Python code.

## 0.4.45

**Normalization**:

- Resolved an issue where GEO IP data was not always infered.

## 0.4.44

**Normalization**:

- Only take the user IP address from the store request's IP for certain platforms. This restores the behavior of the old Python code.

## 0.4.43

**Normalization**:

- Bump size of breadcrumbs.
- Workaround for an issue where we would not parse OS information from User Agent when SDK had already sent OS information.
- Further work on Sentry-internal event ingestion.

## 0.4.42

**Normalization**:

- Fix normalization of version strings from user agents.

## 0.4.41

- Support extended project configuration.

**Internal**:

- Implement event filtering rules.
- Add basic support for Sentry-internal event ingestion.
- Parse and normalize user agent strings.

## 0.4.40

**Internal**:

- Restrict ranges of timestamps to prevent overflows in Python code and UI.

## 0.4.39

**Internal**:

- Fix a bug where stacktrace trimming was not applied during renormalization.

## 0.4.38

**Internal**:

- Added typed spans to Event.

## 0.4.37

**Internal**:

- Added `orig_in_app` to frame data.

## 0.4.36

**Internal**:

- Add new .NET versions for context normalization.

## 0.4.35

**Internal**:

- Fix bug where thread's stacktraces were not normalized.
- Fix bug where a string at max depth of a databag was stringified again.

## 0.4.34

**Internal**:

- Added `data` attribute to frames.
- Added a way to override other trimming behavior in Python normalizer binding.

## 0.4.33

**Internal**:

- Plugin-provided context types should now work properly again.

## 0.4.32

**Internal**:

- Removed `function_name` field from frame and added `raw_function`.

## 0.4.31

**Internal**:

- Add trace context type.

## 0.4.30

**Internal**:

- Make exception messages/values larger to allow for foreign stacktrace data to be attached.

## 0.4.29

**Internal**:

- Added `function_name` field to frame.

## 0.4.28

**Internal**:

- Add missing context type for sessionstack.

## 0.4.27

**Internal**:

- Increase frame vars size again! Byte size was fine, but max depth was way too small.

## 0.4.26

**Internal**:

- Reduce frame vars size.

## 0.4.25

**Internal**:

- Add missing trimming to frame vars.

## 0.4.24

**Internal**:

- Reject non-http/https `help_urls` in exception mechanisms.

## 0.4.23

**Internal**:

- Add basic truncation to event meta to prevent payload size from spiralling out of control.

## 0.4.22

**Internal**:

- Added grouping enhancements to protocol.

## 0.4.21

**Internal**:

- Updated debug image interface with more attributes.

## 0.4.20

**Internal**:

- Added support for `lang` frame and stacktrace attribute.

## 0.4.19

**Internal**:

- Slight changes to allow replacing more normalization code in Sentry with Rust.

## 0.4.18

**Internal**:

- Allow much larger payloads in the extra attribute.

## 0.4.17

**Internal**:

- Added support for protocol changes related to upcoming sentry SDK features. In particular the `none` event type was added.

## 0.4.16

For users of relay, nothing changed at all. This is a release to test embedding some Rust code in Sentry itself.

## 0.4.15

For users of relay, nothing changed at all. This is a release to test embedding some Rust code in Sentry itself.

## 0.4.14

For users of relay, nothing changed at all. This is a release to test embedding some Rust code in Sentry itself.

## 0.4.13

For users of relay, nothing changed at all. This is a release to test embedding some Rust code in Sentry itself.

## 0.4.12

For users of relay, nothing changed at all. This is a release to test embedding some Rust code in Sentry itself.

## 0.4.11

For users of relay, nothing changed at all. This is a release to test embedding some Rust code in Sentry itself.

## 0.4.10

For users of relay, nothing changed at all. This is a release to test embedding some Rust code in Sentry itself.

## 0.4.9

For users of relay, nothing changed at all. This is a release to test embedding some Rust code in Sentry itself.

## 0.4.8

For users of relay, nothing changed at all. This is a release to test embedding some Rust code in Sentry itself.

## 0.4.7

For users of relay, nothing changed at all. This is a release to test embedding some Rust code in Sentry itself.

## 0.4.6

For users of relay, nothing changed at all. This is a release to test embedding some Rust code in Sentry itself.

## 0.4.5

For users of relay, nothing changed at all. This is a release to test embedding some Rust code in Sentry itself.

## 0.4.4

For users of relay, nothing changed at all. This is a release to test embedding some Rust code in Sentry itself.

## 0.4.3

For users of relay, nothing changed at all. This is a release to test embedding some Rust code in Sentry itself.

## 0.4.2

For users of relay, nothing changed at all. This is a release to test embedding some Rust code in Sentry itself.

## 0.4.1

For users of relay, nothing changed at all. This is a release to test embedding some Rust code in Sentry itself.

## 0.4.0

Introducing new Relay modes:

- `proxy`: A proxy for all requests and events.
- `static`: Static configuration for known projects in the file system.
- `managed`: Fetch configurations dynamically from Sentry and update them.

The default Relay mode is `managed`. Users upgrading from previous versions will automatically activate the `managed` mode. To change this setting, add `relay.mode` to `config.yml` or run `semaphore config init` from the command line.

**Breaking Change**: If Relay was used without credentials, the mode needs to be set to `proxy`. The default `managed` mode requires credentials.

For more information on Relay modes, see the [documentation page](https://docs.sentry.io/data-management/relay/options/).

### Configuration Changes

- Added `cache.event_buffer_size` to control the maximum number of events that are buffered in case of network issues or high rates of incoming events.
- Added `limits.max_concurrent_requests` to limit the number of connections that this Relay will use to communicate with the upstream.
- Internal error reporting is now disabled by default. To opt in, set `sentry.enabled`.

### Bugfixes

- Fix a bug that caused events to get unconditionally dropped after five seconds, regardless of the `cache.event_expiry` configuration.
- Fix a memory leak in Relay's internal error reporting.

## 0.3.0

- Changed PII stripping rule format to permit path selectors when applying rules. This means that now `$string` refers to strings for instance and `user.id` refers to the `id` field in the `user` attribute of the event. Temporarily support for old rules is retained.

## 0.2.7

- store: Minor fixes to be closer to Python. Ability to disable trimming of objects, arrays and strings.

## 0.2.6

- Fix bug where PII stripping would remove containers without leaving any metadata about the retraction.
- Fix bug where old `redactPair` rules would stop working.

## 0.2.5

- Rewrite of PII stripping logic. This brings potentially breaking changes to the semantics of PII configs. Most importantly field types such as `"freeform"` and `"databag"` are gone, right now there is only `"container"` and `"text"`. All old field types should have become an alias for `"text"`, but take extra care in ensuring your PII rules still work.

- store: Minor fixes to be closer to Python.

## 0.2.4

For users of relay, nothing changed at all. This is a release to test embedding some Rust code in Sentry itself.

- store: Remove stray print statement.

## 0.2.3

For users of relay, nothing changed at all. This is a release to test embedding some Rust code in Sentry itself.

- store: Fix main performance issues.

## 0.2.2

For users of relay, nothing changed at all. This is a release to test embedding some Rust code in Sentry itself.

- store: Fix segfault when trying to process contexts.
- store: Fix trimming state "leaking" between interfaces, leading to excessive trimming.
- store: Don't serialize empty arrays and objects (with a few exceptions).

## 0.2.1

For users of relay, nothing changed at all. This is a release to test embedding some Rust code in Sentry itself.

- `libsemaphore`: Expose CABI for normalizing event data.

## 0.2.0

Our first major iteration on Relay has landed!

- User documentation is now hosted at <https://docs.sentry.io/relay/>.
- SSL support is now included by default. Just configure a [TLS identity](https://docs.sentry.io/relay/options/#relaytls_identity_path) and you're set.
- Updated event processing: Events from older SDKs are now supported. Also, we've fixed some bugs along the line.
- Introduced full support for PII stripping. See [PII Configuration](https://docs.sentry.io/relay/pii-config/) for instructions.
- Configure with static project settings. Relay will skip querying project states from Sentry and use your provided values instead. See [Project Configuration](https://docs.sentry.io/relay/project-config/) for a full guide.
- Relay now also acts as a proxy for certain API requests. This allows it to receive CSP reports and Minidump crash reports, among others. It also sets `X-Forwarded-For` and includes a Relay signature header.

Besides that, there are many technical changes, including:

- Major rewrite of the internals. Relay no longer requires a special endpoint for sending events to upstream Sentry and processes events individually with less delay than before.
- The executable will exit with a non-zero exit code on startup errors. This makes it easier to catch configuration errors.
- Removed `libsodium` as a production dependency, greatly simplifying requirements for the runtime environment.
- Greatly improved logging and metrics. Be careful with the `DEBUG` and `TRACE` levels, as they are **very** verbose now.
- Improved docker containers.

## 0.1.3

- Added support for metadata format

## 0.1.2

- JSON logging ([#32](https://github.com/getsentry/relay/pull/32))
- Update dependencies

## 0.1.1

- Rename "sentry-relay" to "semaphore"
- Use new features from Rust 1.26
- Prepare binary and Python builds ([#20](https://github.com/getsentry/relay/pull/20))
- Add Dockerfile ([#23](https://github.com/getsentry/relay/pull/23))

## 0.1.0

An initial release of the tool.
