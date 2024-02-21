# Changelog

## Unreleased
- This release requires Python 3.10 or later. There are no intentionally breaking changes included in this release, but we stopped testing against Python 3.9.

## 0.8.45

- Add `allow_negative` to `BuiltinMeasurementKey`. Filter out negative BuiltinMeasurements if `allow_negative` is false. ([#2982](https://github.com/getsentry/relay/pull/2982))
- Add ability to block metric tags matching a glob pattern. ([#2973](https://github.com/getsentry/relay/pull/2973))

## 0.8.44

- Add ability to block metrics matching a glob pattern. ([#2954](https://github.com/getsentry/relay/pull/2954))


## 0.8.43

- Fix JSON capitalization for cardinality config. ([#2979](https://github.com/getsentry/relay/pull/2979))

## 0.8.42

- Add automatic PII scrubbing to `logentry.params`. ([#2956](https://github.com/getsentry/relay/pull/2956))

## 0.8.41

- This release requires Python 3.9 or later. There are no intentionally breaking changes included in this release, but we stopped testing against Python 3.8.
- Normalize event timestamps before validating them, fixing cases where Relay would drop valid events with reason "invalid_transaction". ([#2878](https://github.com/getsentry/relay/pull/2878))
- Normalize error and trace-ids. Values must be valid UUIDs. ([#2931](https://github.com/getsentry/relay/pull/2931))
- Add a data category for indexed spans. ([#2937](https://github.com/getsentry/relay/pull/2937))

## 0.8.39

- Add `_metrics_summary` as temporary key on `Event` for a DDM experiment. ([#2757](https://github.com/getsentry/relay/pull/2757))
- Add metric_bucket data category. ([#2824](https://github.com/getsentry/relay/pull/2824))

## 0.8.38

- `normalize_performance_score` stores 0 to 1 cdf score instead of weighted score for each performance score component. ([#2734](https://github.com/getsentry/relay/pull/2734))

## 0.8.37

- License is now FSL instead of BSL ([#2739](https://github.com/getsentry/relay/pull/2739))
- Skip running `NormalizeProcessor` on renormalization. ([#2744](https://github.com/getsentry/relay/pull/2744))

## 0.8.36

- Validate span timestamps and IDs in light normalization on renormalization. ([#2679](https://github.com/getsentry/relay/pull/2679))
- Rename `validate_sampling_condition` to `validate_rule_condition`. ([#2720](https://github.com/getsentry/relay/pull/2720))

## 0.8.35

- Add `validate_pii_selector` to validate safe fields. ([#2687](https://github.com/getsentry/relay/pull/2687))

## 0.8.34

- Add context for NEL (Network Error Logging) reports to the event schema. ([#2421](https://github.com/getsentry/relay/pull/2421))

## 0.8.33

- Drop events starting or ending before January 1, 1970 UTC. ([#2613](https://github.com/getsentry/relay/pull/2613))
- Remove event spans starting or ending before January 1, 1970 UTC. ([#2627](https://github.com/getsentry/relay/pull/2627))
- Remove event breadcrumbs dating before January 1, 1970 UTC. ([#2635](https://github.com/getsentry/relay/pull/2635))
- Add `PerformanceScoreConfig` config and performance score calculations to measurements for frontend events. ([#2632](https://github.com/getsentry/relay/pull/2632))
- Add `locale` ,`screen_width_pixels`, `screen_height_pixels`, and `uuid` to the device context. ([#2640](https://github.com/getsentry/relay/pull/2640))
- Add feedback DataCategory. ([#2604](https://github.com/getsentry/relay/pull/2604))

## 0.8.32

- Add `scraping_attempts` field to the event schema. ([#2575](https://github.com/getsentry/relay/pull/2575))
- Drop events starting or ending before January 1, 1970 UTC. ([#2613](https://github.com/getsentry/relay/pull/2613))

## 0.8.31

- Add `Reservoir` variant to `SamplingRule`. ([#2550](https://github.com/getsentry/relay/pull/2550))
- Remove dynamic sampling ABI. ([#2515](https://github.com/getsentry/relay/pull/2515))
- Scrub span descriptions with encoded data images. ([#2560](https://github.com/getsentry/relay/pull/2560))

## 0.8.30

- Filter out exceptions originating in Safari extensions. ([#2408](https://github.com/getsentry/relay/pull/2408))
- Add a `DataCategory` for monitor seats (crons). ([#2480](https://github.com/getsentry/relay/pull/2480))
- Expose global config normalization function. ([#2498](https://github.com/getsentry/relay/pull/2498))

## 0.8.29

- Add rudimentary Mypy setup. ([#2384](https://github.com/getsentry/relay/pull/2384))

## 0.8.28

This release requires Python 3.8 or later.

- Add the configuration protocol for generic metrics extraction. ([#2252](https://github.com/getsentry/relay/pull/2252))
- Modernize python syntax. ([#2264](https://github.com/getsentry/relay/pull/2264))

## 0.8.27

- Add is_enabled flag on transaction filter. ([#2251](https://github.com/getsentry/relay/pull/2251))
- Add trace context to CheckIns. ([#2241](https://github.com/getsentry/relay/pull/2241))

## 0.8.26

- Add filter based on transaction names. ([#2118](https://github.com/getsentry/relay/pull/2118))
- Add `lock` attribute to the frame protocol. ([#2171](https://github.com/getsentry/relay/pull/2171))


## 0.8.25



## 0.8.24

- Compile regexes in PII config validation. ([#2152](https://github.com/getsentry/relay/pull/2152))

## 0.8.23

- Add `txNameReady` flag to project config. ([#2128](https://github.com/getsentry/relay/pull/2128))

## 0.8.22

- Store `geo.subdivision` of the end user location. ([#2058](https://github.com/getsentry/relay/pull/2058))
- Scrub URLs in span descriptions. ([#2095](https://github.com/getsentry/relay/pull/2095))
- Add new FFI function for running dynamic sampling. ([#2091](https://github.com/getsentry/relay/pull/2091))

## 0.8.21

- Add a data category for indexed profiles. ([#2051](https://github.com/getsentry/relay/pull/2051))

## 0.8.20

- Add `thread.state` field to protocol. ([#1896](https://github.com/getsentry/relay/pull/1896))
- Smart trim loggers for Java platforms. ([#1941](https://github.com/getsentry/relay/pull/1941))
- Perform PII scrubbing on meta's original_value field. ([#1892](https://github.com/getsentry/relay/pull/1892))
- PII scrub `span.data` by default. ([#1953](https://github.com/getsentry/relay/pull/1953))
- Scrub sensitive cookies. ([#1951](https://github.com/getsentry/relay/pull/1951))
- Changes how device class is determined for iPhone devices. Instead of checking processor frequency, the device model is mapped to a device class. ([#1970](https://github.com/getsentry/relay/pull/1970))
- Don't sanitize transactions if no clustering rules exist and no UUIDs were scrubbed. ([#1976](https://github.com/getsentry/relay/pull/1976))
- Add iPad support for device.class synthesis in light normalization. ([#2008](https://github.com/getsentry/relay/pull/2008))
- Include unknown feature flags in project config when serializing it. ([#2040](https://github.com/getsentry/relay/pull/2040))

## 0.8.19

- Protocol validation for source map image type. ([#1869](https://github.com/getsentry/relay/pull/1869))
- Scrub `span.data.http.query` with default scrubbers. ([#1889](https://github.com/getsentry/relay/pull/1889))
- Add Cloud Resource context. ([#1854](https://github.com/getsentry/relay/pull/1854))
- Add a `DataCategory` for monitors (crons). ([#1886](https://github.com/getsentry/relay/pull/1886))

## 0.8.18

- Add `instruction_addr_adjustment` field to `RawStacktrace`. ([#1716](https://github.com/getsentry/relay/pull/1716))
- Make sure to scrub all the fields with PII. If the fields contain an object, the entire object will be removed. ([#1789](https://github.com/getsentry/relay/pull/1789))
- Add new schema for dynamic sampling rules. ([#1790](https://github.com/getsentry/relay/pull/1790)
- Keep meta for removed custom measurements. ([#1815](https://github.com/getsentry/relay/pull/1815))

## 0.8.17

- Add utility function for matching CODEOWNER paths against a stacktrace filepath ([#1746](https://github.com/getsentry/relay/pull/1746))

## 0.8.16

- The minimum required Python version is now 3.8. This release does not contain known breaking changes for Python 3.7, but we no longer guarantee compatibility.
- Add support for decaying functions in dynamic sampling rules. ([#1692](https://github.com/getsentry/relay/pull/1692))
- Add Profiling Context to event protocol. ([#1748](https://github.com/getsentry/relay/pull/1748))
- Add OpenTelemetry Context to event protocol. ([#1617](https://github.com/getsentry/relay/pull/1617))
- Add `app.in_foreground` and `thread.main` flag to event protocol. ([#1578](https://github.com/getsentry/relay/pull/1578))
- Scrub all fields with IP addresses rather than only known IP address fields. ([#1725](https://github.com/getsentry/relay/pull/1725))
- Disallow `-` in measurement and breakdown names. These items are converted to metrics, which do not allow `-` in their name. ([#1571](https://github.com/getsentry/relay/pull/1571))
- Validate the distribution name in the event. ([#1556](https://github.com/getsentry/relay/pull/1556))
- Use correct meta object for logentry in light normalization. ([#1577](https://github.com/getsentry/relay/pull/1577))

## 0.8.15

- Restore correct behavior when `is_renormalize` is specified on `normalize_event`. ([#1548](https://github.com/getsentry/relay/pull/1548))

## 0.8.14 [YANKED]

**Warning:** This release contains a regression. Please update to a more recent version.

- Add `transaction_info` to event payloads, including the transaction's source and internal original transaction name. ([#1330](https://github.com/getsentry/relay/pull/1330))
- Add user-agent parsing to replays processor. ([#1420](https://github.com/getsentry/relay/pull/1420))
- `convert_datascrubbing_config` will now return an error string when conversion fails on big regexes. ([#1474](https://github.com/getsentry/relay/pull/1474))
- `relay_pii_strip_event` now treats any key containing `token` as a password. ([#1527](https://github.com/getsentry/relay/pull/1527))
- Add data category for indexed transactions. This will come to represent stored transactions, while the existing category will represent transaction metrics. ([#1535](https://github.com/getsentry/relay/pull/1535))

## 0.8.13

- Add a data category constant for Replays. ([#1239](https://github.com/getsentry/relay/pull/1239))
- Add data category constant for processed transactions, encompassing all transactions that have been received and sent through dynamic sampling as well as metrics extraction. ([#1306](https://github.com/getsentry/relay/pull/1306))
- Extend trace sampling protocol to deal with flat user data. ([#1318](https://github.com/getsentry/relay/pull/1318))

## 0.8.12

- Fix missing profile data category in the python library of 0.8.11 by regenerating the header for C-bindings. ([#1278](https://github.com/getsentry/relay/pull/1278))

## 0.8.11

- Add protocol support for custom units on transaction measurements. ([#1256](https://github.com/getsentry/relay/pull/1256))
- Add a profile data category and count profiles in an envelope to apply rate limits. ([#1259](https://github.com/getsentry/relay/pull/1259))

## 0.8.10

- Map Windows version from raw_description to version name (XP, Vista, 11, ...). ([#1219](https://github.com/getsentry/relay/pull/1219))
- Update rust-minidump to 0.10.0 ([#1209](https://github.com/getsentry/relay/pull/1209))
- Update regex to 1.5.5 ([#1207](https://github.com/getsentry/relay/pull/1207))
- Update the user agent parser (uap-core Feb 2020 to Nov 2021). ([#1143](https://github.com/getsentry/relay/pull/1143), [#1145](https://github.com/getsentry/relay/pull/1145))
- Improvements to Unity OS context parsing ([#1150](https://github.com/getsentry/relay/pull/1150))

## 0.8.9

- Add the exclusive time of a span. ([#1061](https://github.com/getsentry/relay/pull/1061))
- Add `ingest_path` to the event schema, capturing Relays that processed this event. ([#1062](https://github.com/getsentry/relay/pull/1062))
- Retrieve OS Context for Unity Events. ([#1072](https://github.com/getsentry/relay/pull/1072))
- Protocol support for client reports. ([#1081](https://github.com/getsentry/relay/pull/1081))
- Add the exclusive time of the transaction's root span. ([#1083](https://github.com/getsentry/relay/pull/1083))
- Build and publish binary wheels for `arm64` / `aarch64` on macOS and Linux. ([#1100](https://github.com/getsentry/relay/pull/1100))

## 0.8.8

- Bump release parser to 1.3.0 and add ability to compare versions. ([#1038](https://github.com/getsentry/relay/pull/1038))

## 1.1.4 - 2021-07-14 [YANKED]

- Bump release parser to 1.1.4. ([#1031](https://github.com/getsentry/relay/pull/1031))

## 0.8.7

- Bump release parser to 1.0.0. ([#1013](https://github.com/getsentry/relay/pull/1013))

## 0.8.6

- Add back `breadcrumb.event_id`. ([#977](https://github.com/getsentry/relay/pull/977))
- Add `frame.stack_start` for chained async stack traces. ([#981](https://github.com/getsentry/relay/pull/981))
- Fix roundtrip error when PII selector starts with number. ([#982](https://github.com/getsentry/relay/pull/982))
- Explicitly declare reprocessing context. ([#1009](https://github.com/getsentry/relay/pull/1009))
- Add `safari-web-extension` to known browser extensions. ([#1011](https://github.com/getsentry/relay/pull/1011))

## 0.8.5

- Skip serializing some null values in frames interface. ([#944](https://github.com/getsentry/relay/pull/944))
- Make request url scrubbable. ([#955](https://github.com/getsentry/relay/pull/955))

## 0.8.4

- Deny backslashes in release names. ([#904](https://github.com/getsentry/relay/pull/904))
- Remove dependencies on `openssl` and `zlib`. ([#914](https://github.com/getsentry/relay/pull/914))
- Fix `and` and `or` operators in PII selectors on fields declaring `pii=maybe`. ([#932](https://github.com/getsentry/relay/pull/932))
- Enable PII stripping on `user.username`. ([#935](https://github.com/getsentry/relay/pull/935))
- Expose dynamic rule condition validation. ([#941](https://github.com/getsentry/relay/pull/941))

## 0.8.3

- Add NSError to mechanism. ([#925](https://github.com/getsentry/relay/pull/925))
- Add snapshot to the stack trace interface. ([#927](https://github.com/getsentry/relay/pull/927))
- Drop python 2.7 support. ([#929](https://github.com/getsentry/relay/pull/929))

## 0.8.2

- Fix compile errors in the sdist with Rust 1.47 and later. ([#801](https://github.com/getsentry/relay/pull/801))
- Emit more useful normalization meta data for invalid tags. ([#808](https://github.com/getsentry/relay/pull/808))
- Internal refactoring such that validating of characters in tags no longer uses regexes internally. ([#814](https://github.com/getsentry/relay/pull/814))
- Normalize `breadcrumb.ty` into `breadcrumb.type` for broken Python SDK versions. ([#824](https://github.com/getsentry/relay/pull/824))
- Emit event errors and normalization errors for unknown breadcrumb keys. ([#824](https://github.com/getsentry/relay/pull/824))
- Make `$error.value` `pii=true`. ([#837](https://github.com/getsentry/relay/pull/837))
- Add protocol support for WASM. ([#852](https://github.com/getsentry/relay/pull/852))
- Add missing fields for Expect-CT reports. ([#865](https://github.com/getsentry/relay/pull/865))
- Support more directives in CSP reports, such as `block-all-mixed-content` and `require-trusted-types-for`. ([#876](https://github.com/getsentry/relay/pull/876))
- Fix a long-standing bug where log messages were not addressible as `$string`. ([#882](https://github.com/getsentry/relay/pull/882))
- Use manylinux2010 to build releases instead of manylinux1 to fix issues with newer Rust. ([#917](https://github.com/getsentry/relay/pull/917))

## 0.8.1

- Add support for measurement ingestion. ([#724](https://github.com/getsentry/relay/pull/724), [#785](https://github.com/getsentry/relay/pull/785))

## 0.8.0

- Fix issue where `$span` would not be recognized in Advanced Data Scrubbing. ([#781](https://github.com/getsentry/relay/pull/781))
- Require macOS 10.15.0 or newer for the macOS wheel after moving to GitHub Actions. ([#780](https://github.com/getsentry/relay/pull/780))

## 0.7.0

- In PII configs, all options on hash and mask redactions (replacement characters, ignored characters, hash algorithm/key) are removed. If they still exist in the configuration, they are ignored. ([#760](https://github.com/getsentry/relay/pull/760))
- Rename to the library target to `relay_cabi` and add documentation. ([#763](https://github.com/getsentry/relay/pull/763))
- Update FFI bindings with a new implementation for error handling. ([#766](https://github.com/getsentry/relay/pull/766))
- **Breaking:** Delete `scrub_event` function from public API. ([#773](https://github.com/getsentry/relay/pull/773))
- Add Relay version version to challenge response. ([#758](https://github.com/getsentry/relay/pull/758))

## 0.6.1

- Removed deprecated `pii_selectors_from_event`.
- Return `UnpackErrorSignatureExpired` from `validate_register_response` when the timestamp is too old.

## 0.6.0

- Updates the authentication mechanism by introducing a signed register state. Signatures of `create_register_challenge` and `validate_register_response` now take a mandatory `secret` parameter, and the public key is encoded into the state. ([#743](https://github.com/getsentry/relay/pull/743))

## 0.5.13

_Note: This accidentally got released as 0.15.13 as well, which has since been yanked._

- Fix Python 3 incompatibilities in Relay authentication helpers. ([#712](https://github.com/getsentry/relay/pull/712))

## 0.5.12

- Always create a spans array for transactions in normalization. ([#667](https://github.com/getsentry/relay/pull/667))
- Retain the full span description in transaction events instead of trimming it. ([#674](https://github.com/getsentry/relay/pull/674))
- Move hashed user ip addresses to `user.id` to avoid invalid IPs going into Snuba. ([#692](https://github.com/getsentry/relay/pull/692))
- Add `is_version_supported` to check for Relay compatibility during authentication. ([#697](https://github.com/getsentry/relay/pull/697))

## 0.5.11

- Add SpanStatus to span struct. ([#603](https://github.com/getsentry/relay/pull/603))
- Apply clock drift correction for timestamps that are too far in the past or future. This fixes a bug where broken transaction timestamps would lead to negative durations. ([#634](https://github.com/getsentry/relay/pull/634), [#654](https://github.com/getsentry/relay/pull/654))
- Add missing .NET 4.8 version mapping for runtime context normalization. ([#642](https://github.com/getsentry/relay/pull/642))
- Expose `DataCategory` and `SpanStatus` via the C-ABI to Python for code sharing. ([#651](https://github.com/getsentry/relay/pull/651))

## 0.5.10

- Set default transaction name ([#576](https://github.com/getsentry/relay/pull/576))
- Apply clock drift correction based on received_at ([#580](https://github.com/getsentry/relay/pull/580), [#582](https://github.com/getsentry/relay/pull/582))
- Add AWS Security Scanner to web crawlers ([#577](https://github.com/getsentry/relay/pull/577))
- Do not default transactions to level error ([#585](https://github.com/getsentry/relay/pull/585))
- Update `sentry-release-parser` to 0.6.0 ([#590](https://github.com/getsentry/relay/pull/590))
- Add schema for success metrics (failed and errored processing) ([#593](https://github.com/getsentry/relay/pull/593))

## 0.5.9

- PII: Make and/or selectors specific.
- Add a browser filter for IE 11.
- Changes to release parsing.
- PII: Expose event values as part of generated selector suggestions.

## 0.5.8

- Fix a bug where exception values and the device name were not PII-strippable.

## 0.5.7

- Release is now a required attribute for session data.
- `unknown` can now be used in place of `unknown_error` for span statuses. A future release will change the canonical format from `unknown_error` to `unknown`.

## 0.5.6

- Minor updates to PII processing: Aliases for value types (`$error` instead of `$exception` to be in sync with Discover column naming) and adding a default for replace-redactions.
- It is now valid to send transactions and spans without `op` set, in which case a default value will be inserted.

## 0.5.5

- Small performance improvements in datascrubbing config converter.
- New, C-style selector syntax (old one still works)

## 0.5.4

- Add event contexts to `pii=maybe`.
- Fix parsing of msgpack breadcrumbs in Rust store.
- Envelopes sent to Rust store can omit the DSN in headers.
- Ability to quote/escape special characters in selectors in PII configs.

## 0.5.3

- Validate release names during event ingestion ([#479](https://github.com/getsentry/relay/pull/479))
- Add browser extension filter ([#470](https://github.com/getsentry/relay/pull/470))
- Add `pii=maybe`, a new kind of event schema field that can only be scrubbed if explicitly addressed.
- Add way to scrub filepaths in a way that does not break processing.
- Add missing errors for JSON parsing and release validation ([#478](https://github.com/getsentry/relay/pull/478))
- Expose more datascrubbing utils ([#464](https://github.com/getsentry/relay/pull/464))

## 0.5.2

- Misc bugfixes in PII processor. Those bugs do not affect the legacy data scrubber exposed in Python.
- Polishing documentation around PII configuration format.
- Signal codes in mach mechanism are no longer required.

## 0.5.1

- Bump xcode version from 7.3 to 9.4, dropping wheel support for some older OS X versions.
- New function `validate_pii_config`.
- Fix a bug in the PII processor that would always remove the entire string on `pattern` rules.
- Ability to correct some clock drift and wrong system time in transaction events.

## 0.5.0

- The package is now called `sentry-relay`.
- Renamed all `Semaphore*` types to `Relay*`.
- Fixed memory leaks in processing functions.

## 0.4.65

- Preserve microsecond precision in all time stamps.
- Record event ids in all outcomes.
- Updates to event processing metrics.
- Add span status mapping from open telemetry.
- Fix glob-matching of newline characters.

## 0.4.64

- Added newline support for general glob code.
- Added span status mapping to python library.

## 0.4.63

- Fix a bug where glob-matching in filters did not behave correctly when the to-be-matched string contained newlines.
- Add `moz-extension:` as scheme for browser extensions (filtering out Firefox addons).
- Raise a dedicated Python exception type for invalid transaction events. Also do not report that error to Sentry from Relay.

## 0.4.62

- Spec out values of `event.contexts.trace.status`.
- `none` is now no longer a valid environment name.
- Do no longer drop transaction events in renormalization.
- Various performance improvements.

## 0.4.61

- Add `thread.errored` attribute ([#306](https://github.com/getsentry/relay/pull/306)).

## 0.4.60

- License is now BSL instead of MIT ([#301](https://github.com/getsentry/relay/pull/301)).
- Transaction events with negative duration are now rejected ([#291](https://github.com/getsentry/relay/pull/291)).
- Fix a panic when normalizing certain dates.

## 0.4.59

- Fix: Normalize legacy stacktrace attributes ([#292](https://github.com/getsentry/relay/pull/292))
- Fix: Validate platform attributes ([#294](https://github.com/getsentry/relay/pull/294))

## 0.4.58

- Expose globbing code from Relay to Python ([#288](https://github.com/getsentry/relay/pull/288))
- Normalize before datascrubbing ([#290](https://github.com/getsentry/relay/pull/290))
- Selectively log internal errors to stderr ([#285](https://github.com/getsentry/relay/pull/285))
- Do not ignore `process_value` result in `scrub_event` ([#284](https://github.com/getsentry/relay/pull/284))

## 0.4.57

- Stricter validation of transaction events

## 0.4.56

- Fix a panic in trimming

## 0.4.55

- Fix more bugs in datascrubbing converter

## 0.4.54

- Fix more bugs in datascrubbing converter

## 0.4.53

- Fix more bugs in datascrubbing converter

## 0.4.52

- Fix more bugs in datascrubbing converter

## 0.4.51

- Fix a few bugs in datascrubbing converter
- Fix a panic on overflowing timestamps

## 0.4.50

- Fix bug where IP scrubbers were applied even when not enabled

## 0.4.49

- Fix handling of panics in CABI/Python bindings

## 0.4.48

- Fix various bugs in the datascrubber and PII processing code to get closer to behavior of the Python implementation.

## 0.4.47

- Fix encoding issue in the Python layer of event normalization.

## 0.4.46

- Resolved a regression in IP address normalization. The new behavior is closer to a line-by-line port of the old Python code.

## 0.4.45

- Resolved an issue where GEO IP data was not always infered.

## 0.4.44

- Only take the user IP address from the store request's IP for certain platforms. This restores the behavior of the old Python code.

## 0.4.43

- Bump size of breadcrumbs
- Workaround for an issue where we would not parse OS information from User Agent when SDK had already sent OS information.

## 0.4.42

- Fix normalization of version strings from user agents.

## 0.4.41

- Parse and normalize user agent strings.

## 0.4.40

- Restrict ranges of timestamps to prevent overflows in Python code and UI.

## 0.4.39

- Fix a bug where stacktrace trimming was not applied during renormalization.

## 0.4.38

- Added typed spans to `Event`.

## 0.4.37

- Added `orig_in_app` to frame data.

## 0.4.36

- Add new .NET versions for context normalization.

## 0.4.35

- Fix bug where thread's stacktraces were not normalized.
- Fix bug where a string at max depth of a databag was stringified again.

## 0.4.34

- Added `data` attribute to frames.
- Added a way to override other trimming behavior in Python normalizer binding.

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

- Increase frame vars size again! Byte size was fine, but max depth was way too small.

## 0.4.26

- Reduce frame vars size.

## 0.4.25

- Add missing trimming to frame vars.

## 0.4.24

- Reject non-http/https `help_urls` in exception mechanisms ([#192](https://github.com/getsentry/relay/pull/192))

## 0.4.23

- Add basic truncation to event meta to prevent payload size from spiralling out of control.

## 0.4.22

- Improve the grouping protocol config ([#190](https://github.com/getsentry/relay/pull/190))

## 0.4.21

- Add new debug image variants ([#188](https://github.com/getsentry/relay/pull/188))
- Trim release and environment ([#184](https://github.com/getsentry/relay/pull/184))

## 0.4.20

- Alias level critical as fatal ([#182](https://github.com/getsentry/relay/pull/182))
- Add device properties from Java/.NET SDKs ([#185](https://github.com/getsentry/relay/pull/185))
- Add `lang` to frame and stacktrace ([#186](https://github.com/getsentry/relay/pull/186))

## 0.4.19

- Add mode for renormalization ([#181](https://github.com/getsentry/relay/pull/181))

## 0.4.18

- Restore the original behavior with supporting very large values in extra ([#180](https://github.com/getsentry/relay/pull/180))

## 0.4.17

- Add untyped spans for tracing ([#179](https://github.com/getsentry/relay/pull/179))
- Add the `none` event type

## 0.4.16

- Add support for synthetic mechanism markers ([#177](https://github.com/getsentry/relay/pull/177))

## 0.4.15

- Fix processors: Do not create `path_item` in `enter_nothing`

## 0.4.14

- Rename `template_info` to template
- Add two new untyped context types: `gpu`, `monitors`
- Rewrite `derive(ProcessValue)` to use `Structure::each_variant` ([#175](https://github.com/getsentry/relay/pull/175))

## 0.4.13

- Allow arrays as header values ([#176](https://github.com/getsentry/relay/pull/176))
- Swap `python-json-read-adapter` to git dependency

## 0.4.12

- Run json.dumps at max depth in databag ([#174](https://github.com/getsentry/relay/pull/174))

## 0.4.11

- Get oshint case-insensitively

## 0.4.10

- Trim `time_spent` to max value of db column

## 0.4.9

- Trim containers one level before max_depth ([#173](https://github.com/getsentry/relay/pull/173))
- Unconditionally overwrite `received`

## 0.4.8

- Fix bugs in array trimming, more code comments ([#172](https://github.com/getsentry/relay/pull/172))

## 0.4.7

- Deal with surrogate escapes in python bindings

## 0.4.6

- Reject exceptions with empty type and value ([#170](https://github.com/getsentry/relay/pull/170))
- Validate remote_addr before backfilling into user ([#171](https://github.com/getsentry/relay/pull/171))

## 0.4.5

- Adjust limits to fit values into db ([#167](https://github.com/getsentry/relay/pull/167))
- Environment is 64 chars in db
- Normalize macOS ([#168](https://github.com/getsentry/relay/pull/168))
- Use right maxchars for `transaction`, `dist`, `release`
- Do not add error to invalid url

## 0.4.4

- Reject unknown debug images ([#163](https://github.com/getsentry/relay/pull/163))
- Include original_value in `Meta::eq` ([#164](https://github.com/getsentry/relay/pull/164))
- Emit correct expectations for common types ([#162](https://github.com/getsentry/relay/pull/162))
- Permit invalid emails in user interface ([#161](https://github.com/getsentry/relay/pull/161))
- Drop long tags correctly ([#165](https://github.com/getsentry/relay/pull/165))
- Do not skip null values in pairlists ([#166](https://github.com/getsentry/relay/pull/166))

## 0.4.3

- Fix broken sdk_info parsing ([#156](https://github.com/getsentry/relay/pull/156))
- Add basic snapshot tests for normalize and event parsing ([#154](https://github.com/getsentry/relay/pull/154))
- Context trimming ([#153](https://github.com/getsentry/relay/pull/153))
- Coerce PHP frame vars array to object ([#159](https://github.com/getsentry/relay/pull/159))

## 0.4.2

- Remove content-type params
- Dont attempt to free() if python is shutting down
- Improve cookie header normalizations ([#151](https://github.com/getsentry/relay/pull/151))
- Implement LogEntry formatting ([#152](https://github.com/getsentry/relay/pull/152))
- Deduplicate tags ([#155](https://github.com/getsentry/relay/pull/155))
- Treat empty paths like no paths in frame normalization
- Remove cookie header when explicit cookies are given

## 0.4.1

- Do not remove empty cookies or headers ([#138](https://github.com/getsentry/relay/pull/138))
- Skip more empty containers ([#139](https://github.com/getsentry/relay/pull/139))
- Make `request.header` values lenient ([#145](https://github.com/getsentry/relay/pull/145))
- Remove internal tags when backfilling ([#146](https://github.com/getsentry/relay/pull/146))
- Implement advanced context normalization ([#140](https://github.com/getsentry/relay/pull/140))
- Retain additional properties in contexts ([#141](https://github.com/getsentry/relay/pull/141))
- Implement very lenient URL parsing ([#147](https://github.com/getsentry/relay/pull/147))
- Do not require breadcrumb timestamps ([#144](https://github.com/getsentry/relay/pull/144))
- Reject tags with long keys ([#149](https://github.com/getsentry/relay/pull/149))

## 0.4.0

- Add new options max_concurrent_events ([#134](https://github.com/getsentry/relay/pull/134))
- Dont move stacktrace before normalizing it ([#135](https://github.com/getsentry/relay/pull/135))
- Fix broken repr and crash when shutting down python
- Port slim_frame_data ([#137](https://github.com/getsentry/relay/pull/137))
- Special treatment for ellipsis in URLs
- Parse request bodies

## 0.3.0

- Changed PII stripping rule format to permit path selectors when applying rules. This means that now `$string` refers to strings for instance and `user.id` refers to the `id` field in the `user` attribute of the event. Temporarily support for old rules is retained.

## 0.2.7

- Minor fixes to be closer to Python. Ability to disable trimming of objects, arrays and strings.

## 0.2.6

- Fix bug where PII stripping would remove containers without leaving any metadata about the retraction.
- Fix bug where old `redactPair` rules would stop working.

## 0.2.5

- Rewrite of PII stripping logic. This brings potentially breaking changes to the semantics of PII configs. Most importantly field types such as `"freeform"` and `"databag"` are gone, right now there is only `"container"` and `"text"`. All old field types should have become an alias for `"text"`, but take extra care in ensuring your PII rules still work.

- Minor fixes to be closer to Python.

## 0.2.4

- Remove stray print statement.

## 0.2.3

- Fix main performance issues.

## 0.2.2

- Fix segfault when trying to process contexts.
- Fix trimming state "leaking" between interfaces, leading to excessive trimming.
- Don't serialize empty arrays and objects (with a few exceptions).

## 0.2.1

- Expose CABI for normalizing event data.

## 0.2.0

- Updated event processing: Events from older SDKs are now supported. Also, we've fixed some bugs along the line.
- Introduced full support for PII stripping.

## 0.1.3

- Added support for metadata format

## 0.1.2

- Update dependencies

## 0.1.1

- Rename "sentry-relay" to "semaphore"
- Use new features from Rust 1.26
- Prepare Python builds ([#20](https://github.com/getsentry/relay/pull/20))

## 0.1.0

An initial release of the library.
