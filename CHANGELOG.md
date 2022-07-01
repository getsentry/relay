# Changelog

## Unreleased

**Features**:

- Extract metrics also from trace-sampled transactions. ([#1317](https://github.com/getsentry/relay/pull/1317))
- Adjust sample rate by envelope header's sample_rate. ([#1327](https://github.com/getsentry/relay/pull/1327))


**Bug Fixes**:

- Parse custom units with length < 15 without crashing. ([#1312](https://github.com/getsentry/relay/pull/1312))
- Split large metrics requests into smaller batches. This avoids failed metrics submission and lost Release Health data due to `413 Payload Too Large` errors on the upstream. ([#1326](https://github.com/getsentry/relay/pull/1326))

**Internal**:

- Reduce number of metrics extracted for release health. ([#1316](https://github.com/getsentry/relay/pull/1316))
- Indicate with thread is the main thread in thread metadata for profiles. ([#1320](https://github.com/getsentry/relay/pull/1320))
- Increase profile maximum size by an order of magnitude. ([#1321](https://github.com/getsentry/relay/pull/1321))
- Add data category constant for processed transactions, encompassing all transactions that have been received and sent through dynamic sampling as well as metrics extraction. ([#1306](https://github.com/getsentry/relay/pull/1306))
- Extract metrics from a configurable amount of custom transaction measurements. ([#1324](https://github.com/getsentry/relay/pull/1324))

## 22.6.0

**Compatibility:** This version of Relay requires Sentry server `22.6.0` or newer.

**Features**:

- Relay is now compatible with CentOS 7 and Red Hat Enterprise Linux 7 onward (kernel version _2.6.32_), depending on _glibc 2.17_ or newer. The `crash-handler` feature, which is currently enabled in the build published to DockerHub, additionally requires _curl 7.29_ or newer. ([#1279](https://github.com/getsentry/relay/pull/1279))
- Optionally start relay with `--upstream-dsn` to pass a Sentry DSN instead of the URL. This can be convenient when starting Relay in environments close to an SDK, where a DSN is already available. ([#1277](https://github.com/getsentry/relay/pull/1277))
- Add a new runtime mode `--aws-runtime-api=$AWS_LAMBDA_RUNTIME_API` that integrates Relay with the AWS Extensions API lifecycle. ([#1277](https://github.com/getsentry/relay/pull/1277))
- Add Replay ItemTypes. ([#1236](https://github.com/getsentry/relay/pull/1236), ([#1239](https://github.com/getsentry/relay/pull/1239))

**Bug Fixes**:

- Session metrics extraction: Count distinct_ids from all session updates to prevent undercounting users. ([#1275](https://github.com/getsentry/relay/pull/1275))
- Session metrics extraction: Count crashed+abnormal towards errored_preaggr. ([#1274](https://github.com/getsentry/relay/pull/1274))

**Internal**:

- Add version 3 to the project configs endpoint. This allows returning pending results which need to be polled later and avoids blocking batched requests on single slow entries. ([#1263](https://github.com/getsentry/relay/pull/1263))
- Emit specific event type tags for "processing.event.produced" metric. ([#1270](https://github.com/getsentry/relay/pull/1270))
- Add support for profile outcomes. ([#1272](https://github.com/getsentry/relay/pull/1272))
- Avoid potential panics when scrubbing minidumps. ([#1282](https://github.com/getsentry/relay/pull/1282))
- Fix typescript profile validation. ([#1283](https://github.com/getsentry/relay/pull/1283))
- Track memory footprint of metrics buckets. ([#1284](https://github.com/getsentry/relay/pull/1284), [#1287](https://github.com/getsentry/relay/pull/1287), [#1288](https://github.com/getsentry/relay/pull/1288))
- Support dedicated topics per metrics usecase, drop metrics from unknown usecases. ([#1285](https://github.com/getsentry/relay/pull/1285))
- Add support for Rust profiles ingestion ([#1296](https://github.com/getsentry/relay/pull/1296))

## 22.5.0

**Features**:

- Add platform, op, http.method and status tag to all extracted transaction metrics. ([#1227](https://github.com/getsentry/relay/pull/1227))
- Add units in built-in measurements. ([#1229](https://github.com/getsentry/relay/pull/1229))
- Add protocol support for custom units on transaction measurements. ([#1256](https://github.com/getsentry/relay/pull/1256))

**Bug Fixes**:

- fix(metrics): Enforce metric name length limit. ([#1238](https://github.com/getsentry/relay/pull/1238))
- Accept and forward unknown Envelope items. In processing mode, drop items individually rather than rejecting the entire request. This allows SDKs to send new data in combined Envelopes in the future. ([#1246](https://github.com/getsentry/relay/pull/1246))
- Stop extracting metrics with outdated names from sessions. ([#1251](https://github.com/getsentry/relay/pull/1251), [#1252](https://github.com/getsentry/relay/pull/1252))
- Update symbolic to pull in fixed Unreal parser that now correctly handles zero-length files. ([#1266](https://github.com/getsentry/relay/pull/1266))

**Internal**:

- Add sampling + tagging by event platform and transaction op. Some (unused) tagging rules from 22.4.0 have been renamed. ([#1231](https://github.com/getsentry/relay/pull/1231))
- Refactor aggregation error, recover from errors more gracefully. ([#1240](https://github.com/getsentry/relay/pull/1240))
- Remove/reject nul-bytes from metric strings. ([#1235](https://github.com/getsentry/relay/pull/1235))
- Remove the unused "internal" data category. ([#1245](https://github.com/getsentry/relay/pull/1245))
- Add the client and version as `sdk` tag to extracted session metrics in the format `name/version`. ([#1248](https://github.com/getsentry/relay/pull/1248))
- Expose `shutdown_timeout` in `OverridableConfig` ([#1247](https://github.com/getsentry/relay/pull/1247))
- Normalize all profiles and reject invalid ones. ([#1250](https://github.com/getsentry/relay/pull/1250))
- Raise a new InvalidCompression Outcome for invalid Unreal compression. ([#1237](https://github.com/getsentry/relay/pull/1237))
- Add a profile data category and count profiles in an envelope to apply rate limits. ([#1259](https://github.com/getsentry/relay/pull/1259))
- Support dynamic sampling by custom tags, operating system name and version, as well as device name and family. ([#1268](https://github.com/getsentry/relay/pull/1268))

## 22.4.0

**Features**:

- Map Windows version from raw_description to version name (XP, Vista, 11, ...). ([#1219](https://github.com/getsentry/relay/pull/1219))

**Bug Fixes**:

- Prevent potential OOM panics when handling corrupt Unreal Engine crashes. ([#1216](https://github.com/getsentry/relay/pull/1216))

**Internal**:

- Remove unused item types. ([#1211](https://github.com/getsentry/relay/pull/1211))
- Pin click dependency in requirements-dev.txt. ([#1214](https://github.com/getsentry/relay/pull/1214))
- Use fully qualified metric resource identifiers (MRI) for metrics ingestion. For example, the sessions duration is now called `d:sessions/duration@s`. ([#1215](https://github.com/getsentry/relay/pull/1215))
- Introduce metric units for rates and information, add support for custom user-declared units, and rename duration units to self-explanatory identifiers such as `second`. ([#1217](https://github.com/getsentry/relay/pull/1217))
- Increase the max profile size to accomodate a new platform. ([#1223](https://github.com/getsentry/relay/pull/1223))
- Set environment as optional when parsing a profile so we get a null value later on. ([#1224](https://github.com/getsentry/relay/pull/1224))
- Expose new tagging rules interface for metrics extracted from transactions. ([#1225](https://github.com/getsentry/relay/pull/1225))
- Return better BadStoreRequest for unreal events. ([#1226](https://github.com/getsentry/relay/pull/1226))

## 22.3.0

**Features**:

- Tag transaction metrics by user satisfaction. ([#1197](https://github.com/getsentry/relay/pull/1197))

**Bug Fixes**:

- CVE-2022-24713: Prevent denial of service through untrusted regular expressions used for PII scrubbing. ([#1207](https://github.com/getsentry/relay/pull/1207))
- Prevent dropping metrics during Relay shutdown if the project is outdated or not cached at time of the shutdown. ([#1205](https://github.com/getsentry/relay/pull/1205))
- Prevent a potential OOM when validating corrupted or exceptional minidumps. ([#1209](https://github.com/getsentry/relay/pull/1209))

**Internal**:

- Spread out metric aggregation over the aggregation window to avoid concentrated waves of metrics requests to the upstream every 10 seconds. Relay now applies jitter to `initial_delay` to spread out requests more evenly over time. ([#1185](https://github.com/getsentry/relay/pull/1185))
- Use a randomized Kafka partitioning key for sessions instead of the session ID. ([#1194](https://github.com/getsentry/relay/pull/1194))
- Add new statsd metrics for bucketing efficiency. ([#1199](https://github.com/getsentry/relay/pull/1199), [#1192](https://github.com/getsentry/relay/pull/1192), [#1200](https://github.com/getsentry/relay/pull/1200))
- Add a `Profile` `ItemType` to represent the profiling data sent from Sentry SDKs. ([#1179](https://github.com/getsentry/relay/pull/1179))

## 22.2.0

**Features**:

- Add the `relay.override_project_ids` configuration flag to support migrating projects from self-hosted to Sentry SaaS. ([#1175](https://github.com/getsentry/relay/pull/1175))

**Internal**:

- Add an option to dispatch billing outcomes to a dedicated topic. ([#1168](https://github.com/getsentry/relay/pull/1168))
- Add new `ItemType` to handle profiling data from Specto SDKs. ([#1170](https://github.com/getsentry/relay/pull/1170))

**Bug Fixes**:

- Fix regression in CSP report parsing. ([#1174](https://github.com/getsentry/relay/pull/1174))
- Ignore replacement_chunks when they aren't used. ([#1180](https://github.com/getsentry/relay/pull/1180))

## 22.1.0

**Features**:

- Flush metrics and outcome aggregators on graceful shutdown. ([#1159](https://github.com/getsentry/relay/pull/1159))
- Extract metrics from sampled transactions. ([#1161](https://github.com/getsentry/relay/pull/1161))

**Internal**:

- Extract normalized dist as metric. ([#1158](https://github.com/getsentry/relay/pull/1158))
- Extract transaction user as metric. ([#1164](https://github.com/getsentry/relay/pull/1164))

## 21.12.0

**Features**:

- Extract measurement ratings, port from frontend. ([#1130](https://github.com/getsentry/relay/pull/1130))
- External Relays perform dynamic sampling and emit outcomes as client reports. This feature is now enabled _by default_. ([#1119](https://github.com/getsentry/relay/pull/1119))
- Metrics extraction config, custom tags. ([#1141](https://github.com/getsentry/relay/pull/1141))
- Update the user agent parser (uap-core Feb 2020 to Nov 2021). This allows Relay and Sentry to infer more recent browsers, operating systems, and devices in events containing a user agent header. ([#1143](https://github.com/getsentry/relay/pull/1143), [#1145](https://github.com/getsentry/relay/pull/1145))
- Improvements to Unity OS context parsing ([#1150](https://github.com/getsentry/relay/pull/1150))

**Bug Fixes**:

- Support Unreal Engine 5 crash reports. ([#1132](https://github.com/getsentry/relay/pull/1132))
- Perform same validation for aggregate sessions as for individual sessions. ([#1140](https://github.com/getsentry/relay/pull/1140))
- Add missing .NET 4.8 release value. ([#1142](https://github.com/getsentry/relay/pull/1142))
- Properly document which timestamps are accepted. ([#1152](https://github.com/getsentry/relay/pull/1152))

**Internal**:

- Add more statsd metrics for relay metric bucketing. ([#1124](https://github.com/getsentry/relay/pull/1124), [#1128](https://github.com/getsentry/relay/pull/1128))
- Add an internal option to capture minidumps for hard crashes. This has to be enabled via the `sentry._crash_db` config parameter. ([#1127](https://github.com/getsentry/relay/pull/1127))
- Fold processing vs non-processing into single actor. ([#1133](https://github.com/getsentry/relay/pull/1133))
- Aggregate outcomes for dynamic sampling, invalid project ID, and rate limits. ([#1134](https://github.com/getsentry/relay/pull/1134))
- Extract session metrics from aggregate sessions. ([#1140](https://github.com/getsentry/relay/pull/1140))
- Prefix names of extracted metrics by `sentry.sessions.` or `sentry.transactions.`. ([#1147](https://github.com/getsentry/relay/pull/1147))
- Extract transaction duration as metric. ([#1148](https://github.com/getsentry/relay/pull/1148))

## 21.11.0

**Features**:

- Add bucket width to bucket protocol. ([#1103](https://github.com/getsentry/relay/pull/1103))
- Support multiple kafka cluster configurations. ([#1101](https://github.com/getsentry/relay/pull/1101))
- Tag metrics by transaction name. ([#1126](https://github.com/getsentry/relay/pull/1126))

**Bug Fixes**:

- Avoid unbounded decompression of encoded requests. A particular request crafted to inflate to large amounts of memory, such as a zip bomb, could put Relay out of memory. ([#1117](https://github.com/getsentry/relay/pull/1117), [#1122](https://github.com/getsentry/relay/pull/1122), [#1123](https://github.com/getsentry/relay/pull/1123))
- Avoid unbounded decompression of UE4 crash reports. Some crash reports could inflate to large amounts of memory before being checked for size, which could put Relay out of memory. ([#1121](https://github.com/getsentry/relay/pull/1121))

**Internal**:

- Aggregate client reports before sending them onwards. ([#1118](https://github.com/getsentry/relay/pull/1118))

## 21.10.0

**Bug Fixes**:

- Correctly validate timestamps for outcomes and sessions. ([#1086](https://github.com/getsentry/relay/pull/1086))
- Run compression on a thread pool when sending to upstream. ([#1085](https://github.com/getsentry/relay/pull/1085))
- Report proper status codes and error messages when sending invalid JSON payloads to an endpoint with a `X-Sentry-Relay-Signature` header. ([#1090](https://github.com/getsentry/relay/pull/1090))
- Enforce attachment and event size limits on UE4 crash reports. ([#1099](https://github.com/getsentry/relay/pull/1099))

**Internal**:

- Add the exclusive time of the transaction's root span. ([#1083](https://github.com/getsentry/relay/pull/1083))
- Add session.status tag to extracted session.duration metric. ([#1087](https://github.com/getsentry/relay/pull/1087))
- Serve project configs for batched requests where one of the project keys cannot be parsed. ([#1093](https://github.com/getsentry/relay/pull/1093))

## 21.9.0

**Features**:

- Add sampling based on transaction name. ([#1058](https://github.com/getsentry/relay/pull/1058))
- Support running Relay without config directory. The most important configuration, including Relay mode and credentials, can now be provided through commandline arguments or environment variables alone. ([#1055](https://github.com/getsentry/relay/pull/1055)
- Protocol support for client reports. ([#1081](https://github.com/getsentry/relay/pull/1081))
- Extract session metrics in non processing relays. ([#1073](https://github.com/getsentry/relay/pull/1073))

**Bug Fixes**:

- Use correct commandline argument name for setting Relay port. ([#1059](https://github.com/getsentry/relay/pull/1059))
- Retrieve OS Context for Unity Events. ([#1072](https://github.com/getsentry/relay/pull/1072))

**Internal**:

- Add new metrics on Relay's performance in dealing with buckets of metric aggregates, as well as the amount of aggregated buckets. ([#1070](https://github.com/getsentry/relay/pull/1070))
- Add the exclusive time of a span. ([#1061](https://github.com/getsentry/relay/pull/1061))
- Remove redundant dynamic sampling processing on fast path. ([#1084](https://github.com/getsentry/relay/pull/1084))

## 21.8.0

- No documented changes.

## 21.7.0

- No documented changes.

## 21.6.3

- No documented changes.

## 21.6.2

**Bug Fixes**:

- Remove connection metrics reported under `connector.*`. They have been fully disabled since version `21.3.0`. ([#1021](https://github.com/getsentry/relay/pull/1021))
- Remove error logs for "failed to extract event" and "failed to store session". ([#1032](https://github.com/getsentry/relay/pull/1032))

**Internal**:

- Assign a random Kafka partition key for session aggregates and metrics to distribute messages evenly. ([#1022](https://github.com/getsentry/relay/pull/1022))
- All fields in breakdown config should be camelCase, and rename the breakdown key name in project options. ([#1020](https://github.com/getsentry/relay/pull/1020))

## 21.6.1

- No documented changes.

## 21.6.0

**Features**:

- Support self-contained envelopes without authentication headers or query parameters. ([#1000](https://github.com/getsentry/relay/pull/1000))
- Support statically configured relays. ([#991](https://github.com/getsentry/relay/pull/991))
- Support namespaced event payloads in multipart minidump submission for Electron Framework. The field has to follow the format `sentry___<namespace>`. ([#1012](https://github.com/getsentry/relay/pull/1012))

**Bug Fixes**:

- Explicitly declare reprocessing context. ([#1009](https://github.com/getsentry/relay/pull/1009))
- Validate the environment attribute in sessions, and drop sessions with invalid releases. ([#1018](https://github.com/getsentry/relay/pull/1018))

**Internal**:

- Gather metrics for corrupted Events with unprintable fields. ([#1008](https://github.com/getsentry/relay/pull/1008))
- Remove project actors. ([#1025](https://github.com/getsentry/relay/pull/1025))

## 21.5.1

**Bug Fixes**:

- Do not leak resources when projects or DSNs are idle. ([#1003](https://github.com/getsentry/relay/pull/1003))

## 21.5.0

**Features**:

- Support the `frame.stack_start` field for chained async stack traces in Cocoa SDK v7. ([#981](https://github.com/getsentry/relay/pull/981))
- Rename configuration fields `cache.event_buffer_size` to `cache.envelope_buffer_size` and `cache.event_expiry` to `cache.envelope_expiry`. The former names are still supported by Relay. ([#985](https://github.com/getsentry/relay/pull/985))
- Add a configuraton flag `relay.ready: always` to mark Relay ready in healthchecks immediately after starting without requiring to authenticate. ([#989](https://github.com/getsentry/relay/pull/989))

**Bug Fixes**:

- Fix roundtrip error when PII selector starts with number. ([#982](https://github.com/getsentry/relay/pull/982))
- Avoid overflow panic for large retry-after durations. ([#992](https://github.com/getsentry/relay/pull/992))

**Internal**:

- Update internal representation of distribution metrics. ([#979](https://github.com/getsentry/relay/pull/979))
- Extract metrics for transaction breakdowns and sessions when the feature is enabled for the organizaiton. ([#986](https://github.com/getsentry/relay/pull/986))
- Assign explicit values to DataCategory enum. ([#987](https://github.com/getsentry/relay/pull/987))

## 21.4.1

**Bug Fixes**:

- Allow the `event_id` attribute on breadcrumbs to link between Sentry events. ([#977](https://github.com/getsentry/relay/pull/977))

## 21.4.0

**Bug Fixes**:

- Parse the Crashpad information extension stream from Minidumps with annotation objects correctly. ([#973](https://github.com/getsentry/relay/pull/973))

**Internal**:

- Emit outcomes for rate limited attachments. ([#951](https://github.com/getsentry/relay/pull/951))
- Remove timestamp from metrics text protocol. ([#972](https://github.com/getsentry/relay/pull/972))
- Add max, min, sum, and count to gauge metrics. ([#974](https://github.com/getsentry/relay/pull/974))

## 21.3.1

**Bug Fixes**:

- Make request url scrubbable. ([#955](https://github.com/getsentry/relay/pull/955))
- Remove dependent items from envelope when dropping transaction item. ([#960](https://github.com/getsentry/relay/pull/960))

**Internal**:

- Emit the `quantity` field for outcomes of events. This field describes the total size in bytes for attachments or the event count for all other categories. A separate outcome is emitted for attachments in a rejected envelope, if any, in addition to the event outcome. ([#942](https://github.com/getsentry/relay/pull/942))
- Add experimental metrics ingestion without bucketing or pre-aggregation. ([#948](https://github.com/getsentry/relay/pull/948))
- Skip serializing some null values in frames interface. ([#944](https://github.com/getsentry/relay/pull/944))
- Add experimental metrics ingestion with bucketing and pre-aggregation. ([#948](https://github.com/getsentry/relay/pull/948), [#952](https://github.com/getsentry/relay/pull/952), [#958](https://github.com/getsentry/relay/pull/958), [#966](https://github.com/getsentry/relay/pull/966), [#969](https://github.com/getsentry/relay/pull/969))
- Change HTTP response for upstream timeouts from 502 to 504. ([#859](https://github.com/getsentry/relay/pull/859))
- Add rule id to outcomes coming from transaction sampling. ([#953](https://github.com/getsentry/relay/pull/953))
- Add support for breakdowns ingestion. ([#934](https://github.com/getsentry/relay/pull/934))
- Ensure empty strings are invalid measurement names. ([#968](https://github.com/getsentry/relay/pull/968))

## 21.3.0

**Features**:

- Relay now picks up HTTP proxies from environment variables. This is made possible by switching to a different HTTP client library.

**Bug Fixes**:

- Deny backslashes in release names. ([#904](https://github.com/getsentry/relay/pull/904))
- Fix a problem with Data Scrubbing source names (PII selectors) that caused `$frame.abs_path` to match, but not `$frame.abs_path || **` or `$frame.abs_path && **`. ([#932](https://github.com/getsentry/relay/pull/932))
- Make username pii-strippable. ([#935](https://github.com/getsentry/relay/pull/935))
- Respond with `400 Bad Request` and an error message `"empty envelope"` instead of `429` when envelopes without items are sent to the envelope endpoint. ([#937](https://github.com/getsentry/relay/pull/937))
- Allow generic Slackbot ([#947](https://github.com/getsentry/relay/pull/947))

**Internal**:

- Emit the `category` field for outcomes of events. This field disambiguates error events, security events and transactions. As a side-effect, Relay no longer emits outcomes for broken JSON payloads or network errors. ([#931](https://github.com/getsentry/relay/pull/931))
- Add inbound filters functionality to dynamic sampling rules. ([#920](https://github.com/getsentry/relay/pull/920))
- The undocumented `http._client` option has been removed. ([#938](https://github.com/getsentry/relay/pull/938))
- Log old events and sessions in the `requests.timestamp_delay` metric. ([#933](https://github.com/getsentry/relay/pull/933))
- Add rule id to outcomes coming from event sampling. ([#943](https://github.com/getsentry/relay/pull/943))
- Fix a bug in rate limiting that leads to accepting all events in the last second of a rate limiting window, regardless of whether the rate limit applies. ([#946](https://github.com/getsentry/relay/pull/946))

## 21.2.0

**Features**:

- By adding `.no-cache` to the DSN key, Relay refreshes project configuration caches immediately. This allows to apply changed settings instantly, such as updates to data scrubbing or inbound filter rules. ([#911](https://github.com/getsentry/relay/pull/911))
- Add NSError to mechanism. ([#925](https://github.com/getsentry/relay/pull/925))
- Add snapshot to the stack trace interface. ([#927](https://github.com/getsentry/relay/pull/927))

**Bug Fixes**:

- Log on INFO level when recovering from network outages. ([#918](https://github.com/getsentry/relay/pull/918))
- Fix a panic in processing minidumps with invalid location descriptors. ([#919](https://github.com/getsentry/relay/pull/919))

**Internal**:

- Improve dynamic sampling rule configuration. ([#907](https://github.com/getsentry/relay/pull/907))
- Compatibility mode for pre-aggregated sessions was removed. The feature is now enabled by default in full fidelity. ([#913](https://github.com/getsentry/relay/pull/913))

## 21.1.0

**Features**:

- Support dynamic sampling for error events. ([#883](https://github.com/getsentry/relay/pull/883))

**Bug Fixes**:

- Make all fields but event-id optional to fix regressions in user feedback ingestion. ([#886](https://github.com/getsentry/relay/pull/886))
- Remove `kafka-ssl` feature because it breaks development workflow on macOS. ([#889](https://github.com/getsentry/relay/pull/889))
- Accept envelopes where their last item is empty and trailing newlines are omitted. This also fixes a panic in some cases. ([#894](https://github.com/getsentry/relay/pull/894))

**Internal**:

- Extract crashpad annotations into contexts. ([#892](https://github.com/getsentry/relay/pull/892))
- Normalize user reports during ingestion and create empty fields. ([#903](https://github.com/getsentry/relay/pull/903))
- Ingest and normalize sample rates from envelope item headers. ([#910](https://github.com/getsentry/relay/pull/910))

## 20.12.1

- No documented changes.

## 20.12.0

**Features**:

- Add `kafka-ssl` compilation feature that builds Kafka linked against OpenSSL. This feature is enabled in Docker containers only. This is only relevant for Relays running as part of on-premise Sentry. ([#881](https://github.com/getsentry/relay/pull/881))
- Relay is now able to ingest pre-aggregated sessions, which will make it possible to efficiently handle applications that produce thousands of sessions per second. ([#815](https://github.com/getsentry/relay/pull/815))
- Add protocol support for WASM. ([#852](https://github.com/getsentry/relay/pull/852))
- Add dynamic sampling for transactions. ([#835](https://github.com/getsentry/relay/pull/835))
- Send network outage metric on healthcheck endpoint hit. ([#856](https://github.com/getsentry/relay/pull/856))

**Bug Fixes**:

- Fix a long-standing bug where log messages were not addressible as `$string`. ([#882](https://github.com/getsentry/relay/pull/882))
- Allow params in content-type for security requests to support content types like `"application/expect-ct-report+json; charset=utf-8"`. ([#844](https://github.com/getsentry/relay/pull/844))
- Fix a panic in CSP filters. ([#848](https://github.com/getsentry/relay/pull/848))
- Do not drop sessions due to an invalid age constraint set to `0`. ([#855](https://github.com/getsentry/relay/pull/855))
- Do not emit outcomes after forwarding envelopes to the upstream, even if that envelope is rate limited, rejected, or dropped. Since the upstream logs an outcome, it would be a duplicate. ([#857](https://github.com/getsentry/relay/pull/857))
- Fix status code for security report. ([#864](https://github.com/getsentry/relay/pull/864))
- Add missing fields for Expect-CT reports. ([#865](https://github.com/getsentry/relay/pull/865))
- Support more directives in CSP reports, such as `block-all-mixed-content` and `require-trusted-types-for`. ([#876](https://github.com/getsentry/relay/pull/876))

**Internal**:

- Add _experimental_ support for picking up HTTP proxies from the regular environment variables. This feature needs to be enabled by setting `http: client: "reqwest"` in your `config.yml`. ([#839](https://github.com/getsentry/relay/pull/839))
- Refactor transparent request forwarding for unknown endpoints. Requests are now entirely buffered in memory and occupy the same queues and actors as other requests. This should not cause issues but may change behavior under load. ([#839](https://github.com/getsentry/relay/pull/839))
- Add reason codes to the `X-Sentry-Rate-Limits` header in store responses. This allows external Relays to emit outcomes with the proper reason codes. ([#850](https://github.com/getsentry/relay/pull/850))
- Emit metrics for outcomes in external relays. ([#851](https://github.com/getsentry/relay/pull/851))
- Make `$error.value` `pii=true`. ([#837](https://github.com/getsentry/relay/pull/837))
- Send `key_id` in partial project config. ([#854](https://github.com/getsentry/relay/pull/854))
- Add stack traces to Sentry error reports. ([#872](https://github.com/getsentry/relay/pull/872))

## 20.11.1

- No documented changes.

## 20.11.0

**Features**:

- Rename upstream retries histogram metric and add upstream requests duration metric. ([#816](https://github.com/getsentry/relay/pull/816))
- Add options for metrics buffering (`metrics.buffering`) and sampling (`metrics.sample_rate`). ([#821](https://github.com/getsentry/relay/pull/821))

**Bug Fixes**:

- Accept sessions with IP address set to `{{auto}}`. This was previously rejected and silently dropped. ([#827](https://github.com/getsentry/relay/pull/827))
- Fix an issue where every retry-after response would be too large by one minute. ([#829](https://github.com/getsentry/relay/pull/829))

**Internal**:

- Always apply cache debouncing for project states. This reduces pressure on the Redis and file system cache. ([#819](https://github.com/getsentry/relay/pull/819))
- Internal refactoring such that validating of characters in tags no longer uses regexes internally. ([#814](https://github.com/getsentry/relay/pull/814))
- Discard invalid user feedback sent as part of envelope. ([#823](https://github.com/getsentry/relay/pull/823))
- Emit event errors and normalization errors for unknown breadcrumb keys. ([#824](https://github.com/getsentry/relay/pull/824))
- Normalize `breadcrumb.ty` into `breadcrumb.type` for broken Python SDK versions. ([#824](https://github.com/getsentry/relay/pull/824))
- Add the client SDK interface for unreal crashes and set the name to `unreal.crashreporter`. ([#828](https://github.com/getsentry/relay/pull/828))
- Fine-tune the selectors for minidump PII scrubbing. ([#818](https://github.com/getsentry/relay/pull/818), [#830](https://github.com/getsentry/relay/pull/830))

## 20.10.1

**Internal**:

- Emit more useful normalization meta data for invalid tags. ([#808](https://github.com/getsentry/relay/pull/808))

## 20.10.0

**Features**:

- Add support for measurement ingestion. ([#724](https://github.com/getsentry/relay/pull/724), [#785](https://github.com/getsentry/relay/pull/785))
- Add support for scrubbing UTF-16 data in attachments ([#742](https://github.com/getsentry/relay/pull/742), [#784](https://github.com/getsentry/relay/pull/784), [#787](https://github.com/getsentry/relay/pull/787))
- Add upstream request metric. ([#793](https://github.com/getsentry/relay/pull/793))
- The padding character in attachment scrubbing has been changed to match the masking character, there is no usability benefit from them being different. ([#810](https://github.com/getsentry/relay/pull/810))

**Bug Fixes**:

- Fix issue where `$span` would not be recognized in Advanced Data Scrubbing. ([#781](https://github.com/getsentry/relay/pull/781))
- Accept big-endian minidumps. ([#789](https://github.com/getsentry/relay/pull/789))
- Detect network outages and retry sending events instead of dropping them. ([#788](https://github.com/getsentry/relay/pull/788))

**Internal**:

- Project states are now cached separately per DSN public key instead of per project ID. This means that there will be multiple separate cache entries for projects with more than one DSN. ([#778](https://github.com/getsentry/relay/pull/778))
- Relay no longer uses the Sentry endpoint to resolve project IDs by public key. Ingestion for the legacy store endpoint has been refactored to rely on key-based caches only. As a result, the legacy endpoint is supported only on managed Relays. ([#800](https://github.com/getsentry/relay/pull/800))
- Fix rate limit outcomes, now emitted only for error events but not transactions. ([#806](https://github.com/getsentry/relay/pull/806), [#809](https://github.com/getsentry/relay/pull/809))

## 20.9.0

**Features**:

- Add support for attaching Sentry event payloads in Unreal crash reports by adding `__sentry` game data entries. ([#715](https://github.com/getsentry/relay/pull/715))
- Support chunked form data keys for event payloads on the Minidump endpoint. Since crashpad has a limit for the length of custom attributes, the sentry event payload can be split up into `sentry__1`, `sentry__2`, etc. ([#721](https://github.com/getsentry/relay/pull/721))
- Periodically re-authenticate with the upstream server. Previously, there was only one initial authentication. ([#731](https://github.com/getsentry/relay/pull/731))
- The module attribute on stack frames (`$frame.module`) and the (usually server side generated) attribute `culprit` can now be scrubbed with advanced data scrubbing. ([#744](https://github.com/getsentry/relay/pull/744))
- Compress outgoing store requests for events and envelopes including attachements using `gzip` content encoding. ([#745](https://github.com/getsentry/relay/pull/745))
- Relay now buffers all requests until it has authenticated with the upstream. ([#747](//github.com/getsentry/relay/pull/747))
- Add a configuration option to change content encoding of upstream store requests. The default is `gzip`, and other options are `identity`, `deflate`, or `br`. ([#771](https://github.com/getsentry/relay/pull/771))

**Bug Fixes**:

- Send requests to the `/envelope/` endpoint instead of the older `/store/` endpoint. This particularly fixes spurious `413 Payload Too Large` errors returned when using Relay with Sentry SaaS. ([#746](https://github.com/getsentry/relay/pull/746))

**Internal**:

- Remove a temporary flag from attachment kafka messages indicating rate limited crash reports to Sentry. This is now enabled by default. ([#718](https://github.com/getsentry/relay/pull/718))
- Performance improvement of http requests to upstream, high priority messages are sent first. ([#678](https://github.com/getsentry/relay/pull/678))
- Experimental data scrubbing on minidumps([#682](https://github.com/getsentry/relay/pull/682))
- Move `generate-schema` from the Relay CLI into a standalone tool. ([#739](//github.com/getsentry/relay/pull/739))
- Move `process-event` from the Relay CLI into a standalone tool. ([#740](//github.com/getsentry/relay/pull/740))
- Add the client SDK to session kafka payloads. ([#751](https://github.com/getsentry/relay/pull/751))
- Add a standalone tool to document metrics in JSON or YAML. ([#752](https://github.com/getsentry/relay/pull/752))
- Emit `processing.event.produced` for user report and session Kafka messages. ([#757](https://github.com/getsentry/relay/pull/757))
- Improve performance of event processing by avoiding regex clone. ([#767](https://github.com/getsentry/relay/pull/767))
- Assign a default name for unnamed attachments, which prevented attachments from being stored in Sentry. ([#769](https://github.com/getsentry/relay/pull/769))
- Add Relay version version to challenge response. ([#758](https://github.com/getsentry/relay/pull/758))

## 20.8.0

**Features**:

- Add the `http.connection_timeout` configuration option to adjust the connection and SSL handshake timeout. The default connect timeout is now increased from 1s to 3s. ([#688](https://github.com/getsentry/relay/pull/688))
- Supply Relay's version during authentication and check if this Relay is still supported. An error message prompting to upgrade Relay will be supplied if Relay is unsupported. ([#697](https://github.com/getsentry/relay/pull/697))

**Bug Fixes**:

- Reuse connections for upstream event submission requests when the server supports connection keepalive. Relay did not consume the response body of all requests, which caused it to reopen a new connection for every event. ([#680](https://github.com/getsentry/relay/pull/680), [#695](https://github.com/getsentry/relay/pull/695))
- Fix hashing of user IP addresses in data scrubbing. Previously, this could create invalid IP addresses which were later rejected by Sentry. Now, the hashed IP address is moved to the `id` field. ([#692](https://github.com/getsentry/relay/pull/692))
- Do not retry authentication with the upstream when a client error is reported (status code 4XX). ([#696](https://github.com/getsentry/relay/pull/696))

**Internal**:

- Extract the event `timestamp` from Minidump files during event normalization. ([#662](https://github.com/getsentry/relay/pull/662))
- Retain the full span description in transaction events instead of trimming it. ([#674](https://github.com/getsentry/relay/pull/674))
- Report all Kafka producer errors to Sentry. Previously, only immediate errors were reported but not those during asynchronous flushing of messages. ([#677](https://github.com/getsentry/relay/pull/677))
- Add "HubSpot Crawler" to the list of web crawlers for inbound filters. ([#693](https://github.com/getsentry/relay/pull/693))
- Improved typing for span data of transaction events, no breaking changes. ([#713](https://github.com/getsentry/relay/pull/713))
- **Breaking change:** In PII configs, all options on hash and mask redactions (replacement characters, ignored characters, hash algorithm/key) are removed. If they still exist in the configuration, they are ignored. ([#760](https://github.com/getsentry/relay/pull/760))

## 20.7.2

**Features**:

- Report metrics for connections to the upstream. These metrics are reported under `connector.*` and include information on connection reuse, timeouts and errors. ([#669](https://github.com/getsentry/relay/pull/669))
- Increased the maximum size of attachments from _50MiB_ to _100MiB_. Most notably, this allows to upload larger minidumps. ([#671](https://github.com/getsentry/relay/pull/671))

**Internal**:

- Always create a spans array for transactions in normalization. This allows Sentry to render the spans UI even if the transaction is empty. ([#667](https://github.com/getsentry/relay/pull/667))

## 20.7.1

- No documented changes.

## 20.7.0

**Features**:

- Sessions and attachments can be rate limited now. These rate limits apply separately from error events, which means that you can continue to send Release Health sessions while you're out of quota with errors. ([#636](https://github.com/getsentry/relay/pull/636))

**Bug Fixes**:

- Outcomes from downstream relays were not forwarded upstream. ([#632](https://github.com/getsentry/relay/pull/632))
- Apply clock drift correction to Release Health sessions and validate timestamps. ([#633](https://github.com/getsentry/relay/pull/633))
- Apply clock drift correction for timestamps that are too far in the past or future. This fixes a bug where broken transaction timestamps would lead to negative durations. ([#634](https://github.com/getsentry/relay/pull/634), [#654](https://github.com/getsentry/relay/pull/654))
- Respond with status code `200 OK` to rate limited minidump and UE4 requests. Third party clients otherwise retry those requests, leading to even more load. ([#646](https://github.com/getsentry/relay/pull/646), [#647](https://github.com/getsentry/relay/pull/647))
- Ingested unreal crash reports no longer have a `misc_primary_cpu_brand` key with GPU information set in the Unreal context. ([#650](https://github.com/getsentry/relay/pull/650))
- Fix ingestion of forwarded outcomes in processing Relays. Previously, `emit_outcomes` had to be set explicitly to enable this. ([#653](https://github.com/getsentry/relay/pull/653))

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
