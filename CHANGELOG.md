# Changelog

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
