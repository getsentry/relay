# Changelog

## 0.3.4

This release was primarily done because publishing 0.3.3 failed.

- Upgrade sentry sdk

## 0.3.3

- store: Minor fixes to be closer to Python.

## 0.3.2

- Fix bug in store that would throw away all context lines.

## 0.3.1

- No longer check required attributes in relay, only in store.
- store: Minor fixes to be closer to Python. Ability to disable trimming of
  objects, arrays and strings.

## 0.3.0

- Changed PII stripping rule format to permit path selectors when applying
  rules.  This means that now `$string` refers to strings for instance and
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
