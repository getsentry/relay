# Contributing to Relay with AI Coding Assistants

Use AI to move faster, not to lower standards. You own every line you submit.

## Ground Rules

- **You are responsible for the code.** Do not commit code you do not understand or cannot maintain.
- **AI output must meet normal standards.** That includes tests, docs, changelog entries, and consistency with existing patterns.
- **Be transparent.** If AI generated a large part of the change, mention it in the PR and explain how you validated it.
- **Validate before opening a PR.** Run `make check` and `make test` first. AI-generated code often looks right but fails lint, tests, or full-feature builds.
- **Review security-sensitive changes carefully.** Be extra cautious in `relay-pii`, `relay-auth`, `relay-quotas`, `relay-event-normalization`, and network-facing code in `relay-server`.

## Repo Setup

```bash
git clone https://github.com/getsentry/relay.git
cd relay
direnv allow
devenv sync
cargo check --all --all-features
```

### Important Context for AI Tools

Tell your AI tool these basics up front:

- Relay is a Rust cargo workspace with many relay-* crates.
- The main binary is in `relay/`.
- Core server logic lives in `relay-server/`.
- The processing feature enables full ingestion; without it, Relay mostly acts as a proxy.
- Integration tests are in `tests/integration/` and are written in Python.
- Snapshot tests use `insta`.
- Prefer make targets over raw cargo commands.

### Commands You Will Use Often

```
make check              # format + lint checks
make test-rust          # Rust tests, default features
make test-rust-all      # Rust tests, all features
make test-integration   # Python integration tests
make build              # dev build
make release            # release build with debug info
make format             # format code
make lint               # clippy
make all                # everything CI runs

cargo check --all --all-features
cargo insta review
```

For integration tests:

```
devservices up kafka redis
make build
.venv/bin/pytest tests/integration -k <test_name>
```

## Relay Mental Model

### Main crates

#### Core pipeline

- `relay-server` — HTTP, envelopes, project state, orchestration
- `relay-event-schema` — event types and schema
- `relay-event-normalization` — normalization and enrichment
- `relay-filter` — inbound filtering
- `relay-sampling` — dynamic sampling
- `relay-metrics` — metrics extraction
- `relay-kafka` — Kafka production

#### Security and correctness

- `relay-pii` — PII scrubbing
- `relay-auth` — auth and relay registration
- `relay-quotas` — rate limits and quotas
- `relay-cardinality` — metric cardinality limits

#### Other data paths

- `relay-spans`, `relay-replays`, `relay-profiling`, `relay-monitors`, `relay-ourlogs`, `relay-otel`

#### Infra and shared code

- `relay-config`, `relay-redis`, `relay-system`, `relay-common`, `relay-base-schema`, `relay-pattern`, `relay-statsd`, `relay-log`, `relay-threading`

#### FFI / Python

- `relay-cabi`, `relay-ffi`, `relay-ffi-macros`, `py/`

#### Event flow

A typical event goes through this path:

1. `relay-server` receives the envelope
2. `relay-auth` validates DSN and project
3. project config is fetched or cached
4. `relay-quotas` checks limits
5. `relay-filter` applies inbound filters
6. `relay-sampling` decides sampling
7. `relay-event-normalization` normalizes and enriches
8. `relay-pii` scrubs sensitive data
9. `relay-metrics` extracts metrics
10. Relay forwards upstream or produces to Kafka

Understand this flow before changing behavior. Many changes affect multiple crates.

#### Useful Exploration Prompts

Examples:

```

Show me how envelopes move through relay-server, starting at the HTTP handler.

Explain how project config is fetched and cached in relay-server.

Show me what a new envelope item type needs to implement, using an existing one as an example.

Show me how the processing feature changes behavior and where cfg gates are used.
```

Use `rg` with AI:

```

rg "ItemType::Transaction" relay-server/
rg "impl.*Annotated" relay-event-schema/
rg "KafkaTopic" relay-kafka/
```

## Common Change Patterns

### Add a new event field

Typical steps:

1. Add the field in relay-event-schema with the right #[metastructure(...)] attributes
2. Update normalization in relay-event-normalization if needed
3. Review snapshots with cargo insta review
4. Update relay-cabi and py/ if the field is exposed to Python
5. Add changelog entries as needed

Good prompt:

```

I want to add a new field `foo_bar`. Show me how a field like `release` is defined,

normalized, and exposed through relay-cabi.
```

### Add or change metrics extraction

Watch for:

- cardinality impact
- feature flags or dynamic kill switches
- tests for both extraction and output format

### Add a Kafka topic or message type

Be extra careful:

- downstream consumers may depend on schema stability
- serialization must stay compatible
- end-to-end integration tests matter

### Change PII scrubbing

Always verify:

- PII cannot leak through nested or unusual data shapes
- rules compose correctly
- tests clearly cover before/after behavior

## Common AI Failure Modes

### Feature flags

AI often writes code that works without processing but breaks with all features enabled.

Always check:

```

cargo check --all --all-features
```

### ```#[metastructure(…)]```

AI tools often invent invalid attributes or skip required ones.

Have the tool inspect existing usage instead of guessing.

### Snapshot tests

After schema or protocol changes:

```

cargo test --all --all-features
cargo insta review
```

Do not blindly accept snapshot updates. Read the diffs.

### Integration tests

AI often puts integration tests in Rust or ignores required services.

Remember:

- integration tests are Python
- they live in `tests/integration/`
- Kafka and Redis must be running

### Changelog

AI usually forgets this.

- Relay server changes go in `CHANGELOG.md`
- Python-facing changes go in `py/CHANGELOG.md`
- Use #skip-changelog only when appropriate

Format:

- Description of the change. (#PR_NUMBER)

### Partial testing

Testing one crate is not enough in a large workspace.

This:

```

cargo test -p relay-event-schema
```

is useful, but this is the real check:

```

cargo test --all --all-features
```

## Security Checklist

Relay handles untrusted traffic on the ingest path. Review every change for:

- **input validation** — malformed input must fail safely; avoid `unwrap()` on untrusted data
- **PII exposure** — new fields or paths may need scrubbing
- **resource usage** — avoid unbounded CPU, memory, or amplification risk
- **auth boundaries** — be very careful around DSN and project routing
- **new dependencies** — keep them minimal and compatible with cargo deny

Useful prompt:

```

What is the attack surface of this change? Consider untrusted input, PII leakage,
resource exhaustion, and auth bypass.
```

Then verify the answer yourself.

## Debugging with AI

Examples:

```

Here is a Rust compiler error in Relay:
[paste error]

Here is the relevant code:
[paste code]

This may involve feature flags or cross-crate types. Explain the failure.
```

```
I have a lifetime error in relay-server. The code borrows from ProjectState
but must be 'static for tokio::spawn. Show me similar patterns in the codebase.
```

```
This integration test in tests/integration/ is failing:
[paste output]

It runs against a live Relay binary with Redis and Kafka. Help me debug it.
```

For debugger-friendly local runs:

```

cargo run --all-features --profile dev-debug — run
```

## Recommended Workflow

1. Understand the relevant crate and code path first
2. Use `rg` and AI to find existing patterns
3. Let AI draft code, but review every line
4. Run `make check` and at least `make test-rust-all`
5. Run `cargo insta review` after schema changes
6 Add changelog entries
6. Review security impact before opening the PR

AI is useful for speed, but not for ownership. In Relay, correctness, safety, and maintainability matter more than how the code was written.
