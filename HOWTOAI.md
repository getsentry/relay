# Contributing to Relay with AI Coding Assistants 🤖

Use AI to move faster, not to lower standards. You own every line you submit.

> "I, for one, welcome our new AI overlords." - Kent Brockman, probably talking about Relay contributions

## Ground Rules

- **You are responsible for the code.** Do not commit code you do not understand or cannot maintain.
- **AI output must meet the same standards as human output.** That includes tests, docs, changelog entries, and consistency with existing patterns.
- **Be transparent.** If AI generated a large part of the change, mention it in the PR and explain how you validated it.
- **Validate before opening a PR.** Run `make check` and `make test` first. AI-generated code often looks right but fails lint, tests, or full-feature builds.
- **Review security-sensitive changes carefully.** Be extra cautious in `relay-pii`, `relay-auth`, `relay-quotas`, `relay-event-normalization`, and network-facing code in `relay-server`.

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
6. Add changelog entries
7. Review security impact before opening the PR

AI is useful for speed, but not for ownership. In Relay, correctness, safety, and maintainability matter more than how the code was written.
