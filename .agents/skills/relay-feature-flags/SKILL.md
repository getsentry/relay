---
name: relay-feature-flags
description: Check changes to the `Feature` enum in `relay-dynamic-config/src/feature.rs` against Relay's feature flag conventions, namely the "Serialized as" doc line, `#[doc(hidden)]` on deprecated features, and in-use features declared before deprecated ones. Use when reviewing a diff or PR that adds, changes, deprecates, or moves a feature flag.
allowed-tools: Read Grep Glob
---

# Relay Feature Flag Conventions

Check the `Feature` enum in `relay-dynamic-config/src/feature.rs` against the three conventions below.

## Scope

1. Read the whole `relay-dynamic-config/src/feature.rs`, not only the changed hunk. The ordering rule needs the full enum.
2. Report only violations on variants that the change adds, edits, moves, or deprecates, or violations that the change causes, such as an in-use feature added after a deprecated one. Do not report violations on untouched variants.
3. Skip the forward compatibility variant marked `#[serde(other)]` (`Unknown`). It has no serialized name and stays the last variant.

## Deprecated features

A feature is deprecated when any of these is true:

- Its variant name starts with `Deprecated`.
- It is listed in `GRADUATED_FEATURE_FLAGS` in the same file.
- Its doc comment says it graduated, is deprecated, is now the default behaviour, or is kept only for external or outdated Relays.

Every other feature is in use. `#[doc(hidden)]` alone does not make a feature deprecated.

## Conventions

### 1. Serialized name in the doc comment

Every feature's doc comment ends with a paragraph of this form, separated from the text above by an empty `///` line:

```rust
    /// Enable continuous profiling.
    ///
    /// Serialized as `organizations:continuous-profiling`.
    #[serde(rename = "organizations:continuous-profiling")]
    ContinuousProfiling,
```

The value must match the name serde uses for the variant: the `#[serde(rename = "...")]` value, or the `rename`/`rename_all` rule that applies. Report a missing line, a missing doc comment, and a value that differs from the serde name, such as after a rename that did not update the doc.

### 2. Deprecated features are hidden

Every deprecated feature has `#[doc(hidden)]`:

```rust
    /// This feature has graduated and is hard-coded for external Relays.
    ///
    /// Serialized as `organizations:profiling`.
    #[doc(hidden)]
    #[serde(rename = "organizations:profiling")]
    DeprecatedProfiling,
```

### 3. In-use features before deprecated features

All in-use features come before all deprecated features. New in-use features go after the last in-use feature, not at the end of the enum. A feature that becomes deprecated moves to the deprecated section. Within each section, keep the existing relative order.

Moving variants is safe: `Feature` derives `Ord`, which only changes the order in which a `FeatureSet` serializes its flags, and the serde names stay the same.

## Reporting

Report each violation as **low** severity. Name the variant, the convention it breaks, and the fix, for example the exact `Serialized as` line to add or where to move the variant. Do not report style issues outside these three conventions.
