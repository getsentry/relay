# OS Context to Tag Propagation Analysis

This document analyzes when and how the OS name from OS context gets propagated into an `os.name` tag on error events in Sentry.

## Summary

The OS name from OS context gets propagated to tags in **two main scenarios**:

1. **Span Tag Extraction** - For transaction events and spans
2. **Transaction Metrics Extraction** - For transaction metrics (not directly for error events)

**Important finding**: There does **NOT** appear to be a direct mechanism that converts OS context to an `os.name` tag for general error events during normal event processing.

## Detailed Analysis

### 1. OS Context Normalization (All Events)

**File**: `relay-event-normalization/src/normalize/contexts.rs`
**Function**: `normalize_os_context()`

This happens during event normalization for ALL event types:

```rust
fn normalize_os_context(os: &mut OsContext) {
    if os.name.value().is_some() || os.version.value().is_some() {
        compute_os_context(os);
        return;
    }
    // ... parsing logic for various OS types ...
}
```

This function:
- Parses raw OS descriptions into structured `os.name`, `os.version`, etc.
- Handles Windows, macOS, iOS, Android, Linux distributions, etc.
- Sets the computed `os.os` field (combines name + version)
- **Does NOT create any tags** - only normalizes the context data

### 2. Span Tag Extraction (Transaction Events Only)

**File**: `relay-event-normalization/src/normalize/span/tag_extraction.rs`
**Function**: `extract_shared_tags()` (line ~340-375)

This happens when `config.enrich_spans` is enabled and **only for transaction events**:

```rust
if let Some(os_context) = event.context::<OsContext>() {
    if let Some(os_name) = os_context.name.value() {
        if tags.mobile.value().is_some_and(|v| v.as_str() == "true") {
            // For mobile spans, only extract os_name if app context exists
            if event.context::<AppContext>().is_some() {
                tags.os_name = os_name.to_string().into();
            }
        } else {
            // For non-mobile spans, always extract os_name
            tags.os_name = os_name.to_string().into();
        }
    }
}
```

**Key points**:
- Only runs when `config.enrich_spans` is true
- Only processes transaction events (`EventType::Transaction`)
- For mobile events, requires both OS context AND app context
- For non-mobile events, extracts OS name if OS context exists
- Creates `os_name` tag on spans

### 3. Transaction Metrics Extraction

**File**: `relay-server/src/metrics_extraction/transactions/mod.rs`
**Function**: `extract_os_name()` and `extract_universal_tags()`

```rust
fn extract_os_name(event: &Event) -> Option<String> {
    let os = event.context::<OsContext>()?;
    os.name.value().cloned()
}

fn extract_universal_tags(event: &Event, config: &TransactionMetricsConfig) -> CommonTags {
    // ...
    if let Some(os_name) = extract_os_name(event) {
        tags.insert(CommonTag::OsName, os_name);  // Maps to "os.name"
    }
    // ...
}
```

This creates `os.name` tags for **transaction metrics**, not event tags.

### 4. Special Case: PlayStation Processing

**File**: `relay-server/src/services/processor/playstation.rs`

For PlayStation events, OS tags are explicitly added:

```rust
add_tag!("os.name", "PlayStation");
```

### 5. Event Tag Processing (General Events)

**File**: `relay-event-normalization/src/event.rs`
**Function**: `normalize_event_tags()`

This function processes event tags but does **NOT** extract tags from contexts. It only:
- Removes internal tags
- Handles environment tag migration
- Validates tag format
- Adds server_name and site as tags

## Key Findings

1. **No Direct OS→Tag Conversion for Error Events**: Unlike `device.class` which gets extracted from device context into tags for all events, there's no equivalent mechanism for OS context.

2. **Span Processing Only**: OS name tag extraction primarily happens during span processing for transaction events, not for general error events.

3. **Transaction Metrics**: OS name is extracted for transaction metrics, but this doesn't create event tags.

4. **GitHub Issue Context**: This explains the issue #95684 - if an error event has OS context with `{"os": {"name": "Any"}}`, it won't automatically create an `os.name` tag because there's no general OS context→tag extraction for non-transaction events.

## Relevant Code Locations

- **OS Context Normalization**: `relay-event-normalization/src/normalize/contexts.rs:215`
- **Span Tag Extraction**: `relay-event-normalization/src/normalize/span/tag_extraction.rs:340-375`
- **Transaction Metrics**: `relay-server/src/metrics_extraction/transactions/mod.rs:59`
- **Event Normalization**: `relay-event-normalization/src/event.rs:336` (calls context normalization)
- **Tag Processing**: `relay-event-normalization/src/event.rs:639` (general event tag processing)

## Recommendation

To address the GitHub issue #95684, Sentry would need to add OS context→tag extraction logic similar to the existing `normalize_device_class()` function, but for OS context. This would need to be added to the general event processing pipeline, not just the span processing pipeline.