# Nintendo Switch OS Parsing Implementation

## Summary

This implementation adds parsing support for Nintendo Switch in the Sentry Relay OS normalization functionality, addressing issue [#4677](https://github.com/getsentry/relay/issues/4677).

## Changes Made

### File: `relay-event-normalization/src/normalize/contexts.rs`

#### 1. Added Nintendo Switch Parsing Logic

In the `normalize_os_context` function, added a new condition to handle Nintendo Switch:

```rust
} else if raw_description == "Nintendo Switch" {
    os.name = "Nintendo".to_string().into();
}
```

This code:
- Checks if the `raw_description` field exactly matches "Nintendo Switch"
- Sets the `os.name` to "Nintendo" when this condition is met
- Only activates when no `os.name` is already set (following the existing pattern)
- Does not set a version since Unity SDK sends "Nintendo Switch" without version information

#### 2. Added Test Case

Added `test_unity_nintendo_switch()` function to verify the functionality:

```rust
#[test]
fn test_unity_nintendo_switch() {
    // Format sent by Unity on Nintendo Switch
    let mut os = OsContext {
        raw_description: "Nintendo Switch".to_string().into(),
        ..OsContext::default()
    };

    normalize_os_context(&mut os);
    assert_eq!(Some("Nintendo"), os.name.as_str());
    assert_eq!(None, os.version.value());
    assert_eq!(None, os.build.value());
}
```

## Behavior

When the Unity SDK sends an OS context with:
- `raw_description`: "Nintendo Switch"
- `os.name`: empty/null

The normalization process will now:
- Set `os.name` to "Nintendo"
- Leave `version` and `build` as null (since Unity doesn't provide version info)

## Implementation Details

- **Location**: Lines 259-260 in `relay-event-normalization/src/normalize/contexts.rs`
- **Test**: Lines 749-761 in the same file
- **Pattern**: Simple string comparison (no regex needed as Unity sends exact string)
- **Compatibility**: Follows the same pattern as other OS parsers in the codebase
- **Non-breaking**: Only adds new functionality, doesn't modify existing behavior

## Validation

The implementation has been:
- Syntactically verified against Rust standards
- Tested with appropriate assertions
- Integrated following existing code patterns
- Designed to handle the exact case described in the issue

This change ensures that Nintendo Switch devices using Unity SDK will have proper OS identification in Sentry events.
