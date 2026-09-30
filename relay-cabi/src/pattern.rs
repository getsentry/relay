//! Relay pattern matching for the C-ABI.

use relay_pattern::Pattern;

use crate::RelayStr;

/// A Relay pattern.
pub struct RelayPattern;

/// Represents a collection of compiled Relay patterns with shared options.
pub struct RelayPatterns;

/// Creates a new Relay [`Pattern`].
#[unsafe(no_mangle)]
#[relay_ffi::catch_unwind]
pub unsafe extern "C" fn relay_pattern_new(
    pattern: *const RelayStr,
    case_insensitive: bool,
    max_complexity: u64,
) -> *mut RelayPattern {
    let pattern = Pattern::builder(unsafe { (*pattern).as_str() })
        .case_insensitive(case_insensitive)
        .max_complexity(max_complexity)
        .build()?;
    Box::into_raw(Box::new(pattern)) as *mut RelayPattern
}

/// Returns `true` if the pattern matches the UTF-8 string.
#[unsafe(no_mangle)]
#[relay_ffi::catch_unwind]
pub unsafe extern "C" fn relay_pattern_is_match(
    pattern: *const RelayPattern,
    value: *const RelayStr,
) -> bool {
    let pattern = unsafe { &*(pattern as *const Pattern) };
    pattern.is_match(unsafe { (*value).as_str() })
}

/// Formats the pattern using its `Display` implementation.
///
/// The returned string is newly allocated and must be freed with `relay_str_free`.
#[unsafe(no_mangle)]
#[relay_ffi::catch_unwind]
pub unsafe extern "C" fn relay_pattern_to_string(pattern: *const RelayPattern) -> RelayStr {
    let pattern = unsafe { &*(pattern as *const Pattern) };
    RelayStr::from_string(pattern.to_string())
}

/// Frees a compiled Relay pattern.
#[unsafe(no_mangle)]
#[relay_ffi::catch_unwind]
pub unsafe extern "C" fn relay_pattern_free(pattern: *mut RelayPattern) {
    if !pattern.is_null() {
        drop(unsafe { Box::from_raw(pattern as *mut Pattern) });
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        RelayErrorCode, relay_err_clear, relay_err_get_last_code, relay_err_get_last_message,
    };

    use super::*;

    macro_rules! test_pattern {
        ($pattern:expr, $haystack:expr, $is_match:expr) => {{
            test_pattern!($pattern, $haystack, $is_match, i:false)
        }};
        ($pattern:expr, $haystack:expr, $is_match:expr, i:$case_insensitive:expr) => {{
            let pattern = unsafe { relay_pattern_new(&RelayStr::new($pattern), $case_insensitive, u64::MAX) };
            // On panic this leaks memory, but we're in a test and accept that.
            assert!(!pattern.is_null());
            assert_eq!(
                unsafe { relay_pattern_is_match(pattern, &RelayStr::new($haystack)) },
                $is_match,
            );
            unsafe { relay_pattern_free(pattern) };
        }};
    }

    #[test]
    fn test_pattern_case_sensitive() {
        test_pattern!("*.{js,py}", "src/hello.py", true);
        test_pattern!("*.{js,py}", "src/hello.rs", false);
        test_pattern!("*.py", "hello.py.bak", false);
        test_pattern!("h?llo", "héllo", true);
        test_pattern!("[a-z]*", "hello", true);
        test_pattern!("[a-z]*", "123", false);
        test_pattern!("*", "hello\nworld", true);
        test_pattern!("", "", false);
        test_pattern!("*", "", true);
    }

    #[test]
    fn test_pattern_case_insensitive() {
        test_pattern!("*.{js,PY}", "src/hello.py", true, i:true);
        test_pattern!("*.{js,PY}", "src/hello.PY", true, i:true);
        test_pattern!("*.{js,PY}", "src/hello.JS", true, i:true);
        test_pattern!("*.{js,PY}", "src/hello.js", true, i:true);
        test_pattern!("", "", false, i:true);
        test_pattern!("*", "", true, i:true);
    }

    #[test]
    fn test_pattern_to_string() {
        let pattern = unsafe { relay_pattern_new(&RelayStr::new("Foo**"), true, u64::MAX) };
        let mut formatted = unsafe { relay_pattern_to_string(pattern) };
        unsafe { relay_pattern_free(pattern) };

        assert_eq!(unsafe { formatted.as_str() }, "foo*");
        unsafe { formatted.free() };
    }

    #[test]
    fn test_invalid_patterns() {
        relay_err_clear();

        let result = unsafe { relay_pattern_new(&RelayStr::new("["), false, u64::MAX) };
        assert!(result.is_null());
        assert!(matches!(
            relay_err_get_last_code(),
            RelayErrorCode::PatternError
        ));

        let mut message = relay_err_get_last_message();
        assert!(unsafe { message.as_str() }.contains("Unbalanced character class"));
        unsafe { message.free() };
        relay_err_clear();
    }
}
