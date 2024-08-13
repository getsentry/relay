//! Binary glob pattern matching for the C-ABI.

use std::borrow::Cow;
use std::num::NonZeroUsize;

use globset::GlobBuilder;
use lru::LruCache;
use once_cell::sync::Lazy;
use regex::bytes::{Regex, RegexBuilder};
use std::sync::{Mutex, PoisonError};

use crate::{RelayBuf, RelayStr};

/// Controls the globbing behaviors.
#[repr(u32)]
pub enum GlobFlags {
    /// When enabled `**` matches over path separators and `*` does not.
    DoubleStar = 1,
    /// Enables case insensitive path matching.
    CaseInsensitive = 2,
    /// Enables path normalization.
    PathNormalize = 4,
    /// Allows newlines.
    AllowNewline = 8,
}

/// Performs a glob operation on bytes.
///
/// Returns `true` if the glob matches, `false` otherwise.
#[no_mangle]
#[relay_ffi::catch_unwind]
pub unsafe extern "C" fn relay_is_glob_match(
    value: *const RelayBuf,
    pat: *const RelayStr,
    flags: GlobFlags,
) -> bool {
    let mut options = GlobOptions::default();
    let flags = flags as u32;
    if (flags & GlobFlags::DoubleStar as u32) != 0 {
        options.double_star = true;
    }
    if (flags & GlobFlags::CaseInsensitive as u32) != 0 {
        options.case_insensitive = true;
    }
    if (flags & GlobFlags::PathNormalize as u32) != 0 {
        options.path_normalize = true;
    }
    if (flags & GlobFlags::AllowNewline as u32) != 0 {
        options.allow_newline = true;
    }
    glob_match_bytes((*value).as_bytes(), (*pat).as_str(), options)
}

/// LRU cache for [`Regex`]s in relation to [`GlobOptions`] and the provided string pattern.
static GLOB_CACHE: Lazy<Mutex<LruCache<(GlobOptions, String), Regex>>> =
    Lazy::new(|| Mutex::new(LruCache::new(NonZeroUsize::new(500).unwrap())));

/// Controls the options of the globber.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
struct GlobOptions {
    /// When enabled `**` matches over path separators and `*` does not.
    pub double_star: bool,
    /// Enables case insensitive path matching.
    pub case_insensitive: bool,
    /// Enables path normalization.
    pub path_normalize: bool,
    /// Allows newlines.
    pub allow_newline: bool,
}

fn translate_pattern(pat: &str, options: GlobOptions) -> Option<Regex> {
    let mut builder = GlobBuilder::new(pat);
    builder.case_insensitive(options.case_insensitive);
    builder.literal_separator(options.double_star);
    let glob = builder.build().ok()?;
    RegexBuilder::new(glob.regex())
        .dot_matches_new_line(options.allow_newline)
        .build()
        .ok()
}

/// Performs a glob operation on bytes.
///
/// Returns `true` if the glob matches, `false` otherwise.
fn glob_match_bytes(value: &[u8], pat: &str, options: GlobOptions) -> bool {
    let (value, pat) = if options.path_normalize {
        (
            Cow::Owned(
                value
                    .iter()
                    .map(|&x| if x == b'\\' { b'/' } else { x })
                    .collect(),
            ),
            pat.replace('\\', "/"),
        )
    } else {
        (Cow::Borrowed(value), pat.to_string())
    };
    let key = (options, pat);
    let mut cache = GLOB_CACHE.lock().unwrap_or_else(PoisonError::into_inner);

    if let Some(pattern) = cache.get(&key) {
        pattern.is_match(&value)
    } else if let Some(pattern) = translate_pattern(&key.1, options) {
        let rv = pattern.is_match(&value);
        cache.put(key, pattern);
        rv
    } else {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn glob_match(value: &str, pat: &str, options: GlobOptions) -> bool {
        glob_match_bytes(value.as_bytes(), pat, options)
    }

    #[test]
    fn test_globs() {
        macro_rules! test_glob {
            ($value:expr, $pat:expr, $is_match:expr, {$($k:ident: $v:expr),*}) => {{
                #[allow(clippy::needless_update)]
                let options = GlobOptions { $($k: $v,)* ..Default::default() };
                assert!(
                    glob_match($value, $pat, options) == $is_match,
                    "expected that {} {} {} with options {:?}",
                    $pat,
                    if $is_match { "matches" } else { "does not match" },
                    $value,
                    &options,
                );
            }}
        }

        test_glob!("hello.py", "*.py", true, {});
        test_glob!("hello.py", "*.js", false, {});
        test_glob!("foo/hello.py", "*.py", true, {});
        test_glob!("foo/hello.py", "*.py", false, {double_star: true});
        test_glob!("foo/hello.py", "**/*.py", true, {double_star: true});
        test_glob!("foo/hello.PY", "**/*.py", false, {double_star: true});
        test_glob!("foo/hello.PY", "**/*.py", true, {double_star: true, case_insensitive: true});
        test_glob!("foo\\hello\\bar.PY", "foo/**/*.py", false, {double_star: true, case_insensitive: true});
        test_glob!("foo\\hello\\bar.PY", "foo/**/*.py", true, {double_star: true, case_insensitive: true, path_normalize: true});
        test_glob!("foo\nbar", "foo*", false, {});
        test_glob!("foo\nbar", "foo*", true, {allow_newline: true});
        test_glob!("1.18.4.2153-2aa83397b", "1.18.[0-4].*", true, {});

        let mut long_string = "x".repeat(1_000_000);
        long_string.push_str(".PY");
        test_glob!(&long_string, "*************************.py", true, {double_star: true, case_insensitive: true, path_normalize: true});
        test_glob!(&long_string, "*************************.js", false, {double_star: true, case_insensitive: true, path_normalize: true});
    }
}
