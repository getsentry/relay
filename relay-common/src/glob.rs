use std::borrow::Cow;

use globset::GlobBuilder;
use lazy_static::lazy_static;
use lru::LruCache;
use parking_lot::Mutex;
use regex::bytes::{Regex, RegexBuilder};

lazy_static! {
    static ref GLOB_CACHE: Mutex<LruCache<(GlobOptions, String), Regex>> =
        Mutex::new(LruCache::new(500));
}

/// Controls the options of the globber.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct GlobOptions {
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
pub fn glob_match_bytes(value: &[u8], pat: &str, options: GlobOptions) -> bool {
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
    let mut cache = GLOB_CACHE.lock();

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

/// Performs a glob operation.
///
/// Returns `true` if the glob matches.
///
/// Note that even though this accepts strings, the case insensitivity here is only
/// applied on ASCII characters as the underlying globber matches on bytes exclusively.
pub fn glob_match(value: &str, pat: &str, options: GlobOptions) -> bool {
    glob_match_bytes(value.as_bytes(), pat, options)
}

#[test]
#[allow(clippy::cognitive_complexity)]
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
