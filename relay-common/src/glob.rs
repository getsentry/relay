//! Helper function to deal with the glob pattern matching.

use std::borrow::Cow;
use std::num::NonZeroUsize;

use globset::GlobBuilder;
use lru::LruCache;
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use regex::bytes::{Regex, RegexBuilder};

/// LRU cache for [`Regex`]s in relation to [`GlobOptions`] and the provided string pattern.
static GLOB_CACHE: Lazy<Mutex<LruCache<(GlobOptions, String), Regex>>> =
    Lazy::new(|| Mutex::new(LruCache::new(NonZeroUsize::new(500).unwrap())));

/// LRU cache for [`Regex`]s in relation to provided string pattern.
static CODEOWNERS_CACHE: Lazy<Mutex<LruCache<String, Regex>>> =
    Lazy::new(|| Mutex::new(LruCache::new(NonZeroUsize::new(500).unwrap())));

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

fn translate_codeowners_pattern(pattern: &str) -> Option<Regex> {
    let mut regex = String::new();

    // Special case backslash can match a backslash file or directory
    if pattern.starts_with('\\') {
        return Regex::new(r"\\(?:\z|/)").ok();
    }

    let anchored = pattern
        .find('/')
        .map_or(false, |pos| pos != pattern.len() - 1);

    if anchored {
        regex += r"\A";
    } else {
        regex += r"(?:\A|/)";
    }

    let matches_dir = pattern.ends_with('/');
    let mut pattern = pattern;
    if matches_dir {
        pattern = pattern.trim_end_matches('/');
    }

    // patterns ending with "/*" are special. They only match items directly in the directory
    // not deeper
    let trailing_slash_star = pattern.len() > 1 && pattern.ends_with("/*");

    let pattern_vec: Vec<char> = pattern.chars().collect();

    let mut num_to_skip = None;
    for (i, &ch) in pattern_vec.iter().enumerate() {
        // Anchored paths may or may not start with a slash
        if i == 0 && anchored && pattern.starts_with('/') {
            regex += r"/?";
            continue;
        }

        if let Some(skip_amount) = num_to_skip {
            num_to_skip = Some(skip_amount - 1);
            // Prevents everything after the * in the pattern from being skipped
            if num_to_skip > Some(0) {
                continue;
            }
        }

        if ch == '*' {
            // Handle double star (**) case properly
            if pattern_vec.get(i + 1) == Some(&'*') {
                let left_anchored = i == 0;
                let leading_slash = i > 0 && pattern_vec.get(i - 1) == Some(&'/');
                let right_anchored = i + 2 == pattern.len();
                let trailing_slash = pattern_vec.get(i + 2) == Some(&'/');

                if (left_anchored || leading_slash) && (right_anchored || trailing_slash) {
                    regex += ".*";
                    num_to_skip = Some(2);
                    // Allows the trailing slash after ** to be optional
                    if trailing_slash {
                        regex += "/?";
                        num_to_skip = Some(3);
                    }
                    continue;
                }
            }
            regex += r"[^/]*";
        } else if ch == '?' {
            regex += r"[^/]";
        } else {
            regex += &regex::escape(ch.to_string().as_str());
        }
    }

    if matches_dir {
        regex += "/";
    } else if trailing_slash_star {
        regex += r"\z";
    } else {
        regex += r"(?:\z|/)";
    }
    Regex::new(&regex).ok()
}

/// Returns `true` if the codeowners regex matches, `false` otherwise.
pub fn codeowners_match_bytes(value: &[u8], pat: &str) -> bool {
    let key = pat.to_string();

    let mut cache = CODEOWNERS_CACHE.lock();

    if let Some(pattern) = cache.get(&key) {
        pattern.is_match(value)
    } else if let Some(pattern) = translate_codeowners_pattern(&key) {
        let result = pattern.is_match(value);
        cache.put(key, pattern);
        result
    } else {
        false
    }
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

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn test_translate_codeowners_pattern() {
        // Matches "*.txt" anywhere it appears
        let pattern = "*.txt";
        let regex = translate_codeowners_pattern(pattern).unwrap();
        assert!(regex.is_match(b"file.txt"));
        assert!(regex.is_match(b"file.txt/"));
        assert!(regex.is_match(b"dir/file.txt"));

        // Matches leading "/dir/<x>.txt", where the leading / is optional and <x> is a single dir or filename
        let pattern = "/dir/*.txt";
        let regex = translate_codeowners_pattern(pattern).unwrap();
        assert!(regex.is_match(b"/dir/file.txt"));
        assert!(regex.is_match(b"dir/file.txt"));
        assert!(!regex.is_match(b"/dir/subdir/file.txt"));

        // Matches "apps/" anywhere it appears
        let pattern = "apps/";
        let regex = translate_codeowners_pattern(pattern).unwrap();
        assert!(regex.is_match(b"apps/file.txt"));
        assert!(regex.is_match(b"/apps/file.txt"));
        assert!(regex.is_match(b"/dir/apps/file.txt"));
        assert!(regex.is_match(b"/dir/subdir/apps/file.txt"));

        // Matches leading "docs/<x>", where <x> is a single dir or filename
        let pattern = "docs/*";
        let regex = translate_codeowners_pattern(pattern).unwrap();
        assert!(regex.is_match(b"docs/getting-started.md"));
        assert!(!regex.is_match(b"docs/build-app/troubleshooting.md"));
        assert!(!regex.is_match(b"something/docs/build-app/troubleshooting.md"));

        // Matches leading "<x>/docs/", where <x> is a single directory
        let pattern = "*/docs/";
        let regex = translate_codeowners_pattern(pattern).unwrap();
        assert!(regex.is_match(b"first/docs/troubleshooting.md"));
        assert!(!regex.is_match(b"docs/getting-started.md"));
        assert!(!regex.is_match(b"first/second/docs/troubleshooting.md"));

        // Matches leading "docs/<x>/something/", where <x> is a single dir
        let pattern = "docs/*/something/";
        let regex = translate_codeowners_pattern(pattern).unwrap();
        assert!(regex.is_match(b"docs/first/something/troubleshooting.md"));
        assert!(!regex.is_match(b"something/docs/first/something/troubleshooting.md"));
        assert!(!regex.is_match(b"docs/first/second/something/getting-started.md"));

        // Matches leading "/docs/", where the leading / is optional
        let pattern = "/docs/";
        let regex = translate_codeowners_pattern(pattern).unwrap();
        assert!(regex.is_match(b"/docs/file.txt"));
        assert!(regex.is_match(b"/docs/subdir/file.txt"));
        assert!(regex.is_match(b"docs/subdir/file.txt"));
        assert!(!regex.is_match(b"app/docs/file.txt"));

        // Matches leading "/red/<x>/file.py", where the leading / is optional and <x> is 0 or more dirs
        let pattern = "/red/**/file.py";
        let regex = translate_codeowners_pattern(pattern).unwrap();
        assert!(regex.is_match(b"red/orange/yellow/green/file.py"));
        assert!(regex.is_match(b"/red/orange/file.py"));
        assert!(regex.is_match(b"red/file.py"));
        assert!(!regex.is_match(b"yellow/file.py"));

        // Matches leading "red/<x>/file.py", where <x> is 0 or more dirs
        let pattern = "red/**/file.py";
        let regex = translate_codeowners_pattern(pattern).unwrap();
        assert!(regex.is_match(b"red/orange/yellow/green/file.py"));
        assert!(regex.is_match(b"red/file.py"));
        assert!(!regex.is_match(b"something/red/file.py"));

        // Matches "<x>/yellow/file.py", where the leading / is optional and <x> is 0 or more dirs
        let pattern = "**/yellow/file.py";
        let regex = translate_codeowners_pattern(pattern).unwrap();
        assert!(regex.is_match(b"red/orange/yellow/file.py"));
        assert!(regex.is_match(b"yellow/file.py"));
        assert!(!regex.is_match(b"/docs/file.py"));

        // Matches "/red/orange/**"", where the leading / is optional and <x> is 0 or more dirs/filenames
        let pattern = "/red/orange/**";
        let regex = translate_codeowners_pattern(pattern).unwrap();
        assert!(regex.is_match(b"red/orange/yellow/file.py"));
        assert!(regex.is_match(b"red/orange/file.py"));
        assert!(!regex.is_match(b"blue/red/orange/file.py"));

        // Matches any ".css" file
        let pattern = "/**/*.css";
        let regex = translate_codeowners_pattern(pattern).unwrap();
        assert!(regex.is_match(b"/docs/subdir/file.css"));
        assert!(regex.is_match(b"file.css"));
        assert!(!regex.is_match(b"/docs/file.txt"));
    }
}
