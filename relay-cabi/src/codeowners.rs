use std::num::NonZeroUsize;
use std::sync::Mutex;

use lru::LruCache;
use once_cell::sync::Lazy;
use regex::bytes::Regex;

use crate::core::{RelayBuf, RelayStr};

/// LRU cache for [`Regex`]s in relation to the provided string pattern.
static CODEOWNERS_CACHE: Lazy<Mutex<LruCache<String, Regex>>> =
    Lazy::new(|| Mutex::new(LruCache::new(NonZeroUsize::new(500).unwrap())));

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

/// Returns `true` if the codeowners path matches the value, `false` otherwise.
#[no_mangle]
#[relay_ffi::catch_unwind]
pub unsafe extern "C" fn relay_is_codeowners_path_match(
    value: *const RelayBuf,
    pattern: *const RelayStr,
) -> bool {
    let value = (*value).as_bytes();
    let pat = (*pattern).as_str();

    let mut cache = CODEOWNERS_CACHE.lock().unwrap();

    if let Some(pattern) = cache.get(pat) {
        pattern.is_match(value)
    } else if let Some(pattern) = translate_codeowners_pattern(pat) {
        let result = pattern.is_match(value);
        cache.put(pat.to_owned(), pattern);
        result
    } else {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
