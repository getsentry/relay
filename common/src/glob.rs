use std::borrow::Cow;
use std::path::{Path, PathBuf};

use globset::GlobBuilder;
use lazy_static::lazy_static;
use lru::LruCache;
use parking_lot::Mutex;
use regex::bytes::Regex;

lazy_static! {
    static ref GLOB_CACHE: Mutex<LruCache<(GlobOptions, String), Regex>> =
        Mutex::new(LruCache::new(500));
}

/// Controls the options of the globber.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct GlobOptions {
    pub double_star: bool,
    pub case_insensitive: bool,
    pub path_normalize: bool,
}

impl Default for GlobOptions {
    fn default() -> GlobOptions {
        GlobOptions {
            double_star: false,
            case_insensitive: false,
            path_normalize: false,
        }
    }
}

fn translate_pattern(pat: &str, options: GlobOptions) -> Option<Regex> {
    let mut builder = GlobBuilder::new(pat);
    builder.case_insensitive(options.case_insensitive);
    builder.literal_separator(options.double_star);
    let glob = builder.build().ok()?;
    Regex::new(glob.regex()).ok()
}

/// Performs a glob operation on bytes.
///
/// Returns `true` if the glob matches.
pub fn glob_match_bytes(value: &[u8], pat: &str, options: GlobOptions) -> bool {
    let (value, pat) = if options.path_normalize {
        (Cow::Owned(value.replace('\\', "/")), pat.replace('\\', "/"))
    } else {
        (Cow::Borrowed(value), pat.to_string())
    };
    let key = (options, pat);
    let mut cache = GLOB_CACHE.lock();

    if let Some(pattern) = cache.get(&key) {
        pattern.is_match(value.as_bytes())
    } else if let Some(pattern) = translate_pattern(&key.1, options) {
        let rv = pattern.is_match(value.as_bytes());
        cache.put(key, pattern);
        rv
    } else {
        false
    }
}

/// Performs a glob operation.
///
/// Returns `true` if the glob matches.
pub fn glob_match(value: &str, pat: &str, options: GlobOptions) -> bool {
    glob_match_bytes(value.as_bytes(), pat, options: GlobOptions)
}

#[test]
fn test_globs() {
    macro_rules! test_glob {
        ($value:expr, $pat:expr, $is_match:expr, {$($k:ident: $v:expr),*}) => {{
            let options = GlobOptions { $($k: $v,)* ..Default::default() };
            assert_eq!(
                glob_match($value, $pat, options),
                $is_match,
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

    let mut long_string = std::iter::repeat('x').take(1000000).collect::<String>();
    long_string.push_str(".PY");
    test_glob!(&long_string, "*************************.py", true, {double_star: true, case_insensitive: true, path_normalize: true});
    test_glob!(&long_string, "*************************.js", false, {double_star: true, case_insensitive: true, path_normalize: true});
}
