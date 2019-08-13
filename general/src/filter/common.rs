use globset::GlobBuilder;

/// Pattern matching for event filters. This function intends to work roughly the same as Python's
/// `fnmatch`.
pub fn is_glob_match(pattern: &str, data: &str) -> bool {
    if pattern.contains('*') {
        // we have a pattern, try a glob match
        if let Ok(pattern) = GlobBuilder::new(pattern).case_insensitive(true).build() {
            let pattern = pattern.compile_matcher();
            return pattern.is_match(data);
        }
    }

    //if we don't use glob patterns just do a simple comparison
    pattern.to_lowercase() == data.to_lowercase()
}
