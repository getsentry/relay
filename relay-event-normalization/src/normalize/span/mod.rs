//! Span normalization logic.

use once_cell::sync::Lazy;
use regex::Regex;

pub mod ai;
pub mod country_subregion;
pub mod description;
pub mod exclusive_time;
pub mod tag_extraction;

/// Regex used to scrub hex IDs and multi-digit numbers from table names and other identifiers.
pub static TABLE_NAME_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r"(?ix)
        [0-9a-f]{8}_[0-9a-f]{4}_[0-9a-f]{4}_[0-9a-f]{4}_[0-9a-f]{12} |
        [0-9a-f]{8,} |
        \d\d+
        ",
    )
    .unwrap()
});
