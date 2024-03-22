//! Type definitions for Sentry metrics.

mod mri;
mod units;

pub use self::mri::*;
pub use self::units::*;

use regex::Regex;
use std::{borrow::Cow, sync::OnceLock};

/// Validates a metric name and normalizes it. This is the statsd name, i.e. without type or unit.
///
/// Metric names cannot be empty, must begin with a letter and can consist of ASCII alphanumerics,
/// underscores, dashes, slashes and periods.
///
/// The function validates that the first character of the metric must be ASCII alphanumeric,
/// later consecutive invalid characters in the name will be replaced with underscores.
pub fn try_normalize_metric_name(name: &str) -> Option<Cow<'_, str>> {
    static NORMALIZE_RE: OnceLock<Regex> = OnceLock::new();

    if !can_be_valid_metric_name(name) {
        return None;
    }

    let normalize_re = NORMALIZE_RE.get_or_init(|| Regex::new("[^a-zA-Z0-9_/.]+").unwrap());
    Some(normalize_re.replace_all(name, "_"))
}

/// Returns whether [`try_normalize_metric_name`] can normalize the passed name.
pub fn can_be_valid_metric_name(name: &str) -> bool {
    name.starts_with(|c: char| c.is_ascii_alphabetic())
}
