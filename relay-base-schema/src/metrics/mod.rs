//! Type definitions for Sentry metrics.

mod mri;
mod name;
mod units;

pub use self::mri::*;
pub use self::name::*;
pub use self::units::*;

use regex::Regex;
use std::{borrow::Cow, sync::OnceLock};

// The limit is determined by the unit size (15) and the maximum MRI length (200).
// By choosing a metric name length of 150, we ensure that 35 characters remain available 
// for the MRI divider characters, metric type, and namespace, which is a reasonable allocation.
const CUSTOM_METRIC_NAME_MAX_SIZE: usize = 150;

/// Validates a metric name and normalizes it. This is the statsd name, i.e. without type or unit.
///
/// Metric names cannot be empty, must begin with a letter and can consist of ASCII alphanumerics,
/// underscores, dashes, and periods. The implementation will further replace dashes with
/// underscores.
///
/// The function validates that the first character of the metric must be ASCII alphanumeric, later
/// consecutive invalid characters in the name will be replaced with underscores.
pub fn try_normalize_metric_name(name: &str) -> Option<Cow<'_, str>> {
    static NORMALIZE_RE: OnceLock<Regex> = OnceLock::new();

    if !can_be_valid_metric_name(name) {
        return None;
    }

    // Note: `-` intentionally missing from this list.
    let normalize_re = NORMALIZE_RE.get_or_init(|| Regex::new("[^a-zA-Z0-9_.]+").unwrap());
    let normalized_name = normalize_re.replace_all(name, "_");

    if normalized_name.len() <= CUSTOM_METRIC_NAME_MAX_SIZE {
        return Some(normalized_name);
    }

    // We limit the string to a fixed size. Here we are taking slices assuming that we have a single
    // character per index since we are normalizing the name above.
    Some(match normalized_name {
        Cow::Borrowed(value) => Cow::Borrowed(&value[..CUSTOM_METRIC_NAME_MAX_SIZE]),
        Cow::Owned(mut value) => {
            value.truncate(CUSTOM_METRIC_NAME_MAX_SIZE);
            Cow::Owned(value)
        }
    })
}

/// Returns whether [`try_normalize_metric_name`] can normalize the passed name.
pub fn can_be_valid_metric_name(name: &str) -> bool {
    name.starts_with(|c: char| c.is_ascii_alphabetic())
}
