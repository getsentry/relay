//! Normalizations specific to trace metrics.

use std::{borrow::Cow, sync::LazyLock};

use regex::Regex;
use relay_event_schema::protocol::TraceMetric;

/// Returned by [`normalize_metric_name`].
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[error("Metric has an invalid name")]
pub struct InvalidMetricName;

/// Normalizes a trace metric name.
///
/// Metric names cannot be empty, must only consist of ASCII alphanumerics, underscores, dashes, and periods.
/// The implementation will replace dashes with underscores.
///
/// Empty metric names are rejected with [`InvalidMetricName`].
pub fn normalize_metric_name(metric: &mut TraceMetric) -> Result<(), InvalidMetricName> {
    static NORMALIZE_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new("[^a-zA-Z0-9_.]+").unwrap());

    let name = metric.name.value_mut().as_mut();
    let Some(name) = name.filter(|s| !s.trim().is_empty()) else {
        return Err(InvalidMetricName);
    };

    if let Cow::Owned(new_name) = NORMALIZE_RE.replace_all(name, "_") {
        *name = new_name;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use relay_protocol::Annotated;

    use super::*;

    fn metric(name: impl Into<String>) -> TraceMetric {
        TraceMetric {
            name: Annotated::new(name.into()),
            ..Default::default()
        }
    }

    macro_rules! assert_metric_name {
        ($name:expr, err) => {{
            assert_eq!(
                normalize_metric_name(&mut metric($name)),
                Err(InvalidMetricName)
            )
        }};
        ($name:expr, $expected:expr) => {{
            let mut metric = metric($name);
            assert_eq!(normalize_metric_name(&mut metric), Ok(()));
            let name = metric.name.value_mut().as_mut().unwrap();
            assert_eq!(name, $expected);
        }};
    }

    #[test]
    fn test_normalize_name_invalid() {
        assert_metric_name!("", err);
        assert_metric_name!("     ", err);
    }

    #[test]
    fn test_normalize_metric_name() {
        assert_metric_name!("foo.bar123", "foo.bar123");
        assert_metric_name!("foo bar", "foo_bar");
        assert_metric_name!("foo!@#bar", "foo_bar");
        assert_metric_name!("   foo.bar    ", "_foo.bar_");
    }
}
