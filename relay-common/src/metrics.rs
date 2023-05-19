/// Validates a metric name. This is the statsd name, i.e. without type or unit.
///
/// Metric names cannot be empty, must begin with a letter and can consist of ASCII alphanumerics,
/// underscores, slashes and periods.
pub fn is_valid_metric_name(name: &str) -> bool {
    let mut iter = name.as_bytes().iter();
    if let Some(first_byte) = iter.next() {
        if first_byte.is_ascii_alphabetic() {
            return iter.all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'/'));
        }
    }
    false
}
