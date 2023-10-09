use relay_statsd::TimerMetric;

pub enum Timers {
    /// Measures how log normalization of SQL queries in span description take.
    ///
    /// This metric is tagged with:
    ///  - `mode`: The method used for normalization (either `parser` or `regex`).
    SpanDescriptionNormalizeSQL,
}

impl TimerMetric for Timers {
    fn name(&self) -> &'static str {
        match *self {
            Self::SpanDescriptionNormalizeSQL => "normalize.span.description.sql",
        }
    }
}
