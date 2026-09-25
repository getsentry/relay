use relay_statsd::TimerMetric;

/// Timer metrics used for filters.
pub enum FilterTimers {
    /// Timing in milliseconds for applying filters to an item.
    ///
    /// This metric is tagged with:
    /// - `item`: The item being filtered.
    ShouldFilter,
}

impl TimerMetric for FilterTimers {
    fn name(&self) -> &'static str {
        match self {
            FilterTimers::ShouldFilter => "filter.should_filter",
        }
    }
}
