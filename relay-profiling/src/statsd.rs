use relay_statsd::DistributionMetric;

#[allow(clippy::enum_variant_names)]
pub enum ProfilingDistributions {
    /// Number of events per successfully parsed Android trace log.
    ///
    /// Tagged with `type`: `chunk` or `legacy`.
    AndroidTraceLogEvents,
    /// Number of methods per successfully parsed Android trace log.
    ///
    /// Tagged with `type`: `chunk` or `legacy`.
    AndroidTraceLogMethods,
    /// Number of threads per successfully parsed Android trace log.
    ///
    /// Tagged with `type`: `chunk` or `legacy`.
    AndroidTraceLogThreads,
}

impl DistributionMetric for ProfilingDistributions {
    fn name(&self) -> &'static str {
        match self {
            Self::AndroidTraceLogEvents => "profiling.android_trace_log.events",
            Self::AndroidTraceLogMethods => "profiling.android_trace_log.methods",
            Self::AndroidTraceLogThreads => "profiling.android_trace_log.threads",
        }
    }
}
