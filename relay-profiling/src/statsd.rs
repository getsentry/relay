use relay_statsd::DistributionMetric;

pub enum ProfilingDistributions {
    /// Number of events per successfully parsed legacy Android trace log.
    AndroidTraceLogEvents,
    /// Number of methods per successfully parsed legacy Android trace log.
    AndroidTraceLogMethods,
    /// Number of threads per successfully parsed legacy Android trace log.
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
