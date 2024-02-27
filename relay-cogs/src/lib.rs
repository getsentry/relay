mod cogs;
mod recorder;
mod utils;

use std::time::Duration;

pub use cogs::*;
pub use recorder::*;

/// Resource ID as tracked in COGS.
///
/// Infrastructure costs are labeled with a resource id,
/// these costs need to be broken down further by the application
/// by [app features](AppFeature).
#[derive(Copy, Clone, Debug)]
pub enum ResourceId {
    /// The Relay resource.
    ///
    /// This includes all computational costs required for running Relay.
    Relay,
}

/// App feature a COGS measurement is related to.
///
/// App features break down the cost of a [`ResourceId`], the
/// app features do no need to directly match a Sentry product.
/// Multiple app features are later grouped and aggregated to determine
/// the cost of a product.
#[derive(Copy, Clone, Debug)]
pub enum AppFeature {
    /// A placeholder which should not be emitted but can be emitted in rare cases,
    /// for example error scenarios.
    ///
    /// It can be useful to start a COGS measurement before it is known
    /// what the measurement should be attributed to.
    /// For example when parsing data, the measurement should be started
    /// before parsing, but only after parsing it is known what to attribute
    /// the measurement to.
    Unattributed,

    /// Metrics are attributed by their namespace, whenever this is not possible
    /// or feasible, this app feature is emitted instead.
    UnattributedMetrics,
    /// When processing an envelope cannot be attributed or is not feasible to be attributed
    /// to a more specific category, this app feature is emitted instead.
    UnattributedEnvelope,

    /// Transactions.
    Transactions,
    /// Errors.
    Errors,
    /// Spans.
    Spans,
    /// Sessions.
    Sessions,
    /// Client reports.
    ClientReports,
    /// Crons check ins.
    CheckIns,
    /// Replays.
    Replays,

    /// Metric metadata.
    MetricMeta,

    /// Metrics in the transactions namespace.
    MetricsTransactions,
    /// Metrics in the spans namespace.
    MetricsSpans,
    /// Metrics in the sessions namespace.
    MetricsSessions,
    /// Metrics in the custom namespace.
    MetricsCustom,
    /// Metrics in the unsupported namespace.
    ///
    /// This is usually not emitted, since metrics in the unsupported
    /// namespace should be dropped before any processing occurs.
    MetricsUnsupported,
}

impl AppFeature {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Unattributed => "unattributed",
            Self::UnattributedMetrics => "unattributed_metrics",
            Self::UnattributedEnvelope => "unattributed_envelope",
            Self::Transactions => "transactions",
            Self::Errors => "errors",
            Self::Spans => "spans",
            Self::Sessions => "sessions",
            Self::ClientReports => "client_reports",
            Self::CheckIns => "check_ins",
            Self::Replays => "replays",
            Self::MetricMeta => "metric_meta",
            Self::MetricsTransactions => "metrics_transactions",
            Self::MetricsSpans => "metrics_spans",
            Self::MetricsSessions => "metrics_sessions",
            Self::MetricsCustom => "metrics_cusomt",
            Self::MetricsUnsupported => "metrics_unsupported",
        }
    }
}

impl utils::Enum<16> for AppFeature {
    fn from_index(index: usize) -> Option<Self> {
        let r = match index {
            0 => Self::Unattributed,
            1 => Self::UnattributedMetrics,
            2 => Self::UnattributedEnvelope,
            3 => Self::Transactions,
            4 => Self::Errors,
            5 => Self::Spans,
            6 => Self::Sessions,
            7 => Self::ClientReports,
            8 => Self::CheckIns,
            9 => Self::Replays,
            10 => Self::MetricMeta,
            11 => Self::MetricsTransactions,
            12 => Self::MetricsSpans,
            13 => Self::MetricsSessions,
            14 => Self::MetricsCustom,
            15 => Self::MetricsUnsupported,
            _ => return None,
        };

        Some(r)
    }

    fn to_index(value: Self) -> usize {
        match value {
            Self::Unattributed => 0,
            Self::UnattributedMetrics => 1,
            Self::UnattributedEnvelope => 2,
            Self::Transactions => 3,
            Self::Errors => 4,
            Self::Spans => 5,
            Self::Sessions => 6,
            Self::ClientReports => 7,
            Self::CheckIns => 8,
            Self::Replays => 9,
            Self::MetricMeta => 10,
            Self::MetricsTransactions => 11,
            Self::MetricsSpans => 12,
            Self::MetricsSessions => 13,
            Self::MetricsCustom => 14,
            Self::MetricsUnsupported => 15,
        }
    }
}

// A COGS measurement.
#[derive(Debug)]
pub struct CogsMeasurement {
    /// The measured resource.
    pub resource: ResourceId,
    /// The measured app feature.
    pub feature: AppFeature,
    /// The measurement value.
    pub value: Value,
}

/// A COGS measurement value.
#[derive(Debug)]
pub enum Value {
    /// A time measurement.
    Time(Duration),
}

#[cfg(test)]
mod tests {
    use crate::utils::Enum;

    use super::*;

    #[test]
    fn test_app_feature() {
        for i in 0.. {
            let Some(f) = AppFeature::from_index(i) else {
                assert_eq!(i, AppFeature::LENGTH);
                break;
            };
            assert_eq!(i, AppFeature::to_index(f));
        }
    }
}
