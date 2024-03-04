//! Break down the cost of Relay by its components and individual features it handles.
//!
//! Relay is a one stop shop for all different kinds of events Sentry supports, Errors,
//! Performance, Metrics, Replays, Crons and more. A single shared resource.
//!
//! This module intends to make it possible to give insights how much time and resources
//! Relay spends processing individual features. The measurements collected can be later used
//! for increased observability and accounting purposes.
//!
//! `relay-cogs` provides a way to give an answer to the questions:
//!  - What portion of Relay's costs can be attributed to feature X?
//!  - How much does feature X cost?
//!
//! ## Collecting COGs Measurements
//!
//! Measurements are collected through [`Cogs`] which attributes the measurement to either a single
//! or to multiple different [app features](AppFeature) belonging to a [resource](ResourceId).
//!
//! Collected and [attributed measurements](CogsMeasurement) then are recorded by a [`CogsRecorder`].
//!
//! ```
//! use relay_cogs::{AppFeature, Cogs, FeatureWeights, ResourceId};
//!
//! enum Message {
//!     Span,
//!     Transaction,
//!     TransactionWithSpans { num_spans: usize },
//! }
//!
//! struct Processor {
//!     cogs: Cogs
//! }
//!
//! impl From<&Message> for FeatureWeights {
//!     fn from(value: &Message) -> Self {
//!         match value {
//!             Message::Span => FeatureWeights::new(AppFeature::Spans),
//!             Message::Transaction => FeatureWeights::new(AppFeature::Transactions),
//!             Message::TransactionWithSpans { num_spans } => FeatureWeights::builder()
//!                 .weight(AppFeature::Spans, *num_spans)
//!                 .weight(AppFeature::Transactions, 1)
//!                 .build(),
//!         }
//!     }
//! }
//!
//! impl Processor {
//!     fn handle_message(&self, mut message: Message) {
//!         let _cogs = self.cogs.timed(ResourceId::Relay, &message);
//!
//!         self.step1(&mut message);
//!         self.step2(&mut message);
//!
//!         // Measurement automatically recorded here.
//!     }
//! #   fn step1(&self, _: &mut Message) {}
//! #   fn step2(&self, _: &mut Message) {}
//! }
//! ```
#![warn(missing_docs)]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png",
    html_favicon_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png"
)]

use std::time::Duration;

mod cogs;
mod recorder;
#[cfg(test)]
mod test;

pub use cogs::*;
pub use recorder::*;
#[cfg(test)]
pub use test::*;

/// Resource ID as tracked in COGS.
///
/// Infrastructure costs are labeled with a resource id,
/// these costs need to be broken down further by the application
/// by [app features](AppFeature).
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
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
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
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
    /// Returns the string representation for this app feature.
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

/// A COGS measurement.
///
/// The measurement has already been attributed to a specific feature.
#[derive(Debug, Clone, Copy)]
pub struct CogsMeasurement {
    /// The measured resource.
    pub resource: ResourceId,
    /// The measured app feature.
    pub feature: AppFeature,
    /// The measurement value.
    pub value: Value,
}

/// A COGS measurement value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Value {
    /// A time measurement.
    Time(Duration),
}
