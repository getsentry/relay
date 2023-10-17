//! Event normalization and processing.

#![warn(missing_docs)]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png",
    html_favicon_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png"
)]

use std::collections::BTreeSet;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use relay_event_schema::processor::{ProcessingResult, ProcessingState, Processor};
use relay_event_schema::protocol::{Event, IpAddr, SpanAttribute};
use relay_protocol::Meta;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

mod clock_drift;
mod event_error;
mod geo;
mod legacy;
mod normalize;
mod regexes;
mod remove_other;
pub mod replay;
mod schema;
mod statsd;
mod transactions;
mod trimming;

pub use normalize::breakdowns::*;
pub use normalize::*;
pub use transactions::*;
pub use user_agent::*;

pub use self::clock_drift::*;
pub use self::geo::*;

pub use sentry_release_parser::{validate_environment, validate_release};

/// Configuration for the [`StoreProcessor`].
#[derive(Serialize, Deserialize, Debug, Default)]
#[serde(default)]
pub struct StoreConfig {
    /// The identifier of the target project, which gets added to the payload.
    pub project_id: Option<u64>,

    /// The IP address of the SDK that sent the event.
    ///
    /// When `{{auto}}` is specified and there is no other IP address in the payload, such as in the
    /// `request` context, this IP address gets added to the `user` context.
    pub client_ip: Option<IpAddr>,

    /// The name and version of the SDK that sent the event.
    pub client: Option<String>,

    /// The internal identifier of the DSN, which gets added to the payload.
    ///
    /// Note that this is different from the DSN's public key. The ID is usually numeric.
    pub key_id: Option<String>,

    /// The version of the protocol.
    ///
    /// This is a deprecated field, as there is no more versioning of Relay event payloads.
    pub protocol_version: Option<String>,

    /// Configuration for issue grouping.
    ///
    /// This configuration is persisted into the event payload to achieve idempotency in the
    /// processing pipeline and for reprocessing.
    pub grouping_config: Option<Value>,

    /// The raw user-agent string obtained from the submission request headers.
    ///
    /// The user agent is used to infer device, operating system, and browser information should the
    /// event payload contain no such data.
    ///
    /// Newer browsers have frozen their user agents and send [`client_hints`](Self::client_hints)
    /// instead. If both a user agent and client hints are present, normalization uses client hints.
    pub user_agent: Option<String>,

    /// A collection of headers sent by newer browsers about the device and environment.
    ///
    /// Client hints are the preferred way to infer device, operating system, and browser
    /// information should the event payload contain no such data. If no client hints are present,
    /// normalization falls back to the user agent.
    pub client_hints: ClientHints<String>,

    /// The time at which the event was received in this Relay.
    ///
    /// This timestamp is persisted into the event payload.
    pub received_at: Option<DateTime<Utc>>,

    /// The time at which the event was sent by the client.
    ///
    /// The difference between this and the `received_at` timestamps is used for clock drift
    /// correction, should a significant difference be detected.
    pub sent_at: Option<DateTime<Utc>>,

    /// The maximum amount of seconds an event can be predated into the future.
    ///
    /// If the event's timestamp lies further into the future, the received timestamp is assumed.
    pub max_secs_in_future: Option<i64>,

    /// The maximum amount of seconds an event can be dated in the past.
    ///
    /// If the event's timestamp is older, the received timestamp is assumed.
    pub max_secs_in_past: Option<i64>,

    /// When `Some(true)`, individual parts of the event payload is trimmed to a maximum size.
    ///
    /// See the event schema for size declarations.
    pub enable_trimming: Option<bool>,

    /// When `Some(true)`, it is assumed that the event has been normalized before.
    ///
    /// This disables certain normalizations, especially all that are not idempotent. The
    /// renormalize mode is intended for the use in the processing pipeline, so an event modified
    /// during ingestion can be validated against the schema and large data can be trimmed. However,
    /// advanced normalizations such as inferring contexts or clock drift correction are disabled.
    ///
    /// `None` equals to `false`.
    pub is_renormalize: Option<bool>,

    /// Overrides the default flag for other removal.
    pub remove_other: Option<bool>,

    /// When `Some(true)`, context information is extracted from the user agent.
    pub normalize_user_agent: Option<bool>,

    /// Emit breakdowns based on given configuration.
    pub breakdowns: Option<normalize::breakdowns::BreakdownsConfig>,

    /// Emit additional span attributes based on given configuration.
    pub span_attributes: BTreeSet<SpanAttribute>,

    /// The SDK's sample rate as communicated via envelope headers.
    ///
    /// It is persisted into the event payload.
    pub client_sample_rate: Option<f64>,

    /// The identifier of the Replay running while this event was created.
    ///
    /// It is persisted into the event payload for correlation.
    pub replay_id: Option<Uuid>,
}

/// The processor that normalizes events for processing and storage.
///
/// This processor is a superset of [`light_normalize_event`], that runs additional and heavier
/// normalization steps. These normalizations should ideally be performed on events that are likely
/// to be ingested, after other functionality such as inbound filters have run.
///
/// See the fields of [`StoreConfig`] for a description of all normalization steps.
pub struct StoreProcessor<'a> {
    config: Arc<StoreConfig>,
    normalize: normalize::NormalizeProcessor<'a>,
}

impl<'a> StoreProcessor<'a> {
    /// Creates a new normalization processor.
    pub fn new(config: StoreConfig, geoip_lookup: Option<&'a GeoIpLookup>) -> Self {
        let config = Arc::new(config);
        StoreProcessor {
            normalize: normalize::NormalizeProcessor::new(config.clone(), geoip_lookup),
            config,
        }
    }

    /// Returns a reference to the config.
    pub fn config(&self) -> &StoreConfig {
        &self.config
    }
}

impl<'a> Processor for StoreProcessor<'a> {
    fn process_event(
        &mut self,
        event: &mut Event,
        meta: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ProcessingResult {
        let is_renormalize = self.config.is_renormalize.unwrap_or(false);
        let remove_other = self.config.remove_other.unwrap_or(!is_renormalize);

        // Convert legacy data structures to current format
        legacy::LegacyProcessor.process_event(event, meta, state)?;

        if !is_renormalize {
            // Normalize data in all interfaces
            self.normalize.process_event(event, meta, state)?;
        }

        if remove_other {
            // Remove unknown attributes at every level
            remove_other::RemoveOtherProcessor.process_event(event, meta, state)?;
        }

        if !is_renormalize {
            // Add event errors for top-level keys
            event_error::EmitEventErrors::new().process_event(event, meta, state)?;
        }

        Ok(())
    }
}
