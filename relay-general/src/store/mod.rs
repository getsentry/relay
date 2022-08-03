//! Utility code for sentry's internal store.
use std::collections::BTreeSet;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::processor::{ProcessingState, Processor};
use crate::protocol::{Event, IpAddr};
use crate::types::{Annotated, Meta, ProcessingResult, SpanAttribute};
use normalize::light_normalize_event;
use transactions::validate_annotated_transaction;

mod clock_drift;
mod event_error;
mod geo;
mod legacy;
mod normalize;
mod remove_other;
mod schema;
mod transactions;
mod trimming;

pub use self::clock_drift::ClockDriftProcessor;
pub use self::geo::{GeoIpError, GeoIpLookup};
pub use normalize::breakdowns::{
    get_breakdown_measurements, BreakdownConfig, BreakdownsConfig, SpanOperationsConfig,
};
pub use normalize::{compute_measurements, is_valid_platform, normalize_dist};
pub use transactions::{
    get_measurement, get_transaction_op, is_high_cardinality_sdk, validate_timestamps,
    validate_transaction,
};

/// The config for store.
#[derive(Serialize, Deserialize, Debug, Default)]
#[serde(default)]
pub struct StoreConfig {
    pub project_id: Option<u64>,
    pub client_ip: Option<IpAddr>,
    pub client: Option<String>,
    pub key_id: Option<String>,
    pub protocol_version: Option<String>,
    pub grouping_config: Option<Value>,
    pub user_agent: Option<String>,
    pub received_at: Option<DateTime<Utc>>,
    pub sent_at: Option<DateTime<Utc>>,

    pub max_secs_in_future: Option<i64>,
    pub max_secs_in_past: Option<i64>,
    pub enable_trimming: Option<bool>,

    /// When `true`, it is assumed the input already ran through normalization with
    /// is_renormalize=false. `None` equals false.
    pub is_renormalize: Option<bool>,

    /// Overrides the default flag for other removal.
    pub remove_other: Option<bool>,

    /// When `true` it adds context information extracted from the user agent
    pub normalize_user_agent: Option<bool>,

    /// Emit breakdowns based on given configuration.
    pub breakdowns: Option<normalize::breakdowns::BreakdownsConfig>,

    /// Emit additional span attributes based on given configuration.
    pub span_attributes: BTreeSet<SpanAttribute>,

    /// The SDK's sample rate as communicated via envelope headers.
    pub client_sample_rate: Option<f64>,
}

/// The processor that normalizes events for store.
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
        let enable_trimming = self.config.enable_trimming.unwrap_or(true);

        // Convert legacy data structures to current format
        legacy::LegacyProcessor.process_event(event, meta, state)?;

        if !is_renormalize {
            // internally noops for non-transaction events
            // TODO: Parts of this processor should probably be a filter once Relay is store so we
            // can revert some changes to ProcessingAction
            transactions::TransactionsProcessor.process_event(event, meta, state)?;
        }

        if !is_renormalize {
            // Check for required and non-empty values
            schema::SchemaProcessor.process_event(event, meta, state)?;

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

        if enable_trimming {
            // Trim large strings and databags down
            trimming::TrimmingProcessor::new().process_event(event, meta, state)?;
        }

        Ok(())
    }
}

pub fn light_normalize<'a>(
    event: &mut Annotated<Event>,
    client_ip: Option<&IpAddr>,
    user_agent: Option<&'a str>,
    received_at: Option<DateTime<Utc>>,
    max_secs_in_past: Option<i64>,
    max_secs_in_future: Option<i64>,
    breakdowns_config: Option<BreakdownsConfig>,
) -> ProcessingResult {
    validate_annotated_transaction(event)?;
    light_normalize_event(
        event,
        client_ip,
        user_agent,
        received_at,
        max_secs_in_past,
        max_secs_in_future,
        breakdowns_config,
    )?;
    Ok(())
}
