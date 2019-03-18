//! Utility code for sentry's internal store.
use std::collections::BTreeSet;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::processor::{ProcessingState, Processor};
use crate::protocol::{Event, IpAddr, GroupingConfig};
use crate::types::{Meta, ValueAction, Annotated};

mod event_error;
mod geo;
mod normalize;
mod remove_other;
mod schema;
mod trimming;

pub use crate::store::geo::GeoIpLookup;

/// The config for the grouping config in store
#[derive(Serialize, Deserialize, Debug, Default)]
#[serde(default)]
pub struct StoreGroupingConfig {
    /// The id of the grouping algorithm that should be used.
    pub id: String,
}

impl StoreGroupingConfig {
    /// Converts this config into one the event structure supports.
    pub fn as_grouping_config(&self) -> GroupingConfig {
        GroupingConfig {
            id: Annotated::from(self.id.clone()),
        }
    }
}

/// The config for store.
#[derive(Serialize, Deserialize, Debug, Default)]
#[serde(default)]
pub struct StoreConfig {
    pub project_id: Option<u64>,
    pub client_ip: Option<IpAddr>,
    pub client: Option<String>,
    pub key_id: Option<String>,
    pub protocol_version: Option<String>,
    pub grouping_config: Option<StoreGroupingConfig>,

    /// Hard limit for stacktrace frames
    /// Corresponds to SENTRY_STACKTRACE_FRAMES_HARD_LIMIT
    pub stacktrace_frames_hard_limit: Option<usize>,

    /// Soft limit for stacktrace frames
    /// Corresponds to SENTRY_MAX_STACKTRACE_FRAMES
    pub max_stacktrace_frames: Option<usize>,

    pub valid_platforms: BTreeSet<String>,
    pub max_secs_in_future: Option<i64>,
    pub max_secs_in_past: Option<i64>,
    pub enable_trimming: Option<bool>,

    /// When `true`, it is assumed the input already ran through normalization with
    /// is_renormalize=false. `None` equals false.
    pub is_renormalize: Option<bool>,
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
    ) -> ValueAction {
        if self.config.is_renormalize.unwrap_or(false) {
            return ValueAction::Keep;
        }

        ValueAction::Keep
            // Check for required and non-empty values
            .and_then(|| schema::SchemaProcessor.process_event(event, meta, state))
            // Normalize data in all interfaces
            .and_then(|| self.normalize.process_event(event, meta, state))
            // Remove unknown attributes at every level
            .and_then(|| remove_other::RemoveOtherProcessor.process_event(event, meta, state))
            // Add event errors for top-level keys
            .and_then(|| event_error::EmitEventErrors::new().process_event(event, meta, state))
            // Trim large strings and databags down
            .and_then(|| match self.config.enable_trimming {
                Some(false) => ValueAction::Keep,
                _ => trimming::TrimmingProcessor::new().process_event(event, meta, state),
            })
    }
}
