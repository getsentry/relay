//! Utility code for sentry's internal store.
use std::collections::BTreeSet;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::processor::{ProcessingState, Processor};
use crate::protocol::Event;
use crate::types::{Meta, ValueAction};

mod geo;
mod normalize;
mod remove_other;
mod schema;
mod trimming;

pub use crate::store::geo::GeoIpLookup;

/// The config for store.
#[derive(Serialize, Deserialize, Debug, Default)]
#[serde(default)]
pub struct StoreConfig {
    pub project_id: Option<u64>,
    pub client_ip: Option<String>,
    pub client: Option<String>,
    pub is_public_auth: bool,
    pub key_id: Option<String>,
    pub protocol_version: Option<String>,
    pub stacktrace_frames_hard_limit: Option<usize>,
    pub valid_platforms: BTreeSet<String>,
    pub max_secs_in_future: Option<i64>,
    pub max_secs_in_past: Option<i64>,
    pub enable_trimming: Option<bool>,
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
        ValueAction::Keep
            // Check for required and non-empty values
            .and_then(|| schema::SchemaProcessor.process_event(event, meta, state))
            // Normalize data in all interfaces
            .and_then(|| self.normalize.process_event(event, meta, state))
            // TODO: Compute hashes from fingerprints
            // Remove unknown attributes at every level
            .and_then(|| remove_other::RemoveOtherProcessor.process_event(event, meta, state))
            // Trim large strings and databags down
            .and_then(|| match self.config.enable_trimming {
                Some(false) => ValueAction::Keep,
                _ => trimming::TrimmingProcessor::new().process_event(event, meta, state),
            })
    }
}

#[cfg(test)]
use {crate::processor::process_value, crate::types::Annotated};

#[test]
fn test_schema_processor_invoked() {
    use crate::protocol::User;

    let mut event = Annotated::new(Event {
        user: Annotated::new(User {
            email: Annotated::new("bananabread".to_owned()),
            ..Default::default()
        }),
        ..Default::default()
    });

    let mut processor = StoreProcessor::new(StoreConfig::default(), None);
    process_value(&mut event, &mut processor, ProcessingState::root());

    assert_eq_dbg!(
        event.value().unwrap().user.value().unwrap().email.value(),
        None
    );
}
