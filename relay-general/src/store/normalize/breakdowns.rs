//! Contains the breakdowns normalization code
//!
//! This module is responsible for generating breakdowns for events, such as span operation breakdowns.
use crate::protocol::{
    Breakdowns, BreakdownsConfig, Event, EventType, Measurement, Measurements, Timestamp,
};
use crate::types::Annotated;

pub fn normalize_breakdowns(event: &mut Event, breakdowns_config: BreakdownsConfig) {
    if breakdowns_config.is_empty() {
        return;
    }

    for (breakdown_name, breakdown_config) in breakdowns_config.iter() {
        if !Breakdowns::is_valid_breakdown_name(&breakdown_name) {
            continue;
        }

        let breakdowns = breakdown_config.parse_event(event);

        // TODO: implement
    }
}
