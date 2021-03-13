//! Contains the breakdowns normalization code
//!
//! This module is responsible for generating breakdowns for events, such as span operation breakdowns.
use crate::protocol::{Breakdowns, BreakdownsConfig, Event};
use crate::types::Annotated;

pub fn normalize_breakdowns(event: &mut Event, breakdowns_config: &BreakdownsConfig) {
    if breakdowns_config.is_empty() {
        return;
    }

    for (breakdown_name, breakdown_config) in breakdowns_config.iter() {
        if !Breakdowns::is_valid_breakdown_name(&breakdown_name) {
            continue;
        }

        let breakdown = match breakdown_config.parse_event(event) {
            None => continue,
            Some(breakdown) => breakdown,
        };

        if breakdown.is_empty() {
            continue;
        }

        let breakdowns = event
            .breakdowns
            .value_mut()
            .get_or_insert_with(Breakdowns::default);

        breakdowns.insert(breakdown_name.clone(), Annotated::new(breakdown));
    }
}
