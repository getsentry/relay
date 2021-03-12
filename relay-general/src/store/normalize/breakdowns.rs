//! Contains the breakdowns normalization code
//!
//! This module is responsible for generating breakdowns for events, such as span operation breakdowns.

use crate::protocol::{BreakdownsConfig, Event, EventType, Measurement, Measurements, Timestamp};
use crate::types::Annotated;

pub fn normalize_breakdowns(event: &mut Event, breakdowns_config: BreakdownsConfig) {
    // TODO: implement
}
