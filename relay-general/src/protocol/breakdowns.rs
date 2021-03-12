use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};

/// Configuration to define breakdown to be generated based on properties and breakdown type.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum BreakdownConfig {
    SpanOp { matches: Vec<String> },
}

type BreakdownName = String;

/// Represents the breakdown configuration for a project.
/// Generate a named (key) breakdown (value).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BreakdownsConfig(pub HashMap<BreakdownName, BreakdownConfig>);

impl Deref for BreakdownsConfig {
    type Target = HashMap<BreakdownName, BreakdownConfig>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for BreakdownsConfig {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

// /// A map of breakdowns.
// ///
// /// TBA.
// #[derive(Clone, Debug, Default, PartialEq, Empty, ToValue, ProcessValue)]
// #[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
// pub struct Breakdowns(pub Object<Measurements>);
