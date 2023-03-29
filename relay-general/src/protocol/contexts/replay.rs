use crate::protocol::EventId;
use crate::types::Annotated;

/// Profile context
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct ReplayContext {
    /// The profile ID.
    #[metastructure(required = "true")]
    pub replay_id: Annotated<EventId>,
}

impl ReplayContext {
    /// The key under which a profile context is generally stored (in `Contexts`)
    pub fn default_key() -> &'static str {
        "replay"
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::protocol::Context;

// }
