use serde::{Deserialize, Serialize};

/// The span attribute to be computed.
#[derive(Clone, Copy, Debug, Eq, PartialEq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SpanAttribute {
    ExclusiveTime,

    /// forward compatibility
    #[serde(other)]
    Unknown,
}
