use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct ClientSdk {
    pub name: String,
    pub version: String,
}
