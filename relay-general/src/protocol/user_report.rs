use crate::protocol::EventId;

use serde::{Deserialize, Serialize};

/// User feedback for an event as sent by the client to the userfeedback/userreport endpoint.
#[derive(Debug, Deserialize, Serialize)]
pub struct UserReport {
    /// The event ID for which this user feedback is created.
    pub event_id: EventId,
    /// The user's name
    #[serde(default)]
    pub name: Option<String>,
    /// The user's email address
    #[serde(default)]
    pub email: Option<String>,
    /// Comments supplied by the user
    pub comments: String,
}
