use crate::protocol::EventId;

use serde::{Deserialize, Serialize};

/// User feedback for an event as sent by the client to the userfeedback/userreport endpoint.
#[derive(Debug, Deserialize, Serialize)]
pub struct UserReport {
    /// The event ID for which this user feedback is created.
    pub event_id: EventId,
    /// The user's name.
    // Empty strings are in fact stored in the database already
    #[serde(default)]
    pub name: String,
    /// The user's email address.
    // Empty strings are in fact stored in the database already
    #[serde(default)]
    pub email: String,
    /// Comments supplied by the user.
    // Empty strings are in fact stored in the database already
    #[serde(default)]
    pub comments: String,
}
