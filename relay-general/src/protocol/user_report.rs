use crate::protocol::EventId;

use serde::{Deserialize, Serialize};

/// User feedback for an event as sent by the client to the userfeedback/userreport endpoint.
///
/// Historically the "schema" for user report has been "defined" as the set of possible
/// keyword-arguments `sentry.models.UserReport` accepts. Anything the model constructor
/// accepts goes.
///
/// For example, `{"email": null}` is only invalid because `UserReport(email=None).save()` is.
///
/// The database/model schema is a bunch of not-null strings that have (pgsql) defaults, so that's
/// how we end up with this struct definition.
#[derive(Debug, Deserialize, Serialize)]
pub struct UserReport {
    /// The event ID for which this user feedback is created.
    pub event_id: EventId,
    /// The user's name.
    #[serde(default)]
    pub name: String,
    /// The user's email address.
    #[serde(default)]
    pub email: String,
    /// Comments supplied by the user.
    #[serde(default)]
    pub comments: String,
}
