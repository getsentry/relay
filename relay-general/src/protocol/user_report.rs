use crate::protocol::EventId;

use serde::{de::DeserializeOwned, Deserialize, Deserializer, Serialize};

/// User feedback for an event as sent by the client to the userfeedback/userreport endpoint.
///
/// Historically the "schema" for user report has been "defined" as the set of possible
/// keyword-arguments `sentry.models.UserReport` accepts. Anything the model constructor
/// accepts goes.
///
/// For example, `{"email": null}` is only invalid because `UserReport(email=None).save()` is. SDKs
/// may neither send this (historically, in Relay we relaxed this already), but more importantly
/// the ingest consumer may never receive this... because it would end up crashing the ingest
/// consumer (while in older versions of Sentry it would simply crash the endpoint).
///
/// The database/model schema is a bunch of not-null strings that have (pgsql) defaults, so that's
/// how we end up with this struct definition.
#[derive(Debug, Deserialize, Serialize)]
pub struct UserReport {
    /// The event ID for which this user feedback is created.
    pub event_id: EventId,
    /// The user's name.
    #[serde(default, deserialize_with = "null_to_default")]
    pub name: String,
    /// The user's email address.
    #[serde(default, deserialize_with = "null_to_default")]
    pub email: String,
    /// Comments supplied by the user.
    #[serde(default, deserialize_with = "null_to_default")]
    pub comments: String,
}

fn null_to_default<'de, D, V>(deserializer: D) -> Result<V, D::Error>
where
    D: Deserializer<'de>,
    V: Default + DeserializeOwned,
{
    let opt = Option::deserialize(deserializer)?;
    Ok(opt.unwrap_or_default())
}
