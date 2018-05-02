use std::fmt;
use std::net::IpAddr;

use uuid::Uuid;

use sentry_types::protocol::v7;

/// The v7 sentry protocol type.
pub type EventV7 = v7::Event<'static>;

/// Additional event meta data.
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
#[serde(default)]
pub struct EventMeta {
    /// the submitting ip address
    pub remote_addr: Option<IpAddr>,
    /// the client that submitted the event.
    pub sentry_client: Option<String>,
}

/// An enum that can hold various types of sentry events.
#[derive(Serialize, Debug, Clone)]
#[serde(untagged)]
pub enum EventVariant {
    /// The version 7 event variant.
    SentryV7(EventV7),
}

impl EventVariant {
    /// Ensures that the event held has an ID.
    ///
    /// This might not do anything for unknown event variants.
    pub fn ensure_id(&mut self) {
        match *self {
            EventVariant::SentryV7(ref mut event) => {
                event.id.get_or_insert_with(Uuid::new_v4);
            }
        }
    }

    /// Returns the ID of the event.
    pub fn id(&self) -> Option<Uuid> {
        match *self {
            EventVariant::SentryV7(ref event) => event.id,
        }
    }

    /// The changeset that should be used for events of this kind.
    pub fn changeset_type(&self) -> &'static str {
        match *self {
            EventVariant::SentryV7(..) => "store_v7",
        }
    }
}

impl fmt::Display for EventVariant {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            EventVariant::SentryV7(ref event) => fmt::Display::fmt(event, f),
        }
    }
}
