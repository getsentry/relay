use std::collections::HashMap;
use std::fmt;
use std::net::IpAddr;

use serde_json;
use url::Url;
use url_serde;
use uuid::Uuid;

use semaphore_common::v7;

use query::AortaChangeset;
use utils::{serialize_origin, StandardBase64};

/// The v7 sentry protocol type.
pub type EventV7 = v7::Event<'static>;

/// Additional event meta data.
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
#[serde(default)]
pub struct EventMeta {
    /// the optional browser origin of the event.
    #[serde(deserialize_with = "url_serde::deserialize")]
    #[serde(serialize_with = "serialize_origin")]
    pub origin: Option<Url>,
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
    /// A foreign event.
    Foreign(ForeignEvent),
}

/// Represents some payload not known to the relay
#[derive(Serialize, Debug, Clone)]
#[serde(rename_all = "lowercase")]
pub enum ForeignPayload {
    /// JSON formatted payload
    Json(serde_json::Value),
    /// Base64 encoded binary data.
    Raw(#[serde(with = "StandardBase64")] Vec<u8>),
}

/// Represents an event unknown to the relay
#[derive(Serialize, Debug, Clone)]
pub struct ForeignEvent {
    /// Store endpoint type.
    pub store_type: String,
    /// A subset of http request headers emitted with the event.
    pub headers: HashMap<String, String>,
    /// The request payload of the event.
    #[serde(flatten)]
    pub payload: ForeignPayload,
}

impl EventVariant {
    /// Ensures that the event held has an ID.
    ///
    /// This might not do anything for unknown event variants.
    pub fn ensure_id(&mut self) {
        match self {
            EventVariant::SentryV7(event) => {
                event.id.get_or_insert_with(Uuid::new_v4);
            }
            _ => {}
        }
    }

    /// Returns the ID of the event.
    pub fn id(&self) -> Option<Uuid> {
        match self {
            EventVariant::SentryV7(event) => event.id,
            EventVariant::Foreign(..) => None,
        }
    }

    /// The changeset that should be used for events of this kind.
    pub fn changeset_type(&self) -> &'static str {
        match self {
            EventVariant::SentryV7(..) => "store_v7",
            EventVariant::Foreign(..) => "store_foreign",
        }
    }
}

impl fmt::Display for EventVariant {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            EventVariant::SentryV7(event) => fmt::Display::fmt(event, f),
            EventVariant::Foreign(..) => write!(f, "<foreign event>"),
        }
    }
}

/// The changeset for event stores.
#[derive(Serialize, Debug)]
pub struct StoreChangeset {
    /// The public key that requested the store.
    pub public_key: String,
    /// The event meta data.
    pub meta: EventMeta,
    /// The event payload.
    pub event: EventVariant,
}

impl AortaChangeset for StoreChangeset {
    fn aorta_changeset_type(&self) -> &'static str {
        self.event.changeset_type()
    }
}
