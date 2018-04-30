use std::fmt;

use uuid::Uuid;

use sentry_types::protocol::v7;

/// The v7 sentry protocol type.
pub type EventV7 = v7::Event<'static>;

/// An enum that can hold various types of sentry events.
#[derive(Serialize, Debug, Clone)]
#[serde(untagged)]
pub enum EventVariant {
    /// The version 7 event variant.
    SentryV7(EventV7),
}

impl EventVariant {
    /// Ensures that the event held has an ID and returns it.
    ///
    /// For some event variants an ID might be impossible to set
    /// in which case `None` is returned.
    pub fn ensure_id(&mut self) -> Option<Uuid> {
        match *self {
            EventVariant::SentryV7(ref mut event) => {
                Some(*event.id.get_or_insert_with(Uuid::new_v4))
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
