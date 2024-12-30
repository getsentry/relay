use crate::services::processor::groups::{Group, GroupPayload};
use crate::utils::{ManagedEnvelope, TypedEnvelope};
use relay_event_schema::protocol::Event;
use relay_protocol::Annotated;

/// A base implementation of a payload that can be processed by a group processor.
///
/// This struct holds a reference to a typed envelope and an optional event. It provides
/// the basic functionality required by the [`GroupPayload`] trait.
pub struct BasePayload<'a, G: Group> {
    managed_envelope: &'a mut TypedEnvelope<G>,
    event: Option<&'a mut Annotated<Event>>,
}

impl<'a, G: Group> BasePayload<'a, G> {
    /// Creates a new [`BasePayload`] without an event.
    ///
    /// This is useful when processing envelopes that don't contain event data.
    pub fn no_event(managed_envelope: &'a mut TypedEnvelope<G>) -> Self {
        Self {
            managed_envelope,
            event: None,
        }
    }
}

impl<'a, G: Group> GroupPayload<'a, G> for BasePayload<'a, G> {
    fn managed_envelope_mut(&mut self) -> &mut ManagedEnvelope {
        self.managed_envelope
    }

    fn managed_envelope(&self) -> &ManagedEnvelope {
        self.managed_envelope
    }

    fn event(&self) -> Option<&Event> {
        self.event.as_ref().and_then(|e| e.value())
    }

    fn remove_event(&mut self) {
        self.event = None;
    }
}
