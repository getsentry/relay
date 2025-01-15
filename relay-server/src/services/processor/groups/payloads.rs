use crate::services::processor::groups::GroupPayload;
use crate::utils::ManagedEnvelope;
use relay_event_schema::protocol::Event;
use relay_protocol::Annotated;

// TODO: use a concrete type of payload over a generic G.

/// A base implementation of a payload that can be processed by a group processor.
///
/// This struct holds a reference to a typed envelope and an optional event. It provides
/// the basic functionality required by the [`GroupPayload`] trait.
pub struct BasePayload<'a> {
    managed_envelope: &'a mut ManagedEnvelope,
    event: Option<&'a mut Annotated<Event>>,
}

impl<'a> BasePayload<'a> {
    /// Creates a new [`BasePayload`] without an event.
    ///
    /// This is useful when processing envelopes that don't contain event data.
    pub fn no_event(managed_envelope: &'a mut ManagedEnvelope) -> Self {
        Self {
            managed_envelope,
            event: None,
        }
    }
}

impl<'a> GroupPayload<'a> for BasePayload<'a> {
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

// TODO: implement a macro to generate a new type payload from the base payload which forwards the
//  methods.
