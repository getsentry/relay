use crate::services::processor::groups::{Group, GroupPayload};
use crate::utils::{ManagedEnvelope, TypedEnvelope};
use relay_event_schema::protocol::Event;
use relay_protocol::Annotated;

pub struct BasePayload<'a, G: Group> {
    managed_envelope: &'a mut TypedEnvelope<G>,
    event: Option<&'a mut Annotated<Event>>,
}

impl<'a, G: Group> BasePayload<'a, G> {
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
