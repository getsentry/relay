use crate::services::processor::groups::GroupPayload;
use crate::utils::ManagedEnvelope;
use relay_event_schema::protocol::Event;
use relay_protocol::Annotated;

pub struct DefaultPayload<'a> {
    managed_envelope: &'a mut ManagedEnvelope,
    event: Option<&'a mut Annotated<Event>>,
}

impl<'a> DefaultPayload<'a> {
    pub fn no_event(managed_envelope: &'a mut ManagedEnvelope) -> Self {
        Self {
            managed_envelope,
            event: None,
        }
    }

    pub fn with_event(
        managed_envelope: &'a mut ManagedEnvelope,
        event: &'a mut Annotated<Event>,
    ) -> Self {
        Self {
            managed_envelope,
            event: Some(event),
        }
    }
}

impl<'a> GroupPayload<'a> for DefaultPayload<'a> {
    fn managed_envelope_mut(&mut self) -> &mut ManagedEnvelope {
        self.managed_envelope
    }

    fn managed_envelope(&self) -> &ManagedEnvelope {
        self.managed_envelope
    }

    fn event_mut(&mut self) -> Option<&mut Event> {
        self.event.as_mut().and_then(|e| e.value_mut().as_mut())
    }

    fn event(&self) -> Option<&Event> {
        self.event.as_ref().and_then(|e| e.value())
    }

    fn remove_event(&mut self) {
        self.event = None;
    }
}
