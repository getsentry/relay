use crate::utils::ManagedEnvelope;
use relay_event_schema::protocol::Event;
use relay_protocol::Annotated;
use std::marker::PhantomData;

/// A payload type that may contain an event, with owned data
#[derive(Debug)]
pub struct MaybeEvent<'a, G> {
    pub event: Option<Annotated<Event>>,
    pub managed_envelope: &'a mut ManagedEnvelope,
    _group: PhantomData<G>,
}

impl<'a, G> MaybeEvent<'a, G> {
    pub fn new(managed_envelope: &'a mut ManagedEnvelope, event: Option<Annotated<Event>>) -> Self {
        Self {
            event,
            managed_envelope,
            _group: PhantomData,
        }
    }

    pub fn remove_event(self) -> MaybeEvent<'a, G> {
        MaybeEvent::new(self.managed_envelope, None)
    }
}

impl<'a, G> From<WithEvent<'a, G>> for MaybeEvent<'a, G> {
    fn from(with_event: WithEvent<'a, G>) -> Self {
        MaybeEvent {
            event: Some(with_event.event),
            managed_envelope: with_event.managed_envelope,
            _group: PhantomData,
        }
    }
}

impl<'a, G> From<NoEvent<'a, G>> for MaybeEvent<'a, G> {
    fn from(no_event: NoEvent<'a, G>) -> Self {
        MaybeEvent {
            event: None,
            managed_envelope: no_event.managed_envelope,
            _group: PhantomData,
        }
    }
}

/// A payload type that must contain an event
#[derive(Debug)]
pub struct WithEvent<'a, G> {
    pub event: Annotated<Event>,
    pub managed_envelope: &'a mut ManagedEnvelope,
    _group: PhantomData<G>,
}

impl<'a, G> WithEvent<'a, G> {
    pub fn new(managed_envelope: &'a mut ManagedEnvelope, event: Annotated<Event>) -> Self {
        debug_assert!(event.value().is_some());

        Self {
            event,
            managed_envelope,
            _group: PhantomData,
        }
    }

    pub fn remove_event(self) -> NoEvent<'a, G> {
        NoEvent {
            managed_envelope: self.managed_envelope,
            _group: PhantomData,
        }
    }
}

/// A payload type that must not contain an event
#[derive(Debug)]
pub struct NoEvent<'a, G> {
    pub managed_envelope: &'a mut ManagedEnvelope,
    _group: PhantomData<G>,
}

impl<'a, G> NoEvent<'a, G> {
    pub fn new(managed_envelope: &'a mut ManagedEnvelope) -> Self {
        Self {
            managed_envelope,
            _group: PhantomData,
        }
    }

    pub fn add_event(self, event: Annotated<Event>) -> WithEvent<'a, G> {
        WithEvent {
            event,
            managed_envelope: self.managed_envelope,
            _group: PhantomData,
        }
    }
    
    pub fn as_maybe(&'a mut self) -> MaybeEventRefMut<'a, G> {
        MaybeEventRefMut::new(self.managed_envelope, None)
    }
}

/// A payload type that may contain an event, with mutable references
#[derive(Debug)]
pub struct MaybeEventRefMut<'a, G> {
    pub event: Option<&'a mut Annotated<Event>>,
    pub managed_envelope: &'a mut ManagedEnvelope,
    _group: PhantomData<G>,
}

impl<'a, G> MaybeEventRefMut<'a, G> {
    pub fn new(
        managed_envelope: &'a mut ManagedEnvelope,
        event: Option<&'a mut Annotated<Event>>,
    ) -> Self {
        Self {
            event,
            managed_envelope,
            _group: PhantomData,
        }
    }
}

impl<'a, G> From<&'a mut MaybeEvent<'a, G>> for MaybeEventRefMut<'a, G> {
    fn from(maybe_event: &'a mut MaybeEvent<'a, G>) -> Self {
        MaybeEventRefMut::new(maybe_event.managed_envelope, maybe_event.event.as_mut())
    }
}

impl<'a, G> From<&'a mut WithEvent<'a, G>> for MaybeEventRefMut<'a, G> {
    fn from(with_event: &'a mut WithEvent<'a, G>) -> Self {
        MaybeEventRefMut::new(with_event.managed_envelope, Some(&mut with_event.event))
    }
}

impl<'a, G> From<&mut NoEvent<'a, G>> for MaybeEventRefMut<'a, G> {
    fn from(no_event: &mut NoEvent<'a, G>) -> Self {
        MaybeEventRefMut::new(no_event.managed_envelope, None)
    }
}

/// A payload type that must contain an event, with mutable references
#[derive(Debug)]
pub struct WithEventRefMut<'a, G> {
    pub event: &'a mut Annotated<Event>,
    pub managed_envelope: &'a mut ManagedEnvelope,
    _group: PhantomData<G>,
}

impl<'a, G> WithEventRefMut<'a, G> {
    pub fn new(managed_envelope: &'a mut ManagedEnvelope, event: &'a mut Annotated<Event>) -> Self {
        debug_assert!(event.value().is_some());

        Self {
            event,
            managed_envelope,
            _group: PhantomData,
        }
    }
}

impl<'a, G> From<&'a mut WithEvent<'a, G>> for WithEventRefMut<'a, G> {
    fn from(with_event: &'a mut WithEvent<'a, G>) -> Self {
        WithEventRefMut::new(with_event.managed_envelope, &mut with_event.event)
    }
}

impl<'a, G> TryFrom<&'a mut MaybeEvent<'a, G>> for WithEventRefMut<'a, G> {
    type Error = ();

    fn try_from(maybe_event: &'a mut MaybeEvent<'a, G>) -> Result<Self, Self::Error> {
        let event = maybe_event.event.as_mut().ok_or(())?;
        Ok(WithEventRefMut::new(maybe_event.managed_envelope, event))
    }
}

impl<'a, G> From<&'a mut WithEventRefMut<'a, G>> for WithEventRefMut<'a, G> {
    fn from(with_event_ref_mut: &'a mut WithEventRefMut<'a, G>) -> Self {
        WithEventRefMut::new(
            with_event_ref_mut.managed_envelope,
            with_event_ref_mut.event,
        )
    }
}

/// A payload type that must not contain an event, with mutable references
#[derive(Debug)]
pub struct NoEventRefMut<'a, G> {
    pub managed_envelope: &'a mut ManagedEnvelope,
    _group: PhantomData<G>,
}

impl<'a, G> NoEventRefMut<'a, G> {
    pub fn new(managed_envelope: &'a mut ManagedEnvelope) -> Self {
        Self {
            managed_envelope,
            _group: PhantomData,
        }
    }
}

impl<'a, G> From<&'a mut NoEvent<'a, G>> for NoEventRefMut<'a, G> {
    fn from(no_event: &'a mut NoEvent<'a, G>) -> Self {
        NoEventRefMut::new(no_event.managed_envelope)
    }
}

impl <'a, 'b: 'a, G> From<&'b mut NoEventRefMut<'a, G>> for NoEventRefMut<'a, G> {
    fn from(no_event_ref_mut: &'b mut NoEventRefMut<'a, G>) -> Self {
        NoEventRefMut::new(no_event_ref_mut.managed_envelope)
    }
}
