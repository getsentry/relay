use std::marker::PhantomData;

use relay_event_schema::protocol::Event;
use relay_protocol::Annotated;

use crate::utils::ManagedEnvelope;

#[derive(Debug, thiserror::Error)]
pub enum PayloadError {
    #[error("the payload has an event but no event is expected")]
    HasEvent,
    #[error("the payload has no event but an event is expected")]
    NoEvent,
}

// The usage is:
// Any -> no enforcement of what has to be there
// NoEvent -> must have no event set
// WithEvent -> needs to enforce that the event is set meaning that withEvent was created (ambiguity of Annotated makes this iffy)

/// A payload type that can either contain an event or not.
///
/// This enum is used in functions that are agnostic about whether an event is present.
/// In contrast, the [`WithEvent`] and [`NoEvent`] variants should be used directly when
/// there must be a strict invariant about event presence/absence.
#[derive(Debug)]
pub enum AnyRefMut<'a, G> {
    WithEvent(&'a mut WithEvent<G>),
    NoEvent(&'a mut NoEvent<G>),
}

impl<'a, G> AnyRefMut<'a, G> {
    pub fn managed_envelope_mut(&mut self) -> &mut ManagedEnvelope {
        match self {
            AnyRefMut::WithEvent(with_event) => &mut with_event.managed_envelope,
            AnyRefMut::NoEvent(no_event) => &mut no_event.managed_envelope,
        }
    }

    pub fn get_mut(&mut self) -> (&mut ManagedEnvelope, Option<&mut Annotated<Event>>) {
        match self {
            AnyRefMut::WithEvent(with_event) => (
                &mut with_event.managed_envelope,
                Some(&mut with_event.event),
            ),
            AnyRefMut::NoEvent(no_event) => (&mut no_event.managed_envelope, None),
        }
    }
}

impl<'a, G> From<&'a mut WithEvent<G>> for AnyRefMut<'a, G> {
    fn from(with_event: &'a mut WithEvent<G>) -> Self {
        AnyRefMut::WithEvent(with_event)
    }
}

impl<'a, G> From<&'a mut NoEvent<G>> for AnyRefMut<'a, G> {
    fn from(no_event: &'a mut NoEvent<G>) -> Self {
        AnyRefMut::NoEvent(no_event)
    }
}

/// A payload type that can either contain an event or not.
///
/// This enum is used in functions that are agnostic about whether an event is present,
/// providing immutable access to the underlying payload.
#[derive(Debug)]
pub enum Any<'a, G> {
    WithEvent(&'a WithEvent<G>),
    NoEvent(&'a NoEvent<G>),
}

impl<'a, G> Any<'a, G> {
    pub fn managed_envelope(&self) -> &ManagedEnvelope {
        match self {
            Any::WithEvent(with_event) => &with_event.managed_envelope,
            Any::NoEvent(no_event) => &no_event.managed_envelope,
        }
    }

    pub fn get(&self) -> (&ManagedEnvelope, Option<&Annotated<Event>>) {
        match self {
            Any::WithEvent(with_event) => (&with_event.managed_envelope, Some(&with_event.event)),
            Any::NoEvent(no_event) => (&no_event.managed_envelope, None),
        }
    }
}

impl<'a, G> From<&'a WithEvent<G>> for Any<'a, G> {
    fn from(with_event: &'a WithEvent<G>) -> Self {
        Any::WithEvent(with_event)
    }
}

impl<'a, G> From<&'a NoEvent<G>> for Any<'a, G> {
    fn from(no_event: &'a NoEvent<G>) -> Self {
        Any::NoEvent(no_event)
    }
}

/// A payload type that must contain an event.
///
/// This type enforces at compile-time that an event is present. Use this type
/// when your code requires an event to be available.
#[derive(Debug)]
pub struct WithEvent<G> {
    pub managed_envelope: ManagedEnvelope,
    pub event: Annotated<Event>,
    _1: PhantomData<G>,
}

impl<G> WithEvent<G> {
    /// Creates a new WithEvent payload containing the given envelope and event.
    pub fn new(managed_envelope: ManagedEnvelope, event: Annotated<Event>) -> Self {
        Self {
            managed_envelope,
            event,
            _1: PhantomData,
        }
    }

    /// Returns true if this payload contains an event with a value.
    pub fn has_event(&self) -> bool {
        self.event.value().is_some()
    }

    /// Returns true if this payload's envelope is empty.
    pub fn has_empty_envelope(&self) -> bool {
        self.managed_envelope.envelope().is_empty()
    }

    /// Removes the event from this payload, converting it into a NoEvent variant.
    pub fn remove_event(self) -> NoEvent<G> {
        NoEvent::new(self.managed_envelope)
    }
}

/// A payload type that must not contain an event.
///
/// This type enforces at compile-time that no event is present. Use this type
/// when your code requires that no event is available.
#[derive(Debug)]
pub struct NoEvent<G> {
    pub managed_envelope: ManagedEnvelope,
    _1: PhantomData<G>,
}

impl<G> NoEvent<G> {
    /// Creates a new NoEvent payload containing the given envelope.
    pub fn new(managed_envelope: ManagedEnvelope) -> Self {
        Self {
            managed_envelope,
            _1: PhantomData,
        }
    }

    /// Returns true if this payload's envelope is empty.
    pub fn has_empty_envelope(&self) -> bool {
        self.managed_envelope.envelope().is_empty()
    }

    /// Adds an event to this payload, converting it into a WithEvent variant.
    pub fn add_event(self, event: Annotated<Event>) -> WithEvent<G> {
        WithEvent::new(self.managed_envelope, event)
    }
}
