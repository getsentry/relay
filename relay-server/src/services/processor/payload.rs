use relay_event_schema::protocol::Event;
use relay_protocol::Annotated;
use std::marker::PhantomData;

use crate::utils::ManagedEnvelope;

/// A payload type that can either contain an event or not, allowing ownership and type changes.
///
/// This enum is used when you need to take ownership of the payload and potentially change
/// its type (e.g., converting from `WithEvent` to `NoEvent` or vice versa). This is useful
/// in processing pipelines where events may be added or removed from the payload.
#[derive(Debug)]
pub enum Any<G> {
    WithEvent(WithEvent<G>),
    NoEvent(NoEvent<G>),
}

impl<G> Any<G> {
    pub fn managed_envelope(&self) -> &ManagedEnvelope {
        match self {
            Any::WithEvent(with_event) => &with_event.managed_envelope,
            Any::NoEvent(no_event) => &no_event.managed_envelope,
        }
    }

    pub fn managed_envelope_mut(&mut self) -> &mut ManagedEnvelope {
        match self {
            Any::WithEvent(with_event) => &mut with_event.managed_envelope,
            Any::NoEvent(no_event) => &mut no_event.managed_envelope,
        }
    }

    pub fn get(&self) -> (&ManagedEnvelope, Option<&Annotated<Event>>) {
        match self {
            Any::WithEvent(with_event) => (&with_event.managed_envelope, Some(&with_event.event)),
            Any::NoEvent(no_event) => (&no_event.managed_envelope, None),
        }
    }

    pub fn get_mut(&mut self) -> (&mut ManagedEnvelope, Option<&mut Annotated<Event>>) {
        match self {
            Any::WithEvent(with_event) => (
                &mut with_event.managed_envelope,
                Some(&mut with_event.event),
            ),
            Any::NoEvent(no_event) => (&mut no_event.managed_envelope, None),
        }
    }

    pub fn has_event(&self) -> bool {
        match self {
            Any::WithEvent(with_event) => with_event.event.value().is_some(),
            Any::NoEvent(_) => false,
        }
    }

    pub fn remove_event(self) -> Self {
        match self {
            Any::WithEvent(with_event) => Any::NoEvent(with_event.remove_event()),
            Any::NoEvent(no_event) => Any::NoEvent(no_event),
        }
    }
}

impl<G> From<WithEvent<G>> for Any<G> {
    fn from(with_event: WithEvent<G>) -> Self {
        Any::WithEvent(with_event)
    }
}

impl<G> From<NoEvent<G>> for Any<G> {
    fn from(no_event: NoEvent<G>) -> Self {
        Any::NoEvent(no_event)
    }
}

/// A payload type that provides read-only access to data that may or may not contain an event.
///
/// This enum is used when you need to inspect the payload contents without modifying them.
/// It borrows the underlying data immutably, making it suitable for validation or analysis
/// operations that don't require modifications.
#[derive(Debug)]
pub enum AnyRef<'a, G> {
    WithEvent(&'a WithEvent<G>),
    NoEvent(&'a NoEvent<G>),
}

impl<'a, G> AnyRef<'a, G> {
    pub fn managed_envelope(&self) -> &ManagedEnvelope {
        match self {
            AnyRef::WithEvent(with_event) => &with_event.managed_envelope,
            AnyRef::NoEvent(no_event) => &no_event.managed_envelope,
        }
    }

    pub fn get(&self) -> (&ManagedEnvelope, Option<&Annotated<Event>>) {
        match self {
            AnyRef::WithEvent(with_event) => {
                (&with_event.managed_envelope, Some(&with_event.event))
            }
            AnyRef::NoEvent(no_event) => (&no_event.managed_envelope, None),
        }
    }
}

impl<'a, G> From<&'a WithEvent<G>> for AnyRef<'a, G> {
    fn from(with_event: &'a WithEvent<G>) -> Self {
        AnyRef::WithEvent(with_event)
    }
}

impl<'a, G> From<&'a NoEvent<G>> for AnyRef<'a, G> {
    fn from(no_event: &'a NoEvent<G>) -> Self {
        AnyRef::NoEvent(no_event)
    }
}

impl<'a, G> From<&'a mut WithEvent<G>> for AnyRef<'a, G> {
    fn from(with_event: &'a mut WithEvent<G>) -> Self {
        AnyRef::WithEvent(with_event)
    }
}

impl<'a, G> From<&'a mut NoEvent<G>> for AnyRef<'a, G> {
    fn from(no_event: &'a mut NoEvent<G>) -> Self {
        AnyRef::NoEvent(no_event)
    }
}

impl<'a, G> From<&'a Any<G>> for AnyRef<'a, G> {
    fn from(any: &'a Any<G>) -> Self {
        match any {
            Any::WithEvent(with_event) => AnyRef::WithEvent(with_event),
            Any::NoEvent(no_event) => AnyRef::NoEvent(no_event),
        }
    }
}

/// A payload type that provides mutable access to data that may or may not contain an event.
///
/// This enum is used when you need to modify the payload contents in-place without changing
/// its type (i.e., without converting between WithEvent and NoEvent). It's useful for
/// operations that update event data or envelope contents while maintaining the current
/// event presence state.
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

impl<'a, G> From<&'a mut Any<G>> for AnyRefMut<'a, G> {
    fn from(any: &'a mut Any<G>) -> Self {
        match any {
            Any::WithEvent(with_event) => AnyRefMut::WithEvent(with_event),
            Any::NoEvent(no_event) => AnyRefMut::NoEvent(no_event),
        }
    }
}

/// A payload type that must contain an event.
///
/// This type enforces at compile-time that an event is present, providing strong guarantees
/// for code that requires event data. Use this type when your processing logic specifically
/// needs to work with events and should fail if no event is available.
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
/// This type enforces at compile-time that no event is present, providing strong guarantees
/// for code that must operate only on non-event data. Use this type when your processing
/// logic specifically handles non-event payloads and should fail if an event is present.
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
