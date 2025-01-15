use relay_event_schema::protocol::Event;
use relay_protocol::Annotated;
use crate::utils::ManagedEnvelope;

struct WithEvent;
struct NoEvent;
struct MaybeEvent;



pub struct Payload<'a> {
    managed_envelope: &'a mut ManagedEnvelope,
    event: Option<Annotated<Event>>
}