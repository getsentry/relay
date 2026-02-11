use relay_base_schema::events::EventType;
use relay_event_schema::protocol::{Event, Metrics};
use relay_protocol::Annotated;

use crate::envelope::{EnvelopeHeaders, Item, ItemType, Items};
use crate::managed::Counted;
use crate::processing::errors::{Error, Result};
use crate::processing::utils::event::EventFullyNormalized;

pub struct ErrorRef<'a> {
    pub event: &'a Annotated<Event>,
    pub attachments: &'a Items,
}

pub struct ErrorRefMut<'a> {
    pub event: &'a mut Annotated<Event>,
    pub attachments: &'a mut Items,
}

pub struct ExpandedError {
    pub headers: EnvelopeHeaders,
    pub fully_normalized: EventFullyNormalized,
    pub metrics: Metrics,

    pub kind: ErrorKind,
}

impl Counted for ExpandedError {
    fn quantities(&self) -> crate::managed::Quantities {
        todo!()
    }
}

pub enum ErrorKind {
    Generic(GenericError),
}

impl ErrorKind {
    pub fn as_ref(&self) -> ErrorRef<'_> {
        match self {
            Self::Generic(error) => error.as_ref(),
        }
    }

    pub fn as_ref_mut(&mut self) -> ErrorRefMut<'_> {
        match self {
            Self::Generic(error) => error.as_ref_mut(),
        }
    }
}

impl From<GenericError> for ErrorKind {
    fn from(value: GenericError) -> Self {
        Self::Generic(value)
    }
}

pub struct GenericError {
    pub event: Annotated<Event>,
    pub attachments: Items,
    // event
    // attachments
}

impl GenericError {
    pub fn try_parse(items: &mut Items) -> Result<Option<Self>> {
        let Some(ev) = take_item_of_type(items, ItemType::Event) else {
            return Ok(None);
        };

        let event = event_from_json_payload(ev, None)?;

        Ok(Some(Self {
            event,
            attachments: items
                .drain_filter(|item| *item.ty() == ItemType::Attachment)
                .collect(),
        }))
    }

    pub fn as_ref(&self) -> ErrorRef<'_> {
        ErrorRef {
            event: &self.event,
            attachments: &self.attachments,
        }
    }

    pub fn as_ref_mut(&mut self) -> ErrorRefMut<'_> {
        ErrorRefMut {
            event: &mut self.event,
            attachments: &mut self.attachments,
        }
    }
}

pub struct UserFeedback {
    // Maybe this is generic
    // event
    // ???
}

pub struct MinidumpError {
    // event
    // minidump
    // other attachments?
}

pub struct AppleCrashReportError {
    // event
    // apple crash report
    // other attachments?
}

pub struct UnrealError {}

pub struct SwitchError {}

pub struct PlaystationError {}

pub struct MaybeSomethingWithOutEventJustToForward {
    // Playstation/prospero only converts in processing
    // Or allow Annotated::empty for events
}
// impl TryFrom<SerializedError> for ExpandedEvent {
//     type Error = Error;
//
//     fn try_from(value: SerializedError) -> Result<Self> {
//         todo!()
//     }
// }

fn take_item_of_type(items: &mut Items, ty: ItemType) -> Option<Item> {
    let index = items.iter().position(|item| item.ty() == &ty);
    index.map(|index| items.swap_remove(index))
}

fn event_from_json_payload(item: Item, event_type: Option<EventType>) -> Result<Annotated<Event>> {
    let mut event =
        Annotated::<Event>::from_json_bytes(&item.payload()).map_err(Error::InvalidJson)?;

    if let Some(event_value) = event.value_mut()
        && event_type.is_some()
    {
        event_value.ty.set_value(event_type);
    }

    Ok(event)
}
