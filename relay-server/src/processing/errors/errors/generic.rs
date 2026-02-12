use relay_event_schema::protocol::Event;
use relay_protocol::Annotated;

use crate::envelope::{ItemType, Items};
use crate::processing::errors::Result;
use crate::processing::errors::errors::{ErrorRef, ErrorRefMut, ParsedError, SentryError, utils};

#[derive(Debug)]
pub struct Generic {
    pub event: Annotated<Event>,
    pub attachments: Items,
}

impl SentryError for Generic {
    fn try_parse(items: &mut Items) -> Result<Option<ParsedError<Self>>> {
        let Some(ev) = utils::take_item_of_type(items, ItemType::Event) else {
            return Ok(None);
        };

        let fully_normalized = ev.fully_normalized();
        let event = utils::event_from_json_payload(ev, None)?;

        let error = Self {
            event,
            attachments: items
                .drain_filter(|item| *item.ty() == ItemType::Attachment)
                .collect(),
        };

        Ok(Some(ParsedError {
            error,
            fully_normalized,
        }))
    }

    fn as_ref(&self) -> ErrorRef<'_> {
        ErrorRef {
            event: &self.event,
            attachments: &self.attachments,
        }
    }

    fn as_ref_mut(&mut self) -> ErrorRefMut<'_> {
        ErrorRefMut {
            event: &mut self.event,
            attachments: &mut self.attachments,
        }
    }
}
