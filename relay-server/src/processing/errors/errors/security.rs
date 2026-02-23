use relay_event_schema::protocol::Event;
use relay_protocol::Annotated;

use crate::envelope::{Item, ItemType};
use crate::managed::{Counted, Quantities};
use crate::processing::errors::Result;
use crate::processing::errors::errors::{
    Context, ErrorRef, ErrorRefMut, ParsedError, SentryError, utils,
};

#[derive(Debug)]
pub struct Security {
    pub event: Annotated<Event>,
    pub attachments: Vec<Item>,
    pub user_reports: Vec<Item>,
}

impl SentryError for Security {
    fn try_expand(items: &mut Vec<Item>, _ctx: Context<'_>) -> Result<Option<ParsedError<Self>>> {
        let Some(ev) = utils::take_item_of_type(items, ItemType::Security) else {
            return Ok(None);
        };

        let error = Self {
            event: utils::event_from_json_payload(ev, None)?,
            attachments: utils::take_items_of_type(items, ItemType::Attachment),
            user_reports: utils::take_items_of_type(items, ItemType::UserReport),
        };

        Ok(Some(ParsedError {
            error,
            fully_normalized: false,
        }))
    }

    fn as_ref(&self) -> ErrorRef<'_> {
        ErrorRef {
            event: &self.event,
            attachments: &self.attachments,
            user_reports: &self.user_reports,
        }
    }

    fn as_ref_mut(&mut self) -> ErrorRefMut<'_> {
        ErrorRefMut {
            event: &mut self.event,
            attachments: &mut self.attachments,
            user_reports: Some(&mut self.user_reports),
        }
    }
}

impl Counted for Security {
    fn quantities(&self) -> Quantities {
        self.as_ref().to_quantities()
    }
}
