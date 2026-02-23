use relay_event_schema::protocol::Event;
use relay_protocol::Annotated;

use crate::envelope::{Item, ItemType};
use crate::managed::{Counted, Quantities};
use crate::processing::errors::Result;
use crate::processing::errors::errors::{
    Context, ErrorRef, ErrorRefMut, ParsedError, SentryError, utils,
};
use crate::services::processor::ProcessingError;

#[derive(Debug)]
pub struct FormData {
    pub event: Annotated<Event>,
    pub attachments: Vec<Item>,
    pub user_reports: Vec<Item>,
}

impl SentryError for FormData {
    fn try_expand(items: &mut Vec<Item>, _ctx: Context<'_>) -> Result<Option<ParsedError<Self>>> {
        let Some(form_data) = utils::take_item_of_type(items, ItemType::FormData) else {
            return Ok(None);
        };

        let event = {
            let mut value = serde_json::Value::Object(Default::default());
            crate::services::processor::event::merge_formdata(&mut value, form_data);
            Annotated::deserialize_with_meta(value).map_err(ProcessingError::InvalidJson)
        }?;

        let error = Self {
            event,
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

impl Counted for FormData {
    fn quantities(&self) -> Quantities {
        self.as_ref().to_quantities()
    }
}
