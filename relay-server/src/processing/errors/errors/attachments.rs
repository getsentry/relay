use relay_event_schema::protocol::Event;
use relay_protocol::Annotated;

use crate::envelope::{AttachmentType, Item, ItemType};
use crate::managed::{Counted, Quantities};
use crate::processing::errors::Result;
use crate::processing::errors::errors::{
    Context, ErrorRef, ErrorRefMut, ParsedError, SentryError, utils,
};

#[derive(Debug)]
pub struct Attachments {
    pub event: Annotated<Event>,
    pub attachments: Vec<Item>,
    pub user_reports: Vec<Item>,
}

impl SentryError for Attachments {
    fn try_expand(items: &mut Vec<Item>, ctx: Context<'_>) -> Result<Option<ParsedError<Self>>> {
        let ev = utils::take_item_by(items, |item| {
            item.attachment_type() == Some(&AttachmentType::EventPayload)
        });
        let b1 = utils::take_item_by(items, |item| {
            item.attachment_type() == Some(&AttachmentType::Breadcrumbs)
        });
        let b2 = utils::take_item_by(items, |item| {
            item.attachment_type() == Some(&AttachmentType::Breadcrumbs)
        });

        if ev.is_none() && b1.is_none() || b2.is_none() {
            return Ok(None);
        }

        let (event, _) = crate::services::processor::event::event_from_attachments(
            ctx.processing.config,
            ev,
            b1,
            b2,
        )?;

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

impl Counted for Attachments {
    fn quantities(&self) -> Quantities {
        self.as_ref().to_quantities()
    }
}
