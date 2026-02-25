use relay_event_schema::protocol::Metrics;
use relay_protocol::Annotated;

use crate::envelope::{AttachmentType, Item, ItemType};
use crate::managed::{Counted, Quantities};
use crate::processing::errors::Result;
use crate::processing::errors::errors::{Context, ParsedError, SentryError, utils};

#[derive(Debug)]
pub struct Attachments {}

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

        let mut metrics = Metrics::default();

        let (event, len) = crate::services::processor::event::event_from_attachments(
            ctx.processing.config,
            ev,
            b1,
            b2,
        )?;
        metrics.bytes_ingested_event = Annotated::new(len as u64);

        Ok(Some(ParsedError {
            event,
            attachments: utils::take_items_of_type(items, ItemType::Attachment),
            user_reports: utils::take_items_of_type(items, ItemType::UserReport),
            error: Self {},
            metrics,
            fully_normalized: false,
        }))
    }
}

impl Counted for Attachments {
    fn quantities(&self) -> Quantities {
        Default::default()
    }
}
