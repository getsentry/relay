use relay_event_schema::protocol::Event;
use relay_protocol::Annotated;

use crate::envelope::{AttachmentType, Item, ItemType};
use crate::managed::{Counted, Quantities};
use crate::processing::errors::Result;
use crate::processing::errors::errors::{
    Context, ErrorRef, ErrorRefMut, ParsedError, SentryError, utils,
};

#[derive(Debug)]
pub struct AppleCrashReport {
    pub event: Annotated<Event>,
    pub attachments: Vec<Item>,
    pub user_reports: Vec<Item>,
}

impl SentryError for AppleCrashReport {
    fn try_expand(items: &mut Vec<Item>, _ctx: Context<'_>) -> Result<Option<ParsedError<Self>>> {
        let Some(apple_crash_report) = utils::take_item_by(items, |item| {
            item.attachment_type() == Some(&AttachmentType::AppleCrashReport)
        }) else {
            return Ok(None);
        };

        let mut event = match utils::take_item_of_type(items, ItemType::Event) {
            Some(event) => utils::event_from_json_payload(event, None)?,
            None => Annotated::empty(),
        };

        // TODO: write metrics
        crate::utils::process_apple_crash_report(
            event.get_or_insert_with(Event::default),
            &apple_crash_report.payload(),
        );

        let mut attachments = items
            .extract_if(.., |item| *item.ty() == ItemType::Attachment)
            .collect::<Vec<_>>();
        attachments.push(apple_crash_report);

        let error = Self {
            event,
            attachments,
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

impl Counted for AppleCrashReport {
    fn quantities(&self) -> Quantities {
        self.as_ref().to_quantities()
    }
}
