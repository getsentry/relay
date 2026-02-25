use relay_event_schema::protocol::Event;
use relay_protocol::Annotated;

use crate::envelope::{AttachmentType, Item, ItemType};
use crate::managed::{Counted, Quantities};
use crate::processing::errors::Result;
use crate::processing::errors::errors::{Context, ParsedError, SentryError, utils};

#[derive(Debug)]
pub struct Minidump {}

impl SentryError for Minidump {
    fn try_expand(items: &mut Vec<Item>, ctx: Context<'_>) -> Result<Option<ParsedError<Self>>> {
        let Some(minidump) = utils::take_item_by(items, |item| {
            item.attachment_type() == Some(&AttachmentType::Minidump)
        }) else {
            return Ok(None);
        };

        let mut metrics = Default::default();

        // TODO: this is copy pasta and can be nicer
        if !ctx.processing.is_processing() {
            let mut attachments: Vec<_> = utils::take_items_of_type(items, ItemType::Attachment);
            attachments.push(minidump);

            return Ok(Some(ParsedError {
                event: utils::try_take_parsed_event(items, &mut metrics, ctx)?,
                attachments,
                user_reports: utils::take_items_of_type(items, ItemType::UserReport),
                error: Self {},
                metrics,
                fully_normalized: false,
            }));
        }

        let mut event = match utils::take_item_of_type(items, ItemType::Event) {
            Some(event) => utils::event_from_json_payload(event, None, &mut metrics, ctx)?,
            None => Annotated::empty(),
        };

        crate::utils::process_minidump(
            event.get_or_insert_with(Event::default),
            &minidump.payload(),
        );
        metrics.bytes_ingested_event_minidump = Annotated::new(minidump.len() as u64);

        let mut attachments = items
            .extract_if(.., |item| *item.ty() == ItemType::Attachment)
            .collect::<Vec<_>>();
        attachments.push(minidump);

        Ok(Some(ParsedError {
            event,
            attachments,
            user_reports: utils::take_items_of_type(items, ItemType::UserReport),
            error: Self {},
            metrics,
            fully_normalized: false,
        }))
    }
}

impl Counted for Minidump {
    fn quantities(&self) -> Quantities {
        Default::default()
    }
}
