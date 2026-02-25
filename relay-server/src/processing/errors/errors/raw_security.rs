use relay_event_schema::protocol::Metrics;
use relay_protocol::Annotated;

use crate::envelope::{Item, ItemType};
use crate::managed::{Counted, Quantities};
use crate::processing::errors::Result;
use crate::processing::errors::errors::{Context, ParsedError, SentryError, utils};

#[derive(Debug)]
pub struct RawSecurity {}

impl SentryError for RawSecurity {
    fn try_expand(items: &mut Vec<Item>, ctx: Context<'_>) -> Result<Option<ParsedError<Self>>> {
        let Some(item) = utils::take_item_of_type(items, ItemType::RawSecurity) else {
            return Ok(None);
        };

        let mut metrics = Metrics::default();

        let (event, len) = crate::services::processor::event::event_from_security_report(
            item,
            ctx.envelope.meta(),
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

impl Counted for RawSecurity {
    fn quantities(&self) -> Quantities {
        Default::default()
    }
}
