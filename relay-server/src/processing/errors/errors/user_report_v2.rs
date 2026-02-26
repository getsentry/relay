use relay_base_schema::events::EventType;

use crate::envelope::{Item, ItemType};
use crate::managed::{Counted, Quantities};
use crate::processing::errors::Result;
use crate::processing::errors::errors::{Context, ParsedError, SentryError, utils};
use crate::statsd::RelayCounters;

#[derive(Debug)]
pub struct UserReportV2 {}

impl SentryError for UserReportV2 {
    fn try_expand(items: &mut Vec<Item>, ctx: Context<'_>) -> Result<Option<ParsedError<Self>>> {
        let Some(ev) = utils::take_item_of_type(items, ItemType::UserReportV2) else {
            return Ok(None);
        };

        let mut metrics = Default::default();

        let attachments: Vec<Item> = utils::take_items_of_type(items, ItemType::Attachment);

        relay_statsd::metric!(
            counter(RelayCounters::FeedbackAttachments) += attachments.len() as u64
        );

        Ok(Some(ParsedError {
            event: utils::event_from_json_payload(ev, EventType::UserReportV2, &mut metrics, ctx)?,
            attachments,
            user_reports: Default::default(),
            error: Self {},
            metrics,
            fully_normalized: false,
        }))
    }
}

impl Counted for UserReportV2 {
    fn quantities(&self) -> Quantities {
        Default::default()
    }
}
