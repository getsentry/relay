use relay_base_schema::events::EventType;
use relay_quotas::DataCategory;

use crate::envelope::{Item, ItemType};
use crate::managed::{Counted, Quantities, RecordKeeper};
use crate::processing::ForwardContext;
use crate::processing::errors::Result;
use crate::processing::errors::errors::{Context, Expansion, SentryError, utils};
use crate::statsd::RelayCounters;

#[derive(Debug)]
pub struct UserReportV2;

impl SentryError for UserReportV2 {
    fn event_category(&self) -> DataCategory {
        DataCategory::UserReportV2
    }

    fn try_expand(items: &mut Vec<Item>, ctx: Context<'_>) -> Result<Option<Expansion<Self>>> {
        let Some(ev) = utils::take_item_of_type(items, ItemType::UserReportV2) else {
            return Ok(None);
        };

        let mut metrics = Default::default();

        let attachments: Vec<Item> = utils::take_items_of_type(items, ItemType::Attachment);

        relay_statsd::metric!(
            counter(RelayCounters::FeedbackAttachments) += attachments.len() as u64
        );

        Ok(Some(Expansion {
            event: Box::new(utils::event_from_json_payload(
                ev,
                EventType::UserReportV2,
                &mut metrics,
                ctx,
            )?),
            attachments,
            user_reports: Default::default(),
            error: Self,
            metrics,
            fully_normalized: false,
        }))
    }

    fn apply_rate_limit(
        &mut self,
        _category: DataCategory,
        _limits: relay_quotas::RateLimits,
        _records: &mut RecordKeeper<'_>,
    ) -> Result<()> {
        Ok(())
    }

    fn serialize_into(self, _items: &mut Vec<Item>, _ctx: ForwardContext<'_>) -> Result<()> {
        Ok(())
    }

    fn minidump_mut(&mut self) -> Option<&mut Item> {
        None
    }
}

impl Counted for UserReportV2 {
    fn quantities(&self) -> Quantities {
        Default::default()
    }
}
