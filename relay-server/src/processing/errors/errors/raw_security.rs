use relay_event_schema::protocol::Metrics;
use relay_protocol::Annotated;
use relay_quotas::DataCategory;

use crate::envelope::{Item, ItemType};
use crate::managed::{Counted, Quantities, RecordKeeper};
use crate::processing::ForwardContext;
use crate::processing::errors::Result;
use crate::processing::errors::errors::{Context, Expansion, SentryError, utils};

#[derive(Debug)]
pub struct RawSecurity;

impl SentryError for RawSecurity {
    fn event_category(&self) -> DataCategory {
        DataCategory::Security
    }

    fn try_expand(items: &mut Vec<Item>, ctx: Context<'_>) -> Result<Option<Expansion<Self>>> {
        let Some(item) = utils::take_item_of_type(items, ItemType::RawSecurity) else {
            return Ok(None);
        };

        let mut metrics = Metrics::default();

        let (event, len) = crate::services::processor::event::event_from_security_report(
            item,
            ctx.envelope.meta(),
        )?;

        metrics.bytes_ingested_event = Annotated::new(len as u64);

        Ok(Some(Expansion {
            event: Box::new(event),
            attachments: utils::take_items_of_type(items, ItemType::Attachment),
            user_reports: utils::take_items_of_type(items, ItemType::UserReport),
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

impl Counted for RawSecurity {
    fn quantities(&self) -> Quantities {
        Default::default()
    }
}
