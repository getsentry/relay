use relay_quotas::DataCategory;

use crate::envelope::{Item, ItemType};
use crate::managed::{Counted, Quantities, RecordKeeper};
use crate::processing::ForwardContext;
use crate::processing::errors::Result;
use crate::processing::errors::errors::{Context, Expansion, SentryError, utils};

#[derive(Debug)]
pub struct Generic;

impl SentryError for Generic {
    fn event_category(&self) -> DataCategory {
        DataCategory::Error
    }

    fn try_expand(items: &mut Vec<Item>, ctx: Context<'_>) -> Result<Option<Expansion<Self>>> {
        let Some(ev) = utils::take_item_of_type(items, ItemType::Event) else {
            return Ok(None);
        };

        let fully_normalized = ev.fully_normalized();
        let mut metrics = Default::default();

        let mut event = utils::event_from_json_payload(ev, None, &mut metrics, ctx)?;

        let skip_normalization = ctx.processing.is_processing() && fully_normalized;
        if !skip_normalization && let Some(event) = event.value_mut() {
            // Event items can never include transactions, so retain the event type and let
            // inference deal with this during normalization.
            event.ty.set_value(None);
        }

        Ok(Some(Expansion {
            event: Box::new(event),
            attachments: utils::take_items_of_type(items, ItemType::Attachment),
            user_reports: utils::take_items_of_type(items, ItemType::UserReport),
            error: Self,
            metrics,
            fully_normalized,
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

impl Counted for Generic {
    fn quantities(&self) -> Quantities {
        Default::default()
    }
}
