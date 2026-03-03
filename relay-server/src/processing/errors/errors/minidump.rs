use relay_event_schema::protocol::Event;
use relay_protocol::Annotated;
use relay_quotas::{DataCategory, RateLimits};

use crate::envelope::{AttachmentType, Item, ItemType};
use crate::managed::{Counted, Quantities, RecordKeeper};
use crate::processing::ForwardContext;
use crate::processing::errors::errors::{Context, Expansion, SentryError, utils};
use crate::processing::errors::{Error, Result};

#[derive(Debug)]
pub struct Minidump {
    pub minidump: Item,
}

impl SentryError for Minidump {
    fn try_expand(items: &mut Vec<Item>, ctx: Context<'_>) -> Result<Option<Expansion<Self>>> {
        let Some(minidump) = utils::take_item_by(items, |item| {
            item.attachment_type() == Some(AttachmentType::Minidump)
        }) else {
            return Ok(None);
        };

        let mut metrics = Default::default();
        let mut event = utils::take_event_from_crash_items(items, &mut metrics, ctx)?;

        if !ctx.processing.is_processing() {
            return Ok(Some(Expansion {
                event: Box::new(event),
                attachments: utils::take_items_of_type(items, ItemType::Attachment),
                user_reports: utils::take_items_of_type(items, ItemType::UserReport),
                error: Self { minidump },
                metrics,
                fully_normalized: false,
            }));
        }

        crate::utils::process_minidump(
            event.get_or_insert_with(Event::default),
            &minidump.payload(),
        );
        metrics.bytes_ingested_event_minidump = Annotated::new(minidump.len() as u64);

        Ok(Some(Expansion {
            event: Box::new(event),
            attachments: utils::take_items_of_type(items, ItemType::Attachment),
            user_reports: utils::take_items_of_type(items, ItemType::UserReport),
            error: Self { minidump },
            metrics,
            fully_normalized: false,
        }))
    }

    fn apply_rate_limit(
        &mut self,
        _category: DataCategory,
        limits: RateLimits,
        records: &mut RecordKeeper<'_>,
    ) -> Result<()> {
        if !self.minidump.rate_limited() {
            self.minidump.set_rate_limited(true);
            records.reject_err(Error::RateLimited(limits), &self.minidump);
        }

        Ok(())
    }

    fn serialize_into(self, items: &mut Vec<Item>, _ctx: ForwardContext<'_>) -> Result<()> {
        items.push(self.minidump);
        Ok(())
    }

    fn minidump_mut(&mut self) -> Option<&mut Item> {
        Some(&mut self.minidump)
    }
}

impl Counted for Minidump {
    fn quantities(&self) -> Quantities {
        // A rate limited crash dump no longer counts as an attachment, but it is still passed
        // along to have its data later extracted into an error (Symbolication).
        //
        // The rate limited information is passed along and will lead to the item later to be
        // dropped.
        match self.minidump.rate_limited() {
            true => Default::default(),
            false => self.minidump.quantities(),
        }
    }
}
