use relay_quotas::{DataCategory, RateLimits};

use crate::envelope::{AttachmentType, Item, ItemType};
use crate::managed::{Counted, Quantities, RecordKeeper};
use crate::processing::ForwardContext;
use crate::processing::errors::errors::{Context, Expansion, SentryError, utils};
use crate::processing::errors::{Error, Result};

/// An NVIDIA Aftermath GPU crash dump
///
/// Builds the event from the envelope's scope and keeps the dump and any shader debug
/// info as attachments, to be symbolicated later like a minidump.
#[derive(Debug)]
pub struct GpuCrash(pub Item);

impl SentryError for GpuCrash {
    fn event_category(&self) -> DataCategory {
        DataCategory::Error
    }

    fn try_expand(items: &mut Vec<Item>, ctx: Context<'_>) -> Result<Option<Expansion<Self>>> {
        let Some(dump) = utils::take_item_by(items, |item| {
            item.attachment_type() == Some(AttachmentType::NvGpuDump)
        }) else {
            return Ok(None);
        };

        let mut metrics = Default::default();
        let event = utils::take_event_from_crash_items(items, &mut metrics, ctx)?;

        Ok(Some(Expansion {
            event: Box::new(event),
            attachments: utils::take_items_of_type(items, ItemType::Attachment),
            user_reports: utils::take_items_of_type(items, ItemType::UserReport),
            error: Self(dump),
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
        if !self.0.rate_limited() {
            self.0.set_rate_limited(true);
            records.reject_err(Error::RateLimited(limits), &self.0);
        }

        Ok(())
    }

    fn serialize_into(self, items: &mut Vec<Item>, _ctx: ForwardContext<'_>) -> Result<()> {
        items.push(self.0);
        Ok(())
    }

    fn minidump_mut(&mut self) -> Option<&mut Item> {
        None
    }
}

impl Counted for GpuCrash {
    fn quantities(&self) -> Quantities {
        match self.0.rate_limited() {
            true => Default::default(),
            false => self.0.quantities(),
        }
    }
}
