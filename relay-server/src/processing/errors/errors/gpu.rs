use relay_event_schema::protocol::Event;
use relay_protocol::{Annotated, Value};
use relay_quotas::{DataCategory, RateLimits};

use crate::envelope::{AttachmentType, Item, ItemType};
use crate::managed::{Counted, Quantities, RecordKeeper};
use crate::processing::ForwardContext;
use crate::processing::errors::errors::{Context, Expansion, SentryError};
use crate::processing::errors::{Error, Result, utils};

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
        let event = utils::take_event_from_crash_items(items, &mut metrics, ctx.processing)?;

        Ok(Some(Expansion {
            event: Box::new(event),
            attachments: utils::take_items_of_type(items, ItemType::Attachment),
            user_reports: utils::take_items_of_type(items, ItemType::UserReport),
            intermediates: vec![],
            error: Self(dump),
            metrics,
            fully_normalized: false,
        }))
    }

    fn apply_rate_limit(
        &mut self,
        _category: DataCategory,
        limits: RateLimits,
        event: &mut Annotated<Event>,
        records: &mut RecordKeeper<'_>,
    ) -> Result<()> {
        let reason_code = limits.longest().and_then(|limit| limit.reason_code.clone());

        if !self.0.rate_limited() {
            records.reject_err(Error::RateLimited(limits), &self.0);
            self.0.set_rate_limited(true);

            if let Some(event) = event.value_mut()
                && let Some(reason_code) = reason_code
            {
                let metadata = event.metadata.get_or_insert_with(Default::default);
                metadata.insert(
                    "rate_limited_reason".to_owned(),
                    Annotated::new(Value::from(reason_code.as_str().to_owned())),
                );
            }
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
