use super::ErrorPayload;
use relay_quotas::{DataCategory, RateLimits};

use crate::envelope::{AttachmentItem, AttachmentType, Item, UserReportItem};
use crate::managed::{Counted, Quantities, RecordKeeper};
use crate::processing::errors::errors::{Context, Expansion, SentryError};
use crate::processing::errors::{Error, Result, utils};

/// An NVIDIA Aftermath GPU crash dump
///
/// Builds the event from the envelope's scope and keeps the dump and any shader debug
/// info as attachments, to be symbolicated later like a minidump.
///
/// In processing mode, this creates the event if there is no scope and adds a placeholder
/// exception.
#[derive(Debug)]
pub struct GpuCrash(pub AttachmentItem);

impl SentryError for GpuCrash {
    fn event_category(&self) -> DataCategory {
        DataCategory::Error
    }

    fn try_expand(items: &mut Vec<Item>, ctx: Context<'_>) -> Result<Option<Expansion<Self>>> {
        let Some(dump) = utils::take_payload_if::<AttachmentItem>(items, |item| {
            item.attachment_type() == Some(AttachmentType::NvGpuDump)
        }) else {
            return Ok(None);
        };

        let mut metrics = Default::default();
        #[cfg_attr(not(feature = "processing"), expect(unused_mut))]
        let mut event = utils::take_event_from_crash_items(items, &mut metrics, ctx.processing)?;

        utils::if_processing!(ctx, {
            crate::utils::process_gpu_crash(event.get_or_insert_with(Default::default));
        });

        Ok(Some(Expansion {
            event: Box::new(event),
            attachments: utils::take_payloads::<AttachmentItem>(items).collect(),
            user_reports: utils::take_payloads::<UserReportItem>(items).collect(),
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
        records: &mut RecordKeeper<'_>,
    ) -> Result<()> {
        if !self.0.rate_limited() {
            records.reject_err(Error::RateLimited(limits), &self.0);
            self.0.set_rate_limited(true);
        }

        Ok(())
    }

    fn into_payload(self) -> ErrorPayload {
        ErrorPayload(smallvec::smallvec![self.0])
    }

    fn minidump_mut(&mut self) -> Option<&mut AttachmentItem> {
        None
    }
}

impl Counted for GpuCrash {
    fn quantities(&self) -> Quantities {
        self.0.quantities()
    }
}
