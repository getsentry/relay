use super::ErrorPayload;
use relay_quotas::{DataCategory, RateLimits};

use crate::envelope::{AttachmentItem, AttachmentType, Item, UserReportItem};
use crate::managed::{Counted, Quantities, RecordKeeper};
use crate::processing::errors::errors::{Context, Expansion, SentryError};
use crate::processing::errors::{Error, Result, utils};

#[derive(Debug)]
pub struct AppleCrashReport(pub AttachmentItem);

impl SentryError for AppleCrashReport {
    fn event_category(&self) -> DataCategory {
        DataCategory::Error
    }

    fn try_expand(items: &mut Vec<Item>, ctx: Context<'_>) -> Result<Option<Expansion<Self>>> {
        let Some(apple_crash_report) = utils::take_item_if::<AttachmentItem>(items, |item| {
            item.attachment_type() == AttachmentType::AppleCrashReport
        }) else {
            return Ok(None);
        };

        let mut metrics = Default::default();
        #[cfg_attr(not(feature = "processing"), expect(unused_mut))]
        let mut event = utils::take_event_from_crash_items(items, &mut metrics, ctx.processing)?;

        utils::if_processing!(ctx, {
            crate::utils::process_apple_crash_report(event.get_or_insert_with(Default::default));
            metrics.bytes_ingested_event_applecrashreport =
                (apple_crash_report.data().len() as u64).into();
        });

        Ok(Some(Expansion {
            event: Box::new(event),
            attachments: utils::take_items::<AttachmentItem>(items).collect(),
            flamegraph: None,
            user_reports: utils::take_items::<UserReportItem>(items).collect(),
            intermediates: vec![],
            error: Self(apple_crash_report),
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
        if !self.0.data().rate_limited() {
            let reason_code = limits.longest().and_then(|limit| limit.reason_code.clone());
            records.reject_err(Error::RateLimited(limits), &self.0);
            self.0.data_mut().set_rate_limited(true);
            self.0.data_mut().set_rate_limit_reason(reason_code);
        }

        Ok(())
    }

    fn into_payload(self) -> ErrorPayload {
        ErrorPayload(smallvec::smallvec![self.0])
    }

    fn payload_attachments(&self) -> smallvec::SmallVec<[&AttachmentItem; 2]> {
        smallvec::smallvec![&self.0]
    }

    fn minidump_mut(&mut self) -> Option<&mut AttachmentItem> {
        None
    }
}

impl Counted for AppleCrashReport {
    fn quantities(&self) -> Quantities {
        // A rate limited crash report no longer counts as an attachment, but it is still passed
        // along to have its data later extracted into an error (Symbolication).
        //
        // The rate limited information is passed along and will lead to the item later to be
        // dropped.
        self.0.quantities()
    }
}
