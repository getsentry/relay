use super::ErrorPayload;
use relay_quotas::DataCategory;

use crate::envelope::{AttachmentItem, AttachmentType, EventItem, Item, UserReportItem};
use crate::managed::{Counted, Quantities, RecordKeeper};
use crate::processing::errors::errors::{Context, Expansion, SentryError};
use crate::processing::errors::{Result, utils};

#[derive(Debug)]
pub struct Generic;

impl SentryError for Generic {
    fn event_category(&self) -> DataCategory {
        DataCategory::Error
    }

    fn try_expand(items: &mut Vec<Item>, ctx: Context<'_>) -> Result<Option<Expansion<Self>>> {
        let Some(ev) = utils::take_item::<EventItem>(items) else {
            return Ok(None);
        };

        let fully_normalized = ev.data().fully_normalized();
        let mut metrics = Default::default();

        let mut event =
            utils::event_from_json(&ev.data().payload(), None, &mut metrics, ctx.processing)?;

        let skip_normalization = ctx.processing.is_processing()
            && ctx.envelope.meta().request_trust().is_trusted()
            && fully_normalized;
        if !skip_normalization && let Some(event) = event.value_mut() {
            // Event items can never include transactions, so retain the event type and let
            // inference deal with this during normalization.
            event.ty.set_value(None);
        }

        let mut attachments: Vec<AttachmentItem> = utils::take_items(items).collect();
        // Separate the first flamegraph from the event's attachments and drop additional graphs.
        let flamegraph = attachments
            .extract_if(.., |item| {
                item.attachment_type() == AttachmentType::Flamegraph
            })
            .fold(None, |first, item| first.or(Some(item)));

        Ok(Some(Expansion {
            event: Box::new(event),
            attachments,
            flamegraph,
            user_reports: utils::take_items::<UserReportItem>(items).collect(),
            intermediates: vec![],
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

    fn into_payload(self) -> ErrorPayload {
        ErrorPayload::default()
    }

    fn minidump_mut(&mut self) -> Option<&mut AttachmentItem> {
        None
    }
}

impl Counted for Generic {
    fn quantities(&self) -> Quantities {
        Default::default()
    }
}
