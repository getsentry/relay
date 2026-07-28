use relay_quotas::{DataCategory, RateLimits};

use crate::envelope::{AttachmentType, Item, ItemType};
use crate::managed::{Counted, Quantities, RecordKeeper};
use crate::processing::ForwardContext;
use crate::processing::errors::errors::{Context, Expansion, SentryError, utils};
use crate::processing::errors::{Error, Result};

/// An NVIDIA Aftermath GPU crash dump (`.nv-gpudmp`).
///
/// Relay splits the GPU crash onto its own event (see [`crate::utils::gpu`]),
/// which carries a copy of the CPU event's scope plus the dump and any shader
/// debug info (`.nvdbg`). Here we turn the copied scope into the event and keep
/// the dump and shader debug info as attachments; Sentry decodes the dump
/// out-of-band (via teapot), analogous to how a minidump is symbolicated.
///
/// The scope is either the crashpad `__sentry-event` payload / breadcrumbs (native
/// crashes) or an Unreal context whose `__sentry` game data holds the event JSON;
/// the latter is promoted to an event before the shared crash-event assembly runs.
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

        // Unreal crashes carry the Sentry event payload in the `__sentry` game-data
        // key of the copied Unreal context, not a standalone event item. Promote it
        // to an event so the shared crash-event assembly below builds the GPU event
        // from it, just like the crashpad `__sentry-event` payload.
        if let Some(context) = utils::take_item_by(items, |item| {
            item.attachment_type() == Some(AttachmentType::UnrealContext)
        }) && let Some(event) = crate::utils::event_item_from_unreal_context(&context.payload())?
        {
            items.push(event);
        }

        let event = utils::take_event_from_crash_items(items, &mut metrics, ctx)?;

        Ok(Some(Expansion {
            event: Box::new(event),
            // The remaining attachments include the shader debug info (`.nvdbg`);
            // the dump itself is kept via `serialize_into` below.
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
        // A rate limited dump no longer counts as an attachment, but it is still
        // passed along so Sentry can decode it into the event later.
        match self.0.rate_limited() {
            true => Default::default(),
            false => self.0.quantities(),
        }
    }
}
