use relay_quotas::DataCategory;

use crate::envelope::{Item, ItemType, Items};
use crate::managed::{Counted, Quantities, RecordKeeper};
use crate::processing::ForwardContext;
use crate::processing::errors::Result;
use crate::processing::errors::errors::{Context, Expansion, SentryError, utils};

#[derive(Debug)]
pub enum Unreal {
    Forward {
        report_items: Items,
    },
    #[cfg(feature = "processing")]
    Process {
        minidump: Option<Item>,
        apple_crash_report: Option<Item>,
    },
}

impl SentryError for Unreal {
    fn event_category(&self) -> DataCategory {
        DataCategory::Error
    }

    fn try_expand(items: &mut Vec<Item>, ctx: Context<'_>) -> Result<Option<Expansion<Self>>> {
        // Take the unreal report, or all the items that previously already got expanded from it.
        let report_items: Items = utils::take_items_by(items, |i| {
            matches!(i.ty(), ItemType::UnrealReport) || i.is_unreal_expanded()
        });
        if report_items.is_empty() {
            return Ok(None);
        }

        let mut metrics = Default::default();

        let expansion = utils::if_not_processing!(ctx, {
            Expansion {
                event: Box::new(utils::take_event_from_crash_items(items, &mut metrics, ctx)?),
                attachments: utils::take_items_of_type(items, ItemType::Attachment),
                user_reports: utils::take_items_of_type(items, ItemType::UserReport),
                error: Self::Forward { report_items },
                metrics,
                fully_normalized: false,
            }
        } else {
            use crate::envelope::AttachmentType;
            use crate::services::processor::ProcessingError;

            let expansion = crate::utils::expand_unreal_items(report_items, ctx.processing.config)?;
            let event = expansion.event;
            let mut attachments = expansion.attachments.into_vec();
            attachments.extend(items.extract_if(.., |item| *item.ty() == ItemType::Attachment));

            // `process` later fills this event in, ideally the event is already filled in here,
            // during the expansion, it is split into two phases now, to keep compatibility with
            // the existing unreal code.
            let mut event = match utils::take_item_of_type(items, ItemType::Event).or(event) {
                Some(event) => utils::event_from_json_payload(event, None, &mut metrics, ctx)?,
                None => utils::take_event_from_attachments(&mut attachments, &mut metrics, ctx)?,
            };

            let mut user_reports: Vec<Item> = utils::take_items_of_type(items, ItemType::UserReport);

            let event_id = ctx.envelope.event_id().unwrap_or_default();
            debug_assert_ne!(event_id, Default::default(), "event id must always be set");

            let user_header = ctx
                .envelope
                .get_header(crate::constants::UNREAL_USER_HEADER)
                .and_then(|v| v.as_str());

            // After the removal of the old error/event processing pipeline, the `expand_unreal` and
            // `process_unreal` functions can be significantly reworked to no longer parse the Unreal
            // context multiple times by merging the functions into one.
            //
            // This is currently still split to avoid too many changes and code duplication at once.
            if let Some(result) =
                crate::utils::process_unreal(event_id, &mut event, &attachments, user_header)
                    .map_err(ProcessingError::InvalidUnrealReport)?
            {
                user_reports.extend(result.user_reports);
            }

            let minidump = utils::take_item_by(&mut attachments, |item| {
                item.attachment_type() == Some(AttachmentType::Minidump)
            });
            let apple_crash_report = utils::take_item_by(&mut attachments, |item| {
                item.attachment_type() == Some(AttachmentType::AppleCrashReport)
            });

            if let Some(minidump) = &minidump {
                crate::utils::process_minidump(
                    event.get_or_insert_with(Default::default),
                    &minidump.payload(),
                );
                metrics.bytes_ingested_event_minidump = (minidump.len() as u64).into();
            }
            if let Some(acr) = &apple_crash_report {
                crate::utils::process_apple_crash_report(
                    event.get_or_insert_with(Default::default)
                );
                metrics.bytes_ingested_event_applecrashreport = (acr.len() as u64).into();
            }

            Expansion {
                event: Box::new(event),
                attachments,
                user_reports,
                error: Self::Process {
                    minidump,
                    apple_crash_report,
                },
                metrics,
                fully_normalized: false,
            }
        });

        Ok(Some(expansion))
    }

    #[cfg(not(feature = "processing"))]
    fn apply_rate_limit(
        &mut self,
        _category: relay_quotas::DataCategory,
        _limits: relay_quotas::RateLimits,
        _records: &mut RecordKeeper<'_>,
    ) -> Result<()> {
        Ok(())
    }

    #[cfg(feature = "processing")]
    fn apply_rate_limit(
        &mut self,
        _category: relay_quotas::DataCategory,
        limits: relay_quotas::RateLimits,
        records: &mut RecordKeeper<'_>,
    ) -> Result<()> {
        use crate::processing::errors::Error;

        let Self::Process {
            minidump,
            apple_crash_report,
        } = self
        else {
            return Ok(());
        };

        if let Some(apple_crash_report) = apple_crash_report
            && !apple_crash_report.rate_limited()
        {
            apple_crash_report.set_rate_limited(true);
            records.reject_err(Error::RateLimited(limits.clone()), &*apple_crash_report);
        }

        if let Some(minidump) = minidump
            && !minidump.rate_limited()
        {
            minidump.set_rate_limited(true);
            records.reject_err(Error::RateLimited(limits.clone()), &*minidump);
        }

        Ok(())
    }

    fn serialize_into(self, items: &mut Vec<Item>, _ctx: ForwardContext<'_>) -> Result<()> {
        match self {
            Self::Forward {
                report_items: report,
            } => items.extend(report),
            #[cfg(feature = "processing")]
            Self::Process {
                minidump,
                apple_crash_report,
            } => {
                items.extend(minidump);
                items.extend(apple_crash_report);
            }
        }

        Ok(())
    }

    fn minidump_mut(&mut self) -> Option<&mut Item> {
        match self {
            Self::Forward { .. } => None,
            #[cfg(feature = "processing")]
            Self::Process { minidump, .. } => minidump.as_mut(),
        }
    }
}

impl Counted for Unreal {
    fn quantities(&self) -> Quantities {
        match self {
            Self::Forward { .. } => Quantities::default(),
            #[cfg(feature = "processing")]
            Self::Process {
                minidump,
                apple_crash_report,
            } => {
                let mut quantities = Quantities::default();

                if let Some(apple_crash_report) = apple_crash_report
                    && !apple_crash_report.rate_limited()
                {
                    quantities.extend(apple_crash_report.quantities());
                }
                if let Some(minidump) = minidump
                    && !minidump.rate_limited()
                {
                    quantities.extend(minidump.quantities());
                }

                quantities
            }
        }
    }
}
