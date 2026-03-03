use relay_event_schema::protocol::Event;
use relay_protocol::Annotated;

use crate::envelope::{AttachmentType, Item, ItemType};
use crate::managed::{Counted, Quantities};
use crate::processing::ForwardContext;
use crate::processing::errors::Result;
use crate::processing::errors::errors::{Context, ParsedError, SentryError, utils};
use crate::services::processor::ProcessingError;

#[derive(Debug)]
pub enum Unreal {
    Forward {
        report: Box<Item>,
    },
    Process {
        minidump: Option<Item>,
        apple_crash_report: Option<Item>,
    },
}

impl SentryError for Unreal {
    fn try_expand(items: &mut Vec<Item>, ctx: Context<'_>) -> Result<Option<ParsedError<Self>>> {
        let Some(report) = utils::take_item_of_type(items, ItemType::UnrealReport) else {
            return Ok(None);
        };

        let mut metrics = Default::default();

        if !ctx.processing.is_processing() {
            return Ok(Some(ParsedError {
                event: Annotated::empty(),
                attachments: utils::take_items_of_type(items, ItemType::Attachment),
                user_reports: utils::take_items_of_type(items, ItemType::UserReport),
                error: Self::Forward {
                    report: Box::new(report),
                },
                metrics,
                fully_normalized: false,
            }));
        }

        let expansion = crate::utils::expand_unreal(report, ctx.processing.config)?;
        let event = expansion.event;
        let mut attachments = expansion.attachments.into_vec();

        let mut event = match utils::take_item_of_type(items, ItemType::Event).or(event) {
            Some(event) => utils::event_from_json_payload(event, None, &mut metrics, ctx)?,
            // `process` later fills this event in, ideally the event is already filled in here,
            // during the expansion, it is split into two phases now, to keep compatibility with
            // the existing unreal code.
            None => Annotated::empty(),
        };

        attachments.extend(items.extract_if(.., |item| *item.ty() == ItemType::Attachment));

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

        let minidump = utils::take_item_by(items, |item| {
            item.attachment_type() == Some(&AttachmentType::Minidump)
        });
        let apple_crash_report = utils::take_item_by(items, |item| {
            item.attachment_type() == Some(&AttachmentType::AppleCrashReport)
        });

        if let Some(minidump) = &minidump {
            crate::utils::process_minidump(
                event.get_or_insert_with(Event::default),
                &minidump.payload(),
            );
            metrics.bytes_ingested_event_minidump = Annotated::new(minidump.len() as u64);
        }
        if let Some(acr) = &apple_crash_report {
            crate::utils::process_apple_crash_report(
                event.get_or_insert_with(Event::default),
                &acr.payload(),
            );
            metrics.bytes_ingested_event_applecrashreport = Annotated::new(acr.len() as u64);
        }

        Ok(Some(ParsedError {
            event,
            attachments,
            user_reports,
            error: Self::Process {
                minidump,
                apple_crash_report,
            },
            metrics,
            fully_normalized: false,
        }))
    }

    fn serialize_into(self, items: &mut Vec<Item>, _ctx: ForwardContext<'_>) -> Result<()> {
        match self {
            Self::Forward { report } => items.push(*report),
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
            Self::Process { minidump, .. } => minidump.as_mut(),
        }
    }
}

impl Counted for Unreal {
    fn quantities(&self) -> Quantities {
        // Like minidumps the crash represents the error and is not counted as an attachment or an
        // additional type.
        Default::default()
    }
}
