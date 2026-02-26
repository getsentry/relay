use relay_dynamic_config::Feature;
use relay_event_schema::protocol::Event;
use relay_protocol::Annotated;

use crate::envelope::{AttachmentType, Item, ItemType};
use crate::managed::{Counted, Quantities};
use crate::processing::errors::Result;
use crate::processing::errors::errors::{Context, ParsedError, SentryError, utils};

// TODO: compare with nnswitch and decide whether we should use enums
#[derive(Debug)]
pub struct Playstation {}

impl SentryError for Playstation {
    #[cfg(not(sentry))]
    fn try_expand(_items: &mut Vec<Item>, _ctx: Context<'_>) -> Result<Option<ParsedError<Self>>> {
        Ok(None)
    }

    #[cfg(sentry)]
    fn try_expand(items: &mut Vec<Item>, ctx: Context<'_>) -> Result<Option<ParsedError<Self>>> {
        use crate::constants::SENTRY_CRASH_PAYLOAD_KEY;
        use crate::services::processor::ProcessingError;
        use crate::statsd::RelayCounters;

        if ctx.processing.should_filter(Feature::PlaystationIngestion) {
            return Ok(None);
        }

        let mut metrics = Default::default();

        let Some(prosperodump) = utils::take_item_by(items, |item| {
            item.attachment_type() == Some(&AttachmentType::Prosperodump)
        }) else {
            return Ok(None);
        };

        if !ctx.processing.is_processing() {
            let mut attachments: Vec<_> = utils::take_items_of_type(items, ItemType::Attachment);
            attachments.push(prosperodump);

            return Ok(Some(ParsedError {
                event: utils::try_take_parsed_event(items, &mut metrics, ctx)?,
                attachments,
                user_reports: utils::take_items_of_type(items, ItemType::UserReport),
                error: Self {},
                metrics,
                fully_normalized: false,
            }));
        }

        relay_statsd::metric!(counter(RelayCounters::PlaystationProcessing) += 1);

        let data = relay_prosperoconv::extract_data(&prosperodump.payload()).map_err(|err| {
            ProcessingError::InvalidPlaystationDump(format!("Failed to extract data: {err}"))
        })?;
        let prospero_dump = relay_prosperoconv::ProsperoDump::parse(&data).map_err(|err| {
            ProcessingError::InvalidPlaystationDump(format!("Failed to parse dump: {err}"))
        })?;
        let minidump_buffer = relay_prosperoconv::write_dump(&prospero_dump).map_err(|err| {
            ProcessingError::InvalidPlaystationDump(format!("Failed to create minidump: {err}"))
        })?;

        let event = utils::take_item_of_type(items, ItemType::Event);
        let prospero_event = prospero_dump.userdata.get(SENTRY_CRASH_PAYLOAD_KEY);

        let mut event = match (event, prospero_event) {
            (Some(event), Some(prospero)) => {
                metrics.bytes_ingested_event =
                    Annotated::new((event.len() + prospero.len()) as u64);
                merge_events(&event, prospero.as_bytes(), ctx)?
            }
            (Some(event), None) => utils::event_from_json_payload(event, None, &mut metrics, ctx)?,
            (None, Some(prospero)) => {
                utils::event_from_json(prospero.as_bytes(), None, &mut metrics, ctx)?
            }
            (None, None) => Annotated::empty(),
        };

        // If "__sentry" is not a key in the userdata do the legacy extraction.
        // This should be removed once all customers migrated to the new format.
        if prospero_event.is_none() {
            crate::services::processor::playstation::legacy_userdata_extraction(
                event.get_or_insert_with(Default::default),
                &prospero_dump,
            );
        }
        crate::services::processor::playstation::merge_playstation_context(
            event.get_or_insert_with(Default::default),
            &prospero_dump,
        );

        crate::utils::process_minidump(event.get_or_insert_with(Event::default), &minidump_buffer);
        metrics.bytes_ingested_event_minidump = Annotated::new(minidump_buffer.len() as u64);

        let mut attachments = items
            .extract_if(.., |item| *item.ty() == ItemType::Attachment)
            .collect::<Vec<_>>();

        attachments.push(prosperodump);
        attachments.push({
            let mut item = Item::new(ItemType::Attachment);
            item.set_filename("generated_minidump.dmp");
            item.set_payload(crate::envelope::ContentType::Minidump, minidump_buffer);
            item.set_attachment_type(AttachmentType::Minidump);
            item
        });

        attachments.extend(prospero_dump.files.iter().map(|file| {
            let mut item = Item::new(ItemType::Attachment);
            item.set_filename(file.name);
            item.set_attachment_type(AttachmentType::Attachment);
            item.set_payload(
                crate::services::processor::playstation::infer_content_type(file.name),
                file.contents.to_owned(),
            );
            item
        }));

        let console_log = {
            let mut console_log = prospero_dump.system_log.into_owned();
            console_log.extend(prospero_dump.log_lines);
            console_log
        };
        if !console_log.is_empty() {
            attachments.push({
                let mut item = Item::new(ItemType::Attachment);
                item.set_filename("console.log");
                item.set_payload(crate::envelope::ContentType::Text, console_log.into_bytes());
                item.set_attachment_type(AttachmentType::Attachment);
                item
            })
        }

        Ok(Some(ParsedError {
            event,
            attachments,
            user_reports: utils::take_items_of_type(items, ItemType::UserReport),
            error: Self {},
            metrics,
            fully_normalized: false,
        }))
    }
}

impl Counted for Playstation {
    fn quantities(&self) -> Quantities {
        Default::default()
    }
}

#[cfg(sentry)]
fn merge_events(
    from_envelope: &Item,
    from_prospero: &[u8],
    ctx: Context<'_>,
) -> Result<Annotated<Event>> {
    use crate::services::{outcome::DiscardItemType, processor::ProcessingError};

    if from_envelope.len().max(from_prospero.len()) > ctx.processing.config.max_event_size() {
        return Err(ProcessingError::PayloadTooLarge(DiscardItemType::Event).into());
    }

    merge_events_inner(from_envelope, from_prospero)
        .map_err(ProcessingError::InvalidJson)
        .map_err(Into::into)
}

#[cfg(sentry)]
fn merge_events_inner(
    from_envelope: &Item,
    from_prospero: &[u8],
) -> Result<Annotated<Event>, serde_json::Error> {
    let from_envelope = serde_json::from_slice(&from_envelope.payload())?;
    let mut from_prospero = serde_json::from_slice(from_prospero)?;

    // Uses the dying message as a base and fills it with values from the event.
    crate::utils::merge_values(&mut from_prospero, from_envelope);

    Annotated::<Event>::deserialize_with_meta(from_prospero)
}
