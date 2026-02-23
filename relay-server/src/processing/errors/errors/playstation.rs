use relay_dynamic_config::Feature;
use relay_event_schema::protocol::Event;
use relay_protocol::Annotated;

use crate::envelope::{AttachmentType, Item, ItemType};
use crate::managed::{Counted, Quantities};
use crate::processing::errors::Result;
use crate::processing::errors::errors::{
    Context, ErrorRef, ErrorRefMut, ParsedError, SentryError, utils,
};

#[derive(Debug)]
pub struct Playstation {
    pub event: Annotated<Event>,
    pub attachments: Vec<Item>,
    pub user_reports: Vec<Item>,
}

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

        let Some(prosperodump) = utils::take_item_by(items, |item| {
            item.attachment_type() == Some(&AttachmentType::Prosperodump)
        }) else {
            return Ok(None);
        };

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
                merge_events(&event, prospero.as_bytes()).map_err(ProcessingError::InvalidJson)?
            }
            (Some(event), None) => utils::event_from_json_payload(event, None)?,
            (None, Some(prospero)) => utils::event_from_json(prospero.as_bytes(), None)?,
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

        let mut attachments = items
            .extract_if(.., |item| *item.ty() == ItemType::Attachment)
            .collect::<Vec<_>>();

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

        let error = Self {
            event,
            attachments,
            user_reports: utils::take_items_of_type(items, ItemType::UserReport),
        };

        Ok(Some(ParsedError {
            error,
            fully_normalized: false,
        }))
    }

    fn as_ref(&self) -> ErrorRef<'_> {
        ErrorRef {
            event: &self.event,
            attachments: &self.attachments,
            user_reports: &self.user_reports,
        }
    }

    fn as_ref_mut(&mut self) -> ErrorRefMut<'_> {
        ErrorRefMut {
            event: &mut self.event,
            attachments: &mut self.attachments,
            user_reports: Some(&mut self.user_reports),
        }
    }
}

impl Counted for Playstation {
    fn quantities(&self) -> Quantities {
        self.as_ref().to_quantities()
    }
}

#[cfg(sentry)]
fn merge_events(
    from_envelope: &Item,
    from_prospero: &[u8],
) -> Result<Annotated<Event>, serde_json::Error> {
    let from_envelope = serde_json::from_slice(&from_envelope.payload())?;
    let mut from_prospero = serde_json::from_slice(from_prospero)?;

    // Uses the dying message as a base and fills it with values from the event.
    crate::utils::merge_values(&mut from_prospero, from_envelope);

    Annotated::<Event>::deserialize_with_meta(from_prospero)
}
