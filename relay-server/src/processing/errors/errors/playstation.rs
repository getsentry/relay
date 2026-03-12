#[cfg(all(sentry, feature = "processing"))]
use relay_event_schema::protocol::Event;
#[cfg(all(sentry, feature = "processing"))]
use relay_protocol::Annotated;
use relay_quotas::{DataCategory, RateLimits};

use crate::envelope::Item;
use crate::managed::{Counted, Quantities, RecordKeeper};
use crate::processing::ForwardContext;
use crate::processing::errors::errors::{Context, Expansion, SentryError};
use crate::processing::errors::{Error, Result};

#[derive(Debug)]
pub struct Playstation {
    prosperodump: Item,
    minidump: Option<Item>,
}

impl SentryError for Playstation {
    fn event_category(&self) -> DataCategory {
        DataCategory::Error
    }

    #[cfg(not(sentry))]
    fn try_expand(_items: &mut Vec<Item>, _ctx: Context<'_>) -> Result<Option<Expansion<Self>>> {
        Ok(None)
    }

    #[cfg(sentry)]
    fn try_expand(items: &mut Vec<Item>, ctx: Context<'_>) -> Result<Option<Expansion<Self>>> {
        use crate::envelope::{AttachmentType, ItemType};
        use crate::processing::errors::errors::utils;
        use relay_dynamic_config::Feature;
        use relay_event_schema::protocol::Metrics;

        if ctx.processing.should_filter(Feature::PlaystationIngestion) {
            return Ok(None);
        }

        let mut metrics = Metrics::default();

        let Some(prosperodump) = utils::take_item_by(items, |item| {
            item.attachment_type() == Some(AttachmentType::Prosperodump)
        }) else {
            return Ok(None);
        };

        let user_reports = utils::take_items_of_type(items, ItemType::UserReport);
        #[cfg_attr(not(feature = "processing"), expect(unused_mut))]
        let mut attachments: Vec<_> = utils::take_items_of_type(items, ItemType::Attachment);

        let (event, minidump) = utils::if_not_processing!(ctx, {
            let event = Box::new(utils::take_parsed_event(items, &mut metrics, ctx)?);
            (event, None)
        } else {
            use crate::constants::SENTRY_CRASH_PAYLOAD_KEY;
            use crate::services::processor::ProcessingError;
            use crate::statsd::RelayCounters;

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
                        ((event.len() + prospero.len()) as u64).into();
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

            let minidump = {
                let mut item = Item::new(ItemType::Attachment);
                item.set_filename("generated_minidump.dmp");
                item.set_payload(crate::envelope::ContentType::Minidump, minidump_buffer);
                item.set_attachment_type(AttachmentType::Minidump);
                // If the original prosperodump is already rate limited, so will be the minidump.
                item.set_rate_limited(prosperodump.rate_limited());
                item
            };

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

            (Box::new(event), Some(minidump))
        });

        Ok(Some(Expansion {
            event,
            attachments,
            user_reports,
            error: Self {
                prosperodump,
                minidump,
            },
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
        if !self.prosperodump.rate_limited() {
            self.prosperodump.set_rate_limited(true);
            records.reject_err(Error::RateLimited(limits.clone()), &self.prosperodump);
        }

        if let Some(minidump) = self.minidump.as_mut()
            && !minidump.rate_limited()
        {
            minidump.set_rate_limited(true);
            records.reject_err(Error::RateLimited(limits), &*minidump);
        }

        Ok(())
    }

    fn serialize_into(self, items: &mut Vec<Item>, _ctx: ForwardContext<'_>) -> Result<()> {
        items.push(self.prosperodump);
        items.extend(self.minidump);
        Ok(())
    }

    fn minidump_mut(&mut self) -> Option<&mut Item> {
        self.minidump.as_mut()
    }
}

impl Counted for Playstation {
    fn quantities(&self) -> Quantities {
        let mut quantities = Quantities::default();

        // A rate limited crash dump no longer counts as an attachment, but it is still passed
        // along to have its data later extracted into an error (Symbolication).
        //
        // The rate limited information is passed along and will lead to the item later to be
        // dropped.
        if !self.prosperodump.rate_limited() {
            quantities.extend(self.prosperodump.quantities());
        }
        if let Some(minidump) = &self.minidump
            && !minidump.rate_limited()
        {
            quantities.extend(minidump.quantities());
        }
        quantities
    }
}

#[cfg(all(sentry, feature = "processing"))]
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

#[cfg(all(sentry, feature = "processing"))]
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
