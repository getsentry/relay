//! Nintendo Switch crash reports processor related code.
//!
//! These functions are included only in the processing mode.

use super::ErrorPayload;

#[cfg(sentry)]
use bytes::Bytes;
#[cfg(sentry)]
use relay_event_schema::protocol::Event;
#[cfg(sentry)]
use relay_protocol::Annotated;
use relay_quotas::DataCategory;

use crate::envelope::{AttachmentItem, Item};
#[cfg(sentry)]
use crate::envelope::{EventItem, SessionItem};
use crate::managed::{Counted, Quantities, RecordKeeper};
use crate::processing::errors::Result;
use crate::processing::errors::errors::{Context, Expansion, SentryError};
#[cfg(sentry)]
use crate::processing::errors::utils;
#[cfg(sentry)]
use crate::services::processor::ProcessingError;

#[derive(Debug)]
#[cfg_attr(not(sentry), expect(dead_code))]
pub enum Nswitch {
    Forward { dying_message: AttachmentItem },
    Process,
}

impl SentryError for Nswitch {
    fn event_category(&self) -> DataCategory {
        DataCategory::Error
    }

    #[cfg(not(sentry))]
    fn try_expand(_items: &mut Vec<Item>, _ctx: Context<'_>) -> Result<Option<Expansion<Self>>> {
        Ok(None)
    }

    #[cfg(sentry)]
    fn try_expand(items: &mut Vec<Item>, ctx: Context<'_>) -> Result<Option<Expansion<Self>>> {
        use crate::envelope::{AttachmentType, UserReportItem};

        let Some(dying_message) = utils::take_item_if::<AttachmentItem>(items, |item| {
            item.attachment_type() == AttachmentType::NintendoSwitchDyingMessage
        }) else {
            return Ok(None);
        };

        let mut metrics = Default::default();

        let attachments: Vec<_> = utils::take_items::<AttachmentItem>(items).collect();
        let user_reports = utils::take_items::<UserReportItem>(items).collect();

        if !ctx.processing.is_processing() {
            return Ok(Some(Expansion {
                event: Box::new(utils::take_parsed_event(
                    items,
                    &mut metrics,
                    ctx.processing,
                )?),
                attachments,
                flamegraph: None,
                user_reports,
                intermediates: vec![],
                error: Self::Forward { dying_message },
                metrics,
                fully_normalized: false,
            }));
        }

        let event = utils::take_item::<EventItem>(items);

        let dying_message = expand_dying_message(dying_message.data().payload())
            .map_err(ProcessingError::InvalidNintendoDyingMessage)?;

        let mut attachments = attachments;
        attachments.extend(dying_message.attachments);

        #[cfg_attr(not(feature = "processing"), expect(unused_mut))]
        let mut event = match (event, dying_message.event) {
            (Some(event), Some(dying_message)) => {
                metrics.bytes_ingested_event =
                    Annotated::new((event.data().len() + dying_message.data().len()) as u64);
                merge_events(event, dying_message, ctx)?
            }
            (Some(event), None) => {
                utils::event_from_json(&event.data().payload(), None, &mut metrics, ctx.processing)?
            }
            (None, Some(event)) => {
                utils::event_from_json(&event.data().payload(), None, &mut metrics, ctx.processing)?
            }
            (None, None) => return Err(ProcessingError::NoEventPayload.into()),
        };

        // Reshape switch crash title to match other native platforms, and mark it as fatal.
        utils::if_processing!(ctx, {
            if let Some(event) = event.value_mut() {
                crate::utils::reshape_switch_crash(event);
            }
        });

        Ok(Some(Expansion {
            event: Box::new(event),
            attachments,
            flamegraph: None,
            user_reports,
            intermediates: dying_message
                .session_updates
                .into_iter()
                .map(Item::from)
                .collect(),
            error: Self::Process {},
            metrics,
            fully_normalized: false,
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
        ErrorPayload(match self {
            Self::Forward { dying_message } => smallvec::smallvec![dying_message],
            Self::Process => Default::default(),
        })
    }

    fn minidump_mut(&mut self) -> Option<&mut AttachmentItem> {
        None
    }
}

impl Counted for Nswitch {
    fn quantities(&self) -> Quantities {
        match self {
            // The item rules own the dying message's zero attachment contribution.
            Self::Forward { dying_message } => dying_message.quantities(),
            // Processing has already consumed the message into the event.
            Self::Process => Quantities::new(),
        }
    }
}

/// An error returned when parsing the dying message attachment.
#[cfg(sentry)]
#[derive(Debug, thiserror::Error)]
pub enum SwitchProcessingError {
    #[error("envelope parsing failed")]
    EnvelopeParsing(#[from] crate::envelope::EnvelopeError),
    #[error("invalid dying message")]
    DyingMessage(#[from] relay_private::nintendo::Error),
}

#[cfg(sentry)]
fn merge_events(
    from_envelope: EventItem,
    from_dying_messages: EventItem,
    ctx: Context<'_>,
) -> Result<Annotated<Event>> {
    use crate::services::outcome::DiscardItemType;

    if from_envelope
        .data()
        .len()
        .max(from_dying_messages.data().len())
        > ctx.processing.config.max_event_size()
    {
        return Err(ProcessingError::PayloadTooLarge(DiscardItemType::Event).into());
    }

    merge_events_inner(from_envelope, from_dying_messages)
        .map_err(ProcessingError::InvalidJson)
        .map_err(Into::into)
}

#[cfg(sentry)]
fn merge_events_inner(
    from_envelope: EventItem,
    from_dying_messages: EventItem,
) -> Result<Annotated<Event>, serde_json::Error> {
    let from_envelope = serde_json::from_slice(&from_envelope.data().payload())?;
    let mut from_dying_message = serde_json::from_slice(&from_dying_messages.data().payload())?;

    // Uses the dying message as a base and fills it with values from the event.
    crate::utils::merge_values(&mut from_dying_message, from_envelope);

    Annotated::<Event>::deserialize_with_meta(from_dying_message)
}

#[cfg(sentry)]
#[derive(Debug, Default)]
struct ExpandedDyingMessage {
    event: Option<EventItem>,
    attachments: Vec<AttachmentItem>,
    session_updates: Vec<SessionItem>,
}

/// Parses DyingMessage contents and updates the envelope.
#[cfg(sentry)]
fn expand_dying_message(payload: Bytes) -> Result<ExpandedDyingMessage, SwitchProcessingError> {
    let data = relay_private::nintendo::expand_dying_message(payload)?;
    expand_dying_message_from_envelope_items(data)
}

/// Merges envelope items with the ones contained in the DyingMessage
#[cfg(sentry)]
fn expand_dying_message_from_envelope_items(
    data: Bytes,
) -> Result<ExpandedDyingMessage, SwitchProcessingError> {
    use crate::Envelope;

    let mut items = Envelope::parse_items_bytes(data)
        .map_err(SwitchProcessingError::EnvelopeParsing)?
        .into_vec();

    let event = utils::take_item::<EventItem>(&mut items);
    let attachments = utils::take_items::<AttachmentItem>(&mut items).collect();
    let session_updates = utils::take_items::<SessionItem>(&mut items).collect();

    if !items.is_empty() {
        // Ignore unsupported items instead of failing to keep forward compatibility.
        relay_log::debug!(
            "Ignoring {} unsupported items in the dying message",
            items.len()
        );
    }

    Ok(ExpandedDyingMessage {
        event,
        attachments,
        session_updates,
    })
}

#[cfg(test)]
#[cfg(all(sentry, feature = "processing"))]
mod tests {
    use super::*;

    use relay_config::{Config, ConfigSnapshot, OverridableConfig};
    use relay_dynamic_config::Feature;
    use relay_event_schema::protocol::Level;
    use relay_protocol::assert_annotated_snapshot;
    use std::io::Write;
    use zstd::bulk::Compressor as ZstdCompressor;

    use crate::Envelope;
    use crate::constants::NNSWITCH_DYING_MESSAGE_FILENAME;
    use crate::envelope::{AttachmentType, ContentType, Item};
    use crate::processing;
    use crate::services::projects::project::ProjectInfo;

    fn ctx() -> Context<'static> {
        static CONFIG: std::sync::LazyLock<ConfigSnapshot> = std::sync::LazyLock::new(|| {
            let mut config = Config::default();
            config
                .apply_override(OverridableConfig {
                    processing: Some("true".to_owned()),
                    ..Default::default()
                })
                .unwrap();
            config.current()
        });

        static PROJECT_INFO: std::sync::LazyLock<ProjectInfo> = std::sync::LazyLock::new(|| {
            let mut project_info = ProjectInfo::default();
            project_info
                .config
                .features
                .0
                .insert(Feature::NintendoEventRewrite);
            project_info
        });

        Context {
            processing: processing::Context {
                config: &CONFIG,
                project_info: &PROJECT_INFO,
                ..processing::Context::for_test()
            },
            ..Context::for_test()
        }
    }

    fn create_envelope_items(dying_message: Bytes) -> Vec<Item> {
        // Note: the attachment length specified in the "outer" envelope attachment is very important.
        //       Otherwise parsing would fail because the inner one can contain line-breaks.
        let envelope =
        r#"{"event_id":"9ec79c33ec9942ab8353589fcb2e04dc","dsn":"https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42"}
{"type":"event"}
{"message":"hello world","level":"error","map":{"a":"val"}}
{"type":"attachment","filename":"dying_message.dat","length":<len>}
"#.replace("<len>", &dying_message.len().to_string());

        let mut envelope =
            Envelope::parse_bytes([Bytes::from(envelope), dying_message].concat().into()).unwrap();

        envelope.take_items_by(|_| true).into_vec()
    }

    #[test]
    fn test_expand_uncompressed_envelope_items() {
        // The attachment content is as follows:
        // - 4 bytes magic = sntr
        // - 1 byte version = 0
        // - 1 byte encoding = 0b0000_0000 - i.e. envelope items, uncompressed
        // - 2 bytes data length = 98 bytes - 0x0062 in big endian representation
        // - 98 bytes of content
        let mut items = create_envelope_items(Bytes::from(
            "sntr\0\0\0\x62\
            {\"type\":\"event\"}\n\
            {\"foo\":\"bar\",\"level\":\"info\",\"map\":{\"b\":\"c\"}}\n\
            {\"type\":\"attachment\",\"length\":2}\n\
            Hi\n",
        ));

        let Item::Attachment(attachment) = &items[1] else {
            panic!("expected an attachment");
        };
        assert_eq!(attachment.filename(), Some(NNSWITCH_DYING_MESSAGE_FILENAME));
        assert_eq!(items[1].data().payload().len(), 106);

        let parsed = Nswitch::try_expand(&mut items, ctx()).unwrap().unwrap();

        assert_annotated_snapshot!(parsed.event, @r#"
        {
          "level": "info",
          "logentry": {
            "formatted": "hello world"
          },
          "foo": "bar",
          "map": {
            "a": "val",
            "b": "c"
          }
        }
        "#);
        assert_eq!(parsed.attachments.len(), 1);
        assert_eq!(parsed.attachments[0].filename(), None);
        assert_eq!(parsed.attachments[0].data().payload(), "Hi".as_bytes());
    }

    #[test]
    fn test_expand_compressed_envelope_items() {
        // encoding 0x10 - i.e. envelope items, Zstandard compressed, no dictionary
        let dying_message = create_compressed_dying_message(0x10);
        let mut items = create_envelope_items(dying_message.into());

        let parsed = Nswitch::try_expand(&mut items, ctx()).unwrap().unwrap();

        assert_annotated_snapshot!(parsed.event, @r#"
        {
          "level": "info",
          "logentry": {
            "formatted": "hello world"
          },
          "foo": "bar",
          "map": {
            "a": "val",
            "b": "c"
          }
        }
        "#);
        assert_eq!(parsed.attachments.len(), 1);
        assert_eq!(parsed.attachments[0].filename(), None);
        assert_eq!(parsed.attachments[0].data().payload(), "Hi".as_bytes());
    }

    fn create_compressed_dying_message(encoding: u8) -> Vec<u8> {
        // The attachment content is as follows:
        // - 4 bytes magic = sntr
        // - 1 byte version = 0
        // - 1 byte encoding
        // - 2 bytes data length = N bytes - in big endian representation
        // - N bytes of compressed content (Zstandard)
        let mut compressor = ZstdCompressor::new(3).unwrap();
        let compressed_data = compressor
            .compress(
                b"\
                {\"type\":\"event\"}\n\
                {\"foo\":\"bar\",\"level\":\"info\",\"map\":{\"b\":\"c\"}}\n\
                {\"type\":\"attachment\",\"length\":2}\n\
                Hi\n\
                ",
            )
            .unwrap();
        let mut dying_message: Vec<u8> = Vec::new();
        dying_message.write_all(b"sntr\0").unwrap();
        dying_message.write_all(&[encoding]).unwrap();
        dying_message
            .write_all(&(compressed_data.len() as u16).to_be_bytes())
            .unwrap();
        dying_message.write_all(&compressed_data).unwrap();
        dying_message
    }

    #[test]
    fn test_expand_works_with_empty_data() {
        let mut items = create_envelope_items(Bytes::from("sntr\0\0\0\0"));

        let _ = Nswitch::try_expand(&mut items, ctx()).unwrap().unwrap();
    }

    #[test]
    fn test_switch_crash_is_reshaped() {
        let mut event = EventItem::new();
        event.data_mut().set_payload(
            ContentType::Json,
            include_bytes!(
                "../../../../../tests/integration/fixtures/native/nnswitch_crportal_event.json"
            )
            .as_slice(),
        );

        let mut dying_message = AttachmentItem::new();
        dying_message.set_filename(NNSWITCH_DYING_MESSAGE_FILENAME);
        dying_message.set_attachment_type(AttachmentType::NintendoSwitchDyingMessage);
        dying_message.data_mut().set_payload(
            ContentType::OctetStream,
            include_bytes!(
                "../../../../../tests/integration/fixtures/native/nnswitch_crportal_dying_message_raw.dat"
            )
            .as_slice(),
        );

        let mut items = vec![event.into(), dying_message.into()];

        let parsed = Nswitch::try_expand(&mut items, ctx()).unwrap().unwrap();

        let event = parsed.event.value().unwrap();

        // The DyingMessage scope patch is the merge base and contributes the fields the
        // forwarded event lacks.
        assert_eq!(
            event.release.value().map(|release| release.as_str()),
            Some("test-app@1.0.0")
        );
        assert_eq!(
            event.environment.value().map(String::as_str),
            Some("integration-test")
        );

        // The crash is rendered as fatal: both the forwarded event and the scope patch carry
        // `level: error`, and the reshape asserts the severity of a captured crash.
        assert_eq!(event.level.value(), Some(&Level::Fatal));

        let exception = event
            .exceptions
            .value()
            .unwrap()
            .values
            .value()
            .unwrap()
            .last()
            .unwrap()
            .value()
            .unwrap();

        // Marked synthetic, so Sentry drops the result-code `type` from the title and falls
        // back to the crashing function. Nintendo's own `handled: false` passes through.
        let mechanism = exception.mechanism.value().unwrap();
        assert_eq!(mechanism.synthetic.value(), Some(&true));
        assert_eq!(mechanism.handled.value(), Some(&false));

        // The result code and its description are preserved; only the `type`'s influence on the
        // title is removed. `value` remains as the issue subtitle.
        assert_eq!(
            exception.ty.value().map(String::as_str),
            Some("2168-0002 SyntheticTestResult")
        );
    }
}
