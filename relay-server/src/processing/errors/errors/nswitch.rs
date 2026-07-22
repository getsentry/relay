//! Nintendo Switch crash reports processor related code.
//!
//! These functions are included only in the processing mode.

use bytes::{Buf, Bytes};
use relay_event_schema::protocol::Event;
use relay_protocol::Annotated;
use std::sync::OnceLock;
use zstd::bulk::Decompressor as ZstdDecompressor;

use relay_quotas::DataCategory;

use crate::Envelope;
use crate::constants::NNSWITCH_SENTRY_MAGIC;
use crate::envelope::{AttachmentType, EnvelopeError, Item, ItemType};
use crate::managed::{Counted, Quantities, RecordKeeper};
use crate::processing::ForwardContext;
use crate::processing::errors::Result;
use crate::processing::errors::errors::{Context, Expansion, SentryError, utils};
use crate::services::outcome::DiscardItemType;
use crate::services::processor::ProcessingError;

/// Limit the size of the decompressed data to prevent an invalid frame blowing up memory usage.
const MAX_DECOMPRESSED_SIZE: usize = 100 * 1024;

#[derive(Debug)]
pub enum Nswitch {
    Forward { dying_message: Item },
    Process,
}

impl SentryError for Nswitch {
    fn event_category(&self) -> DataCategory {
        DataCategory::Error
    }

    fn try_expand(items: &mut Vec<Item>, ctx: Context<'_>) -> Result<Option<Expansion<Self>>> {
        let Some(dying_message) = utils::take_item_by(items, |item| {
            item.attachment_type() == Some(AttachmentType::NintendoSwitchDyingMessage)
        }) else {
            return Ok(None);
        };

        let mut metrics = Default::default();

        let attachments: Vec<_> = utils::take_items_of_type(items, ItemType::Attachment);
        let user_reports = utils::take_items_of_type(items, ItemType::UserReport);

        if !ctx.processing.is_processing() {
            return Ok(Some(Expansion {
                event: Box::new(utils::take_parsed_event(items, &mut metrics, ctx)?),
                attachments,
                user_reports,
                error: Self::Forward { dying_message },
                metrics,
                fully_normalized: false,
            }));
        }

        let event = utils::take_item_of_type(items, ItemType::Event);

        let dying_message = expand_dying_message(dying_message.payload())
            .map_err(ProcessingError::InvalidNintendoDyingMessage)?;

        let mut attachments = attachments;
        attachments.extend(dying_message.attachments);

        #[cfg_attr(not(feature = "processing"), expect(unused_mut))]
        let mut event = match (event, dying_message.event) {
            (Some(event), Some(dying_message)) => {
                metrics.bytes_ingested_event =
                    Annotated::new((event.len() + dying_message.len()) as u64);
                merge_events(event, dying_message, ctx)?
            }
            (Some(event), None) => utils::event_from_json_payload(event, None, &mut metrics, ctx)?,
            (None, Some(event)) => utils::event_from_json_payload(event, None, &mut metrics, ctx)?,
            (None, None) => return Err(ProcessingError::NoEventPayload.into()),
        };

        // Nintendo forwards the crash with the abort result code as the exception `type`, which
        // Sentry would otherwise render as the issue title. Reshape it to look like a native
        // crash on other platforms: fall the title back to the crashing function and render the
        // event as a fatal, unhandled crash. Normalization runs after this and preserves it.
        utils::if_processing!(ctx, {
            if let Some(event) = event.value_mut() {
                crate::utils::reshape_switch_crash(event);
            }
        });

        Ok(Some(Expansion {
            event: Box::new(event),
            attachments,
            user_reports,
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

    fn serialize_into(self, items: &mut Vec<Item>, _ctx: ForwardContext<'_>) -> Result<()> {
        match self {
            Self::Forward { dying_message } => items.push(dying_message),
            Self::Process => {}
        }

        Ok(())
    }

    fn minidump_mut(&mut self) -> Option<&mut Item> {
        None
    }
}

impl Counted for Nswitch {
    fn quantities(&self) -> Quantities {
        // The dying message does not count as an attachment, because it will cease to exist and be
        // fully merged into the error after processing.
        Default::default()
    }
}

/// An error returned when parsing the dying message attachment.
#[derive(Debug, thiserror::Error)]
pub enum SwitchProcessingError {
    #[error("envelope parsing failed")]
    EnvelopeParsing(#[from] EnvelopeError),
    #[error("unexpected EOF, expected {expected:?}")]
    UnexpectedEof { expected: &'static str },
    #[error("invalid {0:?} ({1:?})")]
    InvalidValue(&'static str, usize),
    #[error("Zstandard error")]
    Zstandard(#[source] std::io::Error),
}

fn merge_events(
    from_envelope: Item,
    from_dying_messages: Item,
    ctx: Context<'_>,
) -> Result<Annotated<Event>> {
    if from_envelope.len().max(from_dying_messages.len()) > ctx.processing.config.max_event_size() {
        return Err(ProcessingError::PayloadTooLarge(DiscardItemType::Event).into());
    }

    merge_events_inner(from_envelope, from_dying_messages)
        .map_err(ProcessingError::InvalidJson)
        .map_err(Into::into)
}

fn merge_events_inner(
    from_envelope: Item,
    from_dying_messages: Item,
) -> Result<Annotated<Event>, serde_json::Error> {
    let from_envelope = serde_json::from_slice(&from_envelope.payload())?;
    let mut from_dying_message = serde_json::from_slice(&from_dying_messages.payload())?;

    // Uses the dying message as a base and fills it with values from the event.
    crate::utils::merge_values(&mut from_dying_message, from_envelope);

    Annotated::<Event>::deserialize_with_meta(from_dying_message)
}

#[derive(Debug, Default)]
struct ExpandedDyingMessage {
    event: Option<Item>,
    attachments: Vec<Item>,
}

/// Parses DyingMessage contents and updates the envelope.
/// See dying_message.md for the documentation.
fn expand_dying_message(mut payload: Bytes) -> Result<ExpandedDyingMessage, SwitchProcessingError> {
    payload.advance(NNSWITCH_SENTRY_MAGIC.len());
    let version = payload
        .try_get_u8()
        .map_err(|_| SwitchProcessingError::UnexpectedEof {
            expected: "version",
        })?;

    match version {
        0 => expand_dying_message_v0(payload),
        _ => Err(SwitchProcessingError::InvalidValue(
            "version",
            version as usize,
        )),
    }
}

/// DyingMessage protocol v0 parser.
fn expand_dying_message_v0(
    mut payload: Bytes,
) -> Result<ExpandedDyingMessage, SwitchProcessingError> {
    let encoding_byte = payload
        .try_get_u8()
        .map_err(|_| SwitchProcessingError::UnexpectedEof {
            expected: "encoding",
        })?;
    let format = (encoding_byte >> 6) & 0b0000_0011;
    let compression = (encoding_byte >> 4) & 0b0000_0011;
    let compression_arg = encoding_byte & 0b0000_1111;

    let compressed_length =
        payload
            .try_get_u16()
            .map_err(|_| SwitchProcessingError::UnexpectedEof {
                expected: "compressed data length",
            })?;
    let data = decompress_data(
        payload,
        compressed_length as usize,
        compression,
        compression_arg,
    )?;

    match format {
        0 => expand_dying_message_from_envelope_items(data),
        _ => Err(SwitchProcessingError::InvalidValue(
            "payload format",
            format as usize,
        )),
    }
}

/// Merges envelope items with the ones contained in the DyingMessage
fn expand_dying_message_from_envelope_items(
    data: Bytes,
) -> Result<ExpandedDyingMessage, SwitchProcessingError> {
    let mut items = Envelope::parse_items_bytes(data)
        .map_err(SwitchProcessingError::EnvelopeParsing)?
        .into_vec();

    let event = utils::take_item_of_type(&mut items, ItemType::Event);
    let attachments = utils::take_items_of_type(&mut items, ItemType::Attachment);

    if !items.is_empty() {
        // Ignore unsupported items instead of failing to keep forward compatibility.
        relay_log::debug!(
            "Ignoring {} unsupported items in the dying message",
            items.len()
        );
    }

    Ok(ExpandedDyingMessage { event, attachments })
}

fn decompress_data(
    payload: Bytes,
    compressed_length: usize,
    compression: u8,
    compression_arg: u8,
) -> Result<Bytes, SwitchProcessingError> {
    if payload.len() < compressed_length {
        return Err(SwitchProcessingError::InvalidValue(
            "compressed data length",
            compressed_length,
        ));
    }

    let data = payload.slice(0..compressed_length);
    match compression {
        // No compression
        0 => Ok(data),
        // Zstandard
        1 => decompress_data_zstd(data, compression_arg)
            .map(Bytes::from)
            .map_err(SwitchProcessingError::Zstandard),
        _ => Err(SwitchProcessingError::InvalidValue(
            "compression format",
            compression as usize,
        )),
    }
}

fn get_zstd_dictionary(id: usize) -> Option<&'static zstd::dict::DecoderDictionary<'static>> {
    // Inlined dictionary binary data.
    static ZSTD_DICTIONARIES: &[&[u8]] = &[
        // index 0 = empty dictionary (a.k.a "none")
        b"",
    ];

    // We initialize dictionaries (from their binary representation) only once and reuse them when decompressing.
    static ZSTD_DEC_DICTIONARIES: OnceLock<
        [zstd::dict::DecoderDictionary; ZSTD_DICTIONARIES.len()],
    > = OnceLock::new();
    let dictionaries = ZSTD_DEC_DICTIONARIES.get_or_init(|| {
        let mut dictionaries: [zstd::dict::DecoderDictionary; ZSTD_DICTIONARIES.len()] =
            [zstd::dict::DecoderDictionary::new(ZSTD_DICTIONARIES[0])];
        for i in 0..ZSTD_DICTIONARIES.len() {
            dictionaries[i] = zstd::dict::DecoderDictionary::new(ZSTD_DICTIONARIES[i]);
        }
        dictionaries
    });

    dictionaries.get(id)
}

fn decompress_data_zstd(data: Bytes, dictionary_id: u8) -> std::io::Result<Vec<u8>> {
    let dictionary = get_zstd_dictionary(dictionary_id as usize)
        .ok_or(std::io::Error::other("Unknown compression dictionary"))?;

    let mut decompressor = ZstdDecompressor::with_prepared_dictionary(dictionary)?;
    decompressor.decompress(data.as_ref(), MAX_DECOMPRESSED_SIZE)
}

#[cfg(test)]
#[cfg(feature = "processing")]
mod tests {
    use super::*;

    use relay_config::{Config, OverridableConfig};
    use relay_event_schema::protocol::Level;
    use relay_protocol::assert_annotated_snapshot;
    use std::io::Write;
    use zstd::bulk::Compressor as ZstdCompressor;

    use crate::constants::NNSWITCH_DYING_MESSAGE_FILENAME;
    use crate::envelope::Item;
    use crate::processing;

    fn ctx() -> Context<'static> {
        static CONFIG: std::sync::LazyLock<Config> = std::sync::LazyLock::new(|| {
            let mut config = Config::default();
            config
                .apply_override(OverridableConfig {
                    processing: Some("true".to_owned()),
                    ..Default::default()
                })
                .unwrap();
            config
        });

        Context {
            processing: processing::Context {
                config: &CONFIG,
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

        assert_eq!(items[1].ty(), &ItemType::Attachment);
        assert_eq!(items[1].filename(), Some(NNSWITCH_DYING_MESSAGE_FILENAME));
        assert_eq!(items[1].payload().len(), 106);

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
        assert_eq!(parsed.attachments[0].ty(), &ItemType::Attachment);
        assert_eq!(parsed.attachments[0].filename(), None);
        assert_eq!(parsed.attachments[0].payload(), "Hi".as_bytes());
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
        assert_eq!(parsed.attachments[0].ty(), &ItemType::Attachment);
        assert_eq!(parsed.attachments[0].filename(), None);
        assert_eq!(parsed.attachments[0].payload(), "Hi".as_bytes());
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
    fn test_expand_fails_with_unknown_dictioary() {
        // encoding 0x10 - i.e. envelope items, Zstandard compressed, dictionary ID 1
        let dying_message = create_compressed_dying_message(0b0001_0001);
        let mut items = create_envelope_items(dying_message.into());

        assert!(Nswitch::try_expand(&mut items, ctx()).is_err());
    }

    #[test]
    fn test_expand_fails_on_invalid_data() {
        let mut items = create_envelope_items(Bytes::from(
            "sntr\0\0\0\x62\
            {\"type\":\"event\"}\n\
            ",
        ));
        assert!(Nswitch::try_expand(&mut items, ctx()).is_err());
    }

    #[test]
    fn test_expand_works_with_empty_data() {
        let mut items = create_envelope_items(Bytes::from("sntr\0\0\0\0"));

        let _ = Nswitch::try_expand(&mut items, ctx()).unwrap().unwrap();
    }

    #[test]
    fn test_switch_crash_is_reshaped() {
        // Minimal, empty dying message (magic + version 0 + encoding 0 + length 0): no scope
        // patch, so we exercise only the reshaping of the event Nintendo forwards.
        let dying_message = Bytes::from("sntr\0\0\0\0");

        // The parent envelope event is what Nintendo forwards: the abort result code as the
        // exception `type`, a readable `value`, the crashing function, and level `error`.
        let envelope = r#"{"event_id":"9ec79c33ec9942ab8353589fcb2e04dc","dsn":"https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42"}
{"type":"event"}
{"level":"error","exception":{"values":[{"type":"2168-0002 ResultAccessViolationData","value":"Data access to an invalid memory region was performed. (2: Access Violation Data)","stacktrace":{"frames":[{"function":"ASentryTowerTurret::Shoot"}]}}]}}
{"type":"attachment","filename":"dying_message.dat","length":<len>}
"#
        .replace("<len>", &dying_message.len().to_string());

        let mut envelope =
            Envelope::parse_bytes([Bytes::from(envelope), dying_message].concat().into()).unwrap();
        let mut items = envelope.take_items_by(|_| true).into_vec();

        let parsed = Nswitch::try_expand(&mut items, ctx()).unwrap().unwrap();

        let event = parsed.event.value().unwrap();

        // The crash is rendered as fatal (Nintendo forwarded it as `error`).
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

        // Marked synthetic and unhandled, so Sentry drops the result-code `type` from the
        // title (falling back to the crashing function) and renders it as unhandled.
        let mechanism = exception.mechanism.value().unwrap();
        assert_eq!(mechanism.synthetic.value(), Some(&true));
        assert_eq!(mechanism.handled.value(), Some(&false));

        // The result code and its description are preserved; only the `type`'s influence on the
        // title is removed. `value` remains as the issue subtitle.
        assert_eq!(
            exception.ty.value().map(String::as_str),
            Some("2168-0002 ResultAccessViolationData")
        );
    }
}
