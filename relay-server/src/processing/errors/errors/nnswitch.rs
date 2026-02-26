//! Nintendo Switch crash reports processor related code.
//!
//! These functions are included only in the processing mode.

use bytes::{Buf, Bytes};
use relay_event_schema::protocol::Event;
use relay_protocol::Annotated;
use relay_quotas::DataCategory;
use std::sync::OnceLock;
use zstd::bulk::Decompressor as ZstdDecompressor;

use crate::Envelope;
use crate::envelope::{EnvelopeError, Item, ItemType};
use crate::managed::{Counted, Quantities};
use crate::processing::ForwardContext;
use crate::processing::errors::Result;
use crate::processing::errors::errors::{Context, ParsedError, SentryError, utils};
use crate::services::outcome::DiscardItemType;
use crate::services::processor::ProcessingError;

/// Magic number indicating the dying message file is encoded by sentry-switch SDK.
const SENTRY_MAGIC: &[u8] = "sntr".as_bytes();

/// The file name that Nintendo uses to in the events they forward.
const DYING_MESSAGE_FILENAME: &str = "dying_message.dat";

/// Limit the size of the decompressed data to prevent an invalid frame blowing up memory usage.
const MAX_DECOMPRESSED_SIZE: usize = 100_1024;

#[derive(Debug)]
pub enum Nnswitch {
    Forward { dying_message: Box<Item> },
    Process,
}

impl SentryError for Nnswitch {
    fn try_expand(items: &mut Vec<Item>, ctx: Context<'_>) -> Result<Option<ParsedError<Self>>> {
        let Some(dying_message) = utils::take_item_by(items, is_dying_message) else {
            return Ok(None);
        };

        let mut metrics = Default::default();

        if !ctx.processing.is_processing() {
            return Ok(Some(ParsedError {
                event: utils::try_take_parsed_event(items, &mut metrics, ctx)?,
                attachments: utils::take_items_of_type(items, ItemType::Attachment),
                user_reports: utils::take_items_of_type(items, ItemType::UserReport),
                error: Self::Forward {
                    dying_message: Box::new(dying_message),
                },
                metrics,
                fully_normalized: false,
            }));
        }

        let event = utils::take_item_of_type(items, ItemType::Event);

        let mut attachments = items
            .extract_if(.., |item| *item.ty() == ItemType::Attachment)
            .collect::<Vec<_>>();

        let dying_message = expand_dying_message(dying_message.payload())
            .map_err(ProcessingError::InvalidNintendoDyingMessage)?;

        attachments.extend(dying_message.attachments);

        let event = match (event, dying_message.event) {
            (Some(event), Some(dying_message)) => {
                metrics.bytes_ingested_event =
                    Annotated::new((event.len() + dying_message.len()) as u64);
                merge_events(event, dying_message, ctx)?
            }
            (Some(event), None) => utils::event_from_json_payload(event, None, &mut metrics, ctx)?,
            (None, Some(event)) => utils::event_from_json_payload(event, None, &mut metrics, ctx)?,
            (None, None) => return Err(ProcessingError::NoEventPayload.into()),
        };

        Ok(Some(ParsedError {
            event,
            attachments,
            user_reports: utils::take_items_of_type(items, ItemType::UserReport),
            error: Self::Process {},
            metrics: Default::default(),
            fully_normalized: false,
        }))
    }

    fn serialize_into(self, items: &mut Vec<Item>, _ctx: ForwardContext<'_>) -> Result<()> {
        match self {
            Self::Forward { dying_message } => items.push(*dying_message),
            Self::Process => {}
        }

        Ok(())
    }
}

impl Counted for Nnswitch {
    fn quantities(&self) -> Quantities {
        match self {
            Self::Forward { dying_message } => smallvec::smallvec![
                (DataCategory::AttachmentItem, 1),
                (DataCategory::Attachment, dying_message.len()),
            ],
            Self::Process => Default::default(),
        }
    }
}

/// An error returned when parsing the dying message attachment.
#[derive(Debug, thiserror::Error)]
pub enum SwitchProcessingError {
    #[error("invalid json")]
    InvalidJson(#[source] serde_json::Error),
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

fn is_dying_message(item: &crate::envelope::Item) -> bool {
    item.ty() == &ItemType::Attachment
        && item.payload().starts_with(SENTRY_MAGIC)
        && item.filename() == Some(DYING_MESSAGE_FILENAME)
}

#[derive(Debug, Default)]
struct ExpandedDyingMessage {
    event: Option<Item>,
    attachments: Vec<Item>,
}

/// Parses DyingMessage contents and updates the envelope.
/// See dying_message.md for the documentation.
fn expand_dying_message(mut payload: Bytes) -> Result<ExpandedDyingMessage, SwitchProcessingError> {
    payload.advance(SENTRY_MAGIC.len());
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
    let attachments = items
        .extract_if(.., |item| *item.ty() == ItemType::Attachment)
        .collect();

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
mod tests {
    use relay_protocol::assert_annotated_snapshot;
    use std::io::Write;

    use super::*;
    use crate::envelope::{ContentType, Item};
    use zstd::bulk::Compressor as ZstdCompressor;

    #[test]
    fn test_is_dying_message() {
        let mut item = Item::new(ItemType::Attachment);
        item.set_filename("any");
        item.set_payload(ContentType::OctetStream, Bytes::from("sntrASDF"));
        assert!(!is_dying_message(&item));
        item.set_filename(DYING_MESSAGE_FILENAME);
        assert!(is_dying_message(&item));
        item.set_payload(ContentType::OctetStream, Bytes::from("FOO"));
        assert!(!is_dying_message(&item));
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
        assert_eq!(items[1].filename(), Some(DYING_MESSAGE_FILENAME));
        assert_eq!(items[1].payload().len(), 106);

        let parsed = Nnswitch::try_expand(&mut items, Context::for_test())
            .unwrap()
            .unwrap();

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

        let parsed = Nnswitch::try_expand(&mut items, Context::for_test())
            .unwrap()
            .unwrap();

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

        assert!(Nnswitch::try_expand(&mut items, Context::for_test()).is_err());
    }

    #[test]
    fn test_expand_fails_on_invalid_data() {
        let mut items = create_envelope_items(Bytes::from(
            "sntr\0\0\0\x62\
            {\"type\":\"event\"}\n\
            ",
        ));
        assert!(Nnswitch::try_expand(&mut items, Context::for_test()).is_err());
    }

    #[test]
    fn test_expand_works_with_empty_data() {
        let mut items = create_envelope_items(Bytes::from("sntr\0\0\0\0"));

        let _ = Nnswitch::try_expand(&mut items, Context::for_test())
            .unwrap()
            .unwrap();
    }
}
