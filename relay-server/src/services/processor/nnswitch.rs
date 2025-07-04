//! Nintendo Switch crash reports processor related code.
//!
//! These functions are included only in the processing mode.

use crate::Envelope;
use crate::envelope::{ContentType, EnvelopeError, Item, ItemType};
use crate::managed::TypedEnvelope;
use crate::services::processor::{ErrorGroup, ProcessingError};
use crate::utils;
use bytes::{Buf, Bytes};
use std::sync::OnceLock;
use zstd::bulk::Decompressor as ZstdDecompressor;

/// Magic number indicating the dying message file is encoded by sentry-switch SDK.
const SENTRY_MAGIC: &[u8] = "sntr".as_bytes();

/// The file name that Nintendo uses to in the events they forward.
const DYING_MESSAGE_FILENAME: &str = "dying_message.dat";

/// Limit the size of the decompressed data to prevent an invalid frame blowing up memory usage.
const MAX_DECOMPRESSED_SIZE: usize = 100_1024;

type Result<T> = std::result::Result<T, SwitchProcessingError>;

/// An error returned when parsing the dying message attachment.
#[derive(Debug, thiserror::Error)]
pub enum SwitchProcessingError {
    #[error("invalid json")]
    InvalidJson(#[source] serde_json::Error),
    #[error("envelope parsing failed")]
    EnvelopeParsing(#[from] EnvelopeError),
    #[error("unexpected EOF, expected {expected:?}")]
    UnexpectedEof { expected: String },
    #[error("invalid {0:?} ({1:?})")]
    InvalidValue(String, usize),
    #[error("Zstandard error")]
    Zstandard(#[source] std::io::Error),
}

/// Expands Nintendo Switch crash-reports.
///
/// If the envelope does NOT contain a `dying_message.dat` attachment, it doesn't do anything.
/// If the attachment item is found and it matches the expected format, it parses the attachment
/// and updates the envelope with the content. The processed attachment is then removed from the
/// envelope.
///
/// The envelope may be dropped if it exceeds size limits after decompression. Particularly,
/// this includes cases where a single attachment file exceeds the maximum file size. This is in
/// line with the behavior of the envelope endpoint.
///
/// After this, [`crate::services::processor::EnvelopeProcessorService`] should be able to process
/// the envelope the same way it processes any other envelopes.
///
/// Note: in case of an error, we don't fail but leave the envelope as is.
pub fn expand(
    managed_envelope: &mut TypedEnvelope<ErrorGroup>,
) -> std::result::Result<(), ProcessingError> {
    let envelope = managed_envelope.envelope_mut();

    if let Some(item) = envelope.take_item_by(is_dying_message) {
        if let Err(e) = expand_dying_message(item.payload(), envelope) {
            // If we fail to process the dying message, we need to add back the original attachment.
            envelope.add_item(item);
            return Err(ProcessingError::InvalidNintendoDyingMessage(e));
        }
    }

    Ok(())
}

fn is_dying_message(item: &crate::envelope::Item) -> bool {
    item.ty() == &ItemType::Attachment
        && item.payload().starts_with(SENTRY_MAGIC)
        && item.filename() == Some(DYING_MESSAGE_FILENAME)
}

/// Parses DyingMessage contents and updates the envelope.
/// See dying_message.md for the documentation.
fn expand_dying_message(mut payload: Bytes, envelope: &mut Envelope) -> Result<()> {
    payload.advance(SENTRY_MAGIC.len());
    let version = payload
        .try_get_u8()
        .map_err(|_| SwitchProcessingError::UnexpectedEof {
            expected: "version".into(),
        })?;
    match version {
        0 => expand_dying_message_v0(payload, envelope),
        _ => Err(SwitchProcessingError::InvalidValue(
            "version".into(),
            version as usize,
        )),
    }
}

/// DyingMessage protocol v0 parser.
fn expand_dying_message_v0(mut payload: Bytes, envelope: &mut Envelope) -> Result<()> {
    let encoding_byte = payload
        .try_get_u8()
        .map_err(|_| SwitchProcessingError::UnexpectedEof {
            expected: "encoding".into(),
        })?;
    let format = (encoding_byte >> 6) & 0b0000_0011;
    let compression = (encoding_byte >> 4) & 0b0000_0011;
    let compression_arg = encoding_byte & 0b0000_1111;

    let compressed_length =
        payload
            .try_get_u16()
            .map_err(|_| SwitchProcessingError::UnexpectedEof {
                expected: "compressed data length".into(),
            })?;
    let data = decompress_data(
        payload,
        compressed_length as usize,
        compression,
        compression_arg,
    )?;

    match format {
        0 => expand_dying_message_from_envelope_items(data, envelope),
        _ => Err(SwitchProcessingError::InvalidValue(
            "payload format".into(),
            format as usize,
        )),
    }
}

/// Merges envelope items with the ones contained in the DyingMessage
fn expand_dying_message_from_envelope_items(data: Bytes, envelope: &mut Envelope) -> Result<()> {
    let items =
        Envelope::parse_items_bytes(data).map_err(SwitchProcessingError::EnvelopeParsing)?;
    for item in items {
        // If it's an event type, merge it with the main event one already in the envelope.
        if item.ty() == &ItemType::Event {
            if let Some(event) = envelope.get_item_by_mut(|it| it.ty() == &ItemType::Event) {
                update_event(item, event).map_err(SwitchProcessingError::InvalidJson)?;
                // Don't add this item as a new envelope item now that it's merged.
                continue;
            }
        }
        envelope.add_item(item);
    }
    Ok(())
}

fn update_event(item: Item, event: &mut Item) -> std::result::Result<(), serde_json::Error> {
    let original_json = serde_json::from_slice::<serde_json::Value>(&event.payload())?;
    let mut new_json = serde_json::from_slice(&item.payload())?;
    utils::merge_values(&mut new_json, original_json);
    let new_payload = serde_json::to_vec(&new_json)?;
    event.set_payload(ContentType::Json, new_payload);
    Ok(())
}

fn decompress_data(
    payload: Bytes,
    compressed_length: usize,
    compression: u8,
    compression_arg: u8,
) -> Result<Bytes> {
    if payload.len() < compressed_length {
        return Err(SwitchProcessingError::InvalidValue(
            "compressed data length".into(),
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
            "compression format".into(),
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
    use relay_system::Addr;
    use std::io::Write;

    use super::*;
    use crate::envelope::Item;
    use crate::managed::ManagedEnvelope;
    use crate::services::processor::ProcessingGroup;
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

    fn create_envelope(dying_message: Bytes) -> TypedEnvelope<ErrorGroup> {
        // Note: the attachment length specified in the "outer" envelope attachment is very important.
        //       Otherwise parsing would fail because the inner one can contain line-breaks.
        let envelope =
        r#"{"event_id":"9ec79c33ec9942ab8353589fcb2e04dc","dsn":"https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42"}
{"type":"event"}
{"message":"hello world","level":"error","map":{"a":"val"}}
{"type":"attachment","filename":"dying_message.dat","length":<len>}
"#.replace("<len>", &dying_message.len().to_string());

        let envelope = ManagedEnvelope::new(
            Envelope::parse_bytes([Bytes::from(envelope), dying_message].concat().into()).unwrap(),
            Addr::dummy(),
            Addr::dummy(),
        );
        (envelope, ProcessingGroup::Error).try_into().unwrap()
    }

    #[test]
    fn test_expand_uncompressed_envelope_items() {
        // The attachment content is as follows:
        // - 4 bytes magic = sntr
        // - 1 byte version = 0
        // - 1 byte encoding = 0b0000_0000 - i.e. envelope items, uncompressed
        // - 2 bytes data length = 98 bytes - 0x0062 in big endian representation
        // - 98 bytes of content
        let mut envelope = create_envelope(Bytes::from(
            "sntr\0\0\0\x62\
            {\"type\":\"event\"}\n\
            {\"foo\":\"bar\",\"level\":\"info\",\"map\":{\"b\":\"c\"}}\n\
            {\"type\":\"attachment\",\"length\":2}\n\
            Hi\n",
        ));

        let items: Vec<_> = envelope.envelope().items().collect();
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].ty(), &ItemType::Event);
        assert_eq!(
            items[0].payload(),
            "{\"message\":\"hello world\",\"level\":\"error\",\"map\":{\"a\":\"val\"}}"
        );
        assert_eq!(items[1].ty(), &ItemType::Attachment);
        assert_eq!(items[1].filename(), Some(DYING_MESSAGE_FILENAME));
        assert_eq!(items[1].payload().len(), 106);

        expand(&mut envelope).unwrap();

        let items: Vec<_> = envelope.envelope().items().collect();
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].ty(), &ItemType::Event);
        assert_eq!(
            items[0].payload(),
            "{\"foo\":\"bar\",\"level\":\"info\",\"map\":{\"a\":\"val\",\"b\":\"c\"},\"message\":\"hello world\"}"
        );
        assert_eq!(items[1].ty(), &ItemType::Attachment);
        assert_eq!(items[1].filename(), None);
        assert_eq!(items[1].payload(), "Hi".as_bytes());
    }

    #[test]
    fn test_expand_compressed_envelope_items() {
        // encoding 0x10 - i.e. envelope items, Zstandard compressed, no dictionary
        let dying_message = create_compressed_dying_message(0x10);
        let mut envelope = create_envelope(dying_message.into());

        let items: Vec<_> = envelope.envelope().items().collect();
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].ty(), &ItemType::Event);
        assert_eq!(
            items[0].payload(),
            "{\"message\":\"hello world\",\"level\":\"error\",\"map\":{\"a\":\"val\"}}"
        );
        assert_eq!(items[1].ty(), &ItemType::Attachment);
        assert_eq!(items[1].filename(), Some(DYING_MESSAGE_FILENAME));
        assert_eq!(items[1].payload().len(), 98);

        expand(&mut envelope).unwrap();

        let items: Vec<_> = envelope.envelope().items().collect();
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].ty(), &ItemType::Event);
        assert_eq!(
            items[0].payload(),
            "{\"foo\":\"bar\",\"level\":\"info\",\"map\":{\"a\":\"val\",\"b\":\"c\"},\"message\":\"hello world\"}"
        );
        assert_eq!(items[1].ty(), &ItemType::Attachment);
        assert_eq!(items[1].filename(), None);
        assert_eq!(items[1].payload(), "Hi".as_bytes());
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
        let mut envelope = create_envelope(dying_message.into());

        assert!(expand(&mut envelope).is_err());
    }

    #[test]
    fn test_expand_fails_on_invalid_data() {
        let mut envelope = create_envelope(Bytes::from(
            "sntr\0\0\0\x62\
            {\"type\":\"event\"}\n\
            ",
        ));
        assert!(expand(&mut envelope).is_err());
    }

    #[test]
    fn test_expand_works_with_empty_data() {
        let mut envelope = create_envelope(Bytes::from("sntr\0\0\0\0"));
        assert!(expand(&mut envelope).is_ok());
    }
}
