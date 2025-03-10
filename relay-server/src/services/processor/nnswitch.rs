//! Nintendo Switch processor related code.
//!
//! These functions are included only in the processing mode.

use crate::envelope::{ContentType, EnvelopeError, Item, ItemType};
use crate::services::processor::{ErrorGroup, ProcessingError};
use crate::utils::{self, TypedEnvelope};
use crate::Envelope;
use bytes::Bytes;

type Result<T> = std::result::Result<T, SwitchProcessingError>;

#[derive(Debug, thiserror::Error)]
pub(crate) enum SwitchProcessingError {
    #[error("invalid json")]
    InvalidJson(#[source] serde_json::Error),
    #[error("envelope parsing failed")]
    EnvelopeParsing(#[from] EnvelopeError),
    #[error("unexpected EOF at offset {offset:?}, expected {expected:?}")]
    UnexpectedEof { offset: usize, expected: String },
    #[error("invalid {0:?}")]
    InvalidValue(String),
    #[error("Zstandard error")]
    Zstandard(#[source] std::io::Error),
}

/// Expands Nintendo Switch DyingMessage attachment.
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
    let envelope: &mut &mut crate::Envelope = &mut managed_envelope.envelope_mut();

    if let Some(item) = envelope.take_item_by(is_dying_message) {
        if let Err(e) = expand_dying_message(item.payload(), envelope) {
            // If we fail to process the dying message, we need to add back the original attachment.
            envelope.add_item(item);
            return Err(ProcessingError::InvalidNintendoDyingMessage(e));
        }
    }

    Ok(())
}

/// Magic number indicating the dying message file is encoded by sentry-switch SDK.
const SENTRY_MAGIC: &[u8] = "sntr".as_bytes();

/// The file name that Nintendo uses to in the events they forward.
const DYING_MESSAGE_FILENAME: &str = "dying_message.dat";

fn is_dying_message(item: &crate::envelope::Item) -> bool {
    item.ty() == &ItemType::Attachment
        && item.payload().starts_with(SENTRY_MAGIC)
        && item.filename() == Some(DYING_MESSAGE_FILENAME)
}

/// Parses DyingMessage contents and updates the envelope.
/// See https://github.com/getsentry/sentry-switch/blob/main/docs/protocol/README.md
fn expand_dying_message(payload: Bytes, envelope: &mut Envelope) -> Result<()> {
    let mut offset = SENTRY_MAGIC.len();
    let version = payload
        .get(offset)
        .ok_or_else(|| SwitchProcessingError::UnexpectedEof {
            offset,
            expected: "version".into(),
        })?;
    offset += 1;
    match version {
        0 => expand_dying_message_v0(payload, offset, envelope),
        _ => Err(SwitchProcessingError::InvalidValue("version".into())),
    }
}

/// DyingMessage protocol v0 parser.
/// Header format:
/// 1 byte - payload encoding
/// 2 bytes - payload size (compressed payload size if compression is used) (0-65535)
fn expand_dying_message_v0(
    payload: Bytes,
    mut offset: usize,
    envelope: &mut Envelope,
) -> Result<()> {
    // The payload encoding is stored as a single byte but stores multiple components by splitting bits to groups:
    // - 2 bits (uint2) - format (after decompression), possible values:
    //   - `0` = envelope items without envelope header
    // - 2 bits (uint2) - compression algorithm, possible values:
    //   - `0` = none
    //   - `1` = Zstandard
    // - 4 bits (uint4) - compression algorithm specific argument, e.g. dictionary identifier
    let encoding_byte =
        payload
            .get(offset)
            .ok_or_else(|| SwitchProcessingError::UnexpectedEof {
                offset,
                expected: "encoding".into(),
            })?;
    let format = (encoding_byte >> 6) & 0b0000_0011;
    let compression = (encoding_byte >> 4) & 0b0000_0011;
    let compression_arg = encoding_byte & 0b0000_1111;
    offset += 1;

    if payload.len() < offset + 2 {
        return Err(SwitchProcessingError::UnexpectedEof {
            offset,
            expected: "compressed data length".into(),
        });
    }
    let compressed_length = u16::from_be_bytes([payload[offset], payload[offset + 1]]);
    offset += 2;
    let data = decompress_data(
        payload,
        offset,
        compressed_length,
        compression,
        compression_arg,
    )?;

    match format {
        0 => expand_dying_message_from_envelope_items(data, envelope),
        _ => Err(SwitchProcessingError::InvalidValue("payload format".into())),
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
    // TODO is it OK to merge events this way, without updating envelope item headers?
    let original_json = serde_json::from_slice::<serde_json::Value>(&event.payload())?;
    let mut new_json = serde_json::from_slice(&item.payload())?;
    utils::merge_values(&mut new_json, original_json);
    let new_payload = serde_json::to_vec(&new_json)?;
    event.set_payload(ContentType::Json, new_payload);
    Ok(())
}

fn decompress_data(
    payload: Bytes,
    offset: usize,
    compressed_length: u16,
    compression: u8,
    compression_arg: u8,
) -> Result<Bytes> {
    let data_end_offset = offset + compressed_length as usize;
    if payload.len() < data_end_offset {
        return Err(SwitchProcessingError::InvalidValue(
            "compressed data length".into(),
        ));
    }

    let data = payload.slice(offset..data_end_offset);
    match compression {
        // No compression
        0 => Ok(data),
        // Zstandard
        1 => {
            // TODO compression_arg is dictionary ID, currently not implemented so must be 0
            if compression_arg != 0 {
                return Err(SwitchProcessingError::InvalidValue(
                    "Zstandard dictionary ID".into(),
                ));
            }
            zstd::decode_all(data.as_ref())
                .map(Bytes::from)
                .map_err(SwitchProcessingError::Zstandard)
        }
        _ => Err(SwitchProcessingError::InvalidValue(
            "compression format".into(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use relay_system::Addr;
    use std::io::{Cursor, Write};

    use super::*;
    use crate::envelope::Item;
    use crate::services::processor::ProcessingGroup;
    use crate::utils::ManagedEnvelope;

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
        let mut envelope = String::from(
            "\
            {\"event_id\":\"9ec79c33ec9942ab8353589fcb2e04dc\",\"dsn\":\"https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42\"}\n\
            {\"type\":\"event\"}\n\
            {\"message\":\"hello world\",\"level\":\"error\",\"map\":{\"a\":\"val\"}}\n\
            {\"type\":\"attachment\",\"filename\":\"dying_message.dat\",\"length\":");
        envelope += dying_message.len().to_string().as_str();
        envelope += "}\n";
        ManagedEnvelope::new(
            Envelope::parse_bytes([Bytes::from(envelope), dying_message].concat().into()).unwrap(),
            Addr::dummy(),
            Addr::dummy(),
            ProcessingGroup::Error,
        )
        .try_into()
        .unwrap()
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
        // The attachment content is as follows:
        // - 4 bytes magic = sntr
        // - 1 byte version = 0
        // - 1 byte encoding = 0b0001_0000 - 0x10 - i.e. envelope items, Zstandard compressed
        // - 2 bytes data length = N bytes - in big endian representation
        // - N bytes of compressed content (Zstandard)
        let compressed_data = zstd::encode_all(
            Cursor::new(Bytes::from(
                "\
                {\"type\":\"event\"}\n\
                {\"foo\":\"bar\",\"level\":\"info\",\"map\":{\"b\":\"c\"}}\n\
                {\"type\":\"attachment\",\"length\":2}\n\
                Hi\n\
                ",
            )),
            3,
        )
        .unwrap();
        let mut dying_message: Vec<u8> = Vec::new();
        dying_message.write_all(b"sntr\0\x10").unwrap();
        dying_message
            .write_all(&(compressed_data.len() as u16).to_be_bytes())
            .unwrap();
        dying_message.write_all(&compressed_data).unwrap();

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
        assert_eq!(items[1].payload().len(), 97);

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
    fn test_expand_fails_on_invalid_data() {
        let mut envelope = create_envelope(Bytes::from(
            "sntr\0\0\0\x62\
            {\"type\":\"event\"}\n\
            ",
        ));
        assert!(expand(&mut envelope).is_err());
    }
}
