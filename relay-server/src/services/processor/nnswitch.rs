//! Nintendo Switch processor related code.
//!
//! These functions are included only in the processing mode.

use std::f32::consts::E;

use crate::envelope::ItemType;
use crate::services::processor::{ErrorGroup, EventFullyNormalized, ProcessingError};
use crate::utils::TypedEnvelope;
use crate::Envelope;
use bytes::Bytes;
use relay_config::Config;
use relay_event_schema::protocol::Event;
use relay_protocol::Annotated;

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
    config: &Config,
) -> Result<(), ProcessingError> {
    let envelope: &mut &mut crate::Envelope = &mut managed_envelope.envelope_mut();

    if let Some(item) = envelope.take_item_by(is_dying_message) {
        expand_dying_message(item.payload(), envelope, config);
    }

    Ok(())
}

/// Magic number indicating the dying message file is encoded by sentry-switch SDK.
const SENTRY_MAGIC: &[u8] = "sntr".as_bytes();

fn is_dying_message(item: &crate::envelope::Item) -> bool {
    if item.ty() != &ItemType::Attachment {
        return false;
    }
    if item.filename() != Some("dying_message.dat") {
        return false;
    }
    // Check if the attachment payload starts with magic number 0x736E7472 ('sntr')
    item.payload().starts_with(SENTRY_MAGIC)
}

// TODO
/// Extracts event information from an unreal context.
///
/// If the event does not contain an unreal context, this function does not perform any action.
/// If there was no event payload prior to this function, it is created.
pub fn process(
    managed_envelope: &mut TypedEnvelope<ErrorGroup>,
    event: &mut Annotated<Event>,
) -> Result<Option<EventFullyNormalized>, ProcessingError> {
    // if utils::process_unreal_envelope(event, managed_envelope.envelope_mut())
    //     .map_err(ProcessingError::InvalidUnrealReport)?
    // {
    //     return Ok(Some(EventFullyNormalized(false)));
    // }

    Ok(None)
}

/// Parses dying_message.dat contents and updates the envelope.
/// See https://github.com/getsentry/sentry-switch/blob/main/docs/protocol/README.md
fn expand_dying_message(
    payload: Bytes,
    envelope: &mut Envelope,
    config: &Config,
) -> Result<(), ProcessingError> {
    let mut offset = SENTRY_MAGIC.len();

    // lead byte consists of two uint4 values - header version (v0 - v15) & header data length (0-15 bytes)
    let lead_byte = payload
        .get(offset)
        .ok_or(ProcessingError::InvalidNintendoDyingMessage)?;
    let version = lead_byte >> 4;
    let _header_length = lead_byte & 0b0000_1111;
    offset += 1;

    match version {
        0 => expand_dying_message_v0(payload, offset, envelope, config),
        _ => Err(ProcessingError::InvalidNintendoDyingMessage),
    }
}

/// DyingMessage protocol v0 parser.
/// 1 byte - payload encoding
/// 2 bytes - payload size (compressed payload size if compression is used) (0-65535)
fn expand_dying_message_v0(
    payload: Bytes,
    mut offset: usize,
    envelope: &mut Envelope,
    _config: &Config,
) -> Result<(), ProcessingError> {
    // The payload encoding is stored as a single byte but stores multiple components by splitting bits to groups:
    // - 2 bits (uint2) - format (after decompression), possible values:
    //   - `0` = envelope items without envelope header
    // - 2 bits (uint2) - compression algorithm, possible values:
    //   - `0` = none
    //   - `1` = Zstandard
    // - remaining 4 bits are currently unused
    let encoding_byte = payload
        .get(offset)
        .ok_or(ProcessingError::InvalidNintendoDyingMessage)?;
    let format = (encoding_byte >> 6) & 0b0000_0011;
    let compression = (encoding_byte >> 4) & 0b0000_0011;
    offset += 1;

    if payload.len() < offset + 2 {
        return Err(ProcessingError::InvalidNintendoDyingMessage);
    }
    let data_length = u16::from_le_bytes([payload[offset], payload[offset + 1]]);
    offset += 2;
    let data = decompress_data(payload, offset, data_length as usize, compression)?;

    match format {
        0 => {
            // Merge envelope items with the ones contained in the DyingMessage
            if let Ok(items) = Envelope::parse_items_bytes(data) {
                for item in items {
                    // If it's an event type, merge it with the existing one already in the envelope.
                    if item.ty() == &ItemType::Event {
                        if let Some(event) = envelope.get_item_by(|it| it.ty() == &ItemType::Event)
                        {
                            // TODO implement merging

                            // Don't add this item as a new envelope item now that it's merged.
                            continue;
                        }
                    }
                    envelope.add_item(item);
                }
            }
            Ok(())
        }
        _ => Err(ProcessingError::InvalidNintendoDyingMessage),
    }
}

fn decompress_data(
    payload: Bytes,
    offset: usize,
    compressed_length: usize,
    compression: u8,
) -> Result<Bytes, ProcessingError> {
    if payload.len() >= offset + compressed_length {
        let data = payload.slice(offset..(offset + compressed_length));
        match compression {
            0 => return Ok(data),
            1 => {
                if let Ok(decompressed) = zstd::decode_all(data.as_ref()) {
                    return Ok(Bytes::from(decompressed));
                }
            }
            _ => {}
        };
    }
    return Err(ProcessingError::InvalidNintendoDyingMessage);
}
