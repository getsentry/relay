//! Item bundle (`event.item_bundle` attachments) related code.
//!
//! See `item_bundle.md` for the format.
//! These functions are included only in the processing mode.

use super::ErrorPayload;

use bytes::{Buf, Bytes};
use relay_event_schema::protocol::Event;
use relay_protocol::Annotated;
use std::sync::OnceLock;
use zstd::bulk::Decompressor as ZstdDecompressor;

use relay_quotas::DataCategory;

use crate::Envelope;
use crate::constants::ITEM_BUNDLE_MAGIC;
use crate::envelope::{
    AttachmentItem, AttachmentType, EnvelopeError, EventItem, Item, SessionItem, UserReportItem,
};
use crate::managed::{Counted, Quantities, RecordKeeper};
use crate::processing::errors::errors::{Context, Expansion, SentryError};
use crate::processing::errors::{Result, utils};
use crate::services::outcome::DiscardItemType;
use crate::services::processor::ProcessingError;

/// Limit the size of the decompressed data to prevent an invalid frame blowing up memory usage.
const MAX_DECOMPRESSED_SIZE: usize = 100 * 1024;

#[derive(Debug)]
pub enum ItemBundle {
    Forward(AttachmentItem),
    Process,
}

impl SentryError for ItemBundle {
    fn event_category(&self) -> DataCategory {
        DataCategory::Error
    }

    fn try_expand(items: &mut Vec<Item>, ctx: Context<'_>) -> Result<Option<Expansion<Self>>> {
        let Some(item_bundle) = utils::take_item_if::<AttachmentItem>(items, |item| {
            item.attachment_type() == AttachmentType::ItemBundle
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
                error: Self::Forward(item_bundle),
                metrics,
                fully_normalized: false,
            }));
        }

        let event = utils::take_item::<EventItem>(items);

        let item_bundle = expand_item_bundle(item_bundle.data().payload())
            .map_err(ProcessingError::InvalidItemBundle)?;

        let mut attachments = attachments;
        attachments.extend(item_bundle.attachments);

        #[cfg_attr(not(feature = "processing"), expect(unused_mut))]
        let mut event = match (event, item_bundle.event) {
            (Some(event), Some(item_bundle)) => {
                metrics.bytes_ingested_event =
                    Annotated::new((event.data().len() + item_bundle.data().len()) as u64);
                merge_events(event, item_bundle, ctx)?
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
            intermediates: item_bundle
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
            Self::Forward(item_bundle) => smallvec::smallvec![item_bundle],
            Self::Process => Default::default(),
        })
    }

    fn minidump_mut(&mut self) -> Option<&mut AttachmentItem> {
        None
    }
}

impl Counted for ItemBundle {
    fn quantities(&self) -> Quantities {
        match self {
            // The item rules own the item bundle's zero attachment contribution.
            Self::Forward(item_bundle) => item_bundle.quantities(),
            // Processing has already consumed the message into the event.
            Self::Process => Quantities::new(),
        }
    }
}

/// An error returned when parsing the item bundle attachment.
#[derive(Debug, thiserror::Error)]
pub enum ItemBundleError {
    #[error("envelope parsing failed")]
    EnvelopeParsing(#[from] EnvelopeError),
    #[error("unexpected EOF, expected {expected:?}")]
    UnexpectedEof { expected: &'static str },
    #[error("invalid magic number")]
    InvalidMagic,
    #[error("invalid {0:?} ({1:?})")]
    InvalidValue(&'static str, usize),
    #[error("Zstandard error")]
    Zstandard(#[source] std::io::Error),
}

fn merge_events(
    from_envelope: EventItem,
    from_item_bundle: EventItem,
    ctx: Context<'_>,
) -> Result<Annotated<Event>> {
    if from_envelope
        .data()
        .len()
        .max(from_item_bundle.data().len())
        > ctx.processing.config.max_event_size()
    {
        return Err(ProcessingError::PayloadTooLarge(DiscardItemType::Event).into());
    }

    merge_events_inner(from_envelope, from_item_bundle)
        .map_err(ProcessingError::InvalidJson)
        .map_err(Into::into)
}

fn merge_events_inner(
    from_envelope: EventItem,
    from_item_bundle: EventItem,
) -> Result<Annotated<Event>, serde_json::Error> {
    let from_envelope = serde_json::from_slice(&from_envelope.data().payload())?;
    let mut base = serde_json::from_slice(&from_item_bundle.data().payload())?;

    // Uses the item bundle as a base and fills it with values from the event.
    crate::utils::merge_values(&mut base, from_envelope);

    Annotated::<Event>::deserialize_with_meta(base)
}

#[derive(Debug, Default)]
struct ExpandedItemBundle {
    event: Option<EventItem>,
    attachments: Vec<AttachmentItem>,
    session_updates: Vec<SessionItem>,
}

/// Parses ItemBundle contents and updates the envelope.
/// See `item_bundle.md` for the documentation.
fn expand_item_bundle(mut payload: Bytes) -> Result<ExpandedItemBundle, ItemBundleError> {
    // The attachment type usually comes from an explicitly set item header, in which case nothing
    // has inspected the payload yet. Validate the magic here so a crafted short payload can't
    // panic the `advance` below.
    if !payload.starts_with(ITEM_BUNDLE_MAGIC) {
        return Err(ItemBundleError::InvalidMagic);
    }
    payload.advance(ITEM_BUNDLE_MAGIC.len());
    let version = payload
        .try_get_u8()
        .map_err(|_| ItemBundleError::UnexpectedEof {
            expected: "version",
        })?;

    match version {
        0 => expand_item_bundle_v0(payload),
        _ => Err(ItemBundleError::InvalidValue("version", version as usize)),
    }
}

/// ItemBundle protocol v0 parser.
fn expand_item_bundle_v0(mut payload: Bytes) -> Result<ExpandedItemBundle, ItemBundleError> {
    let encoding_byte = payload
        .try_get_u8()
        .map_err(|_| ItemBundleError::UnexpectedEof {
            expected: "encoding",
        })?;
    let format = (encoding_byte >> 6) & 0b0000_0011;
    let compression = (encoding_byte >> 4) & 0b0000_0011;
    let compression_arg = encoding_byte & 0b0000_1111;

    let compressed_length = payload
        .try_get_u16()
        .map_err(|_| ItemBundleError::UnexpectedEof {
            expected: "compressed data length",
        })?;
    let data = decompress_data(
        payload,
        compressed_length as usize,
        compression,
        compression_arg,
    )?;

    match format {
        0 => expand_item_bundle_from_envelope_items(data),
        _ => Err(ItemBundleError::InvalidValue(
            "payload format",
            format as usize,
        )),
    }
}

/// Merges envelope items with the ones contained in the ItemBundle
fn expand_item_bundle_from_envelope_items(
    data: Bytes,
) -> Result<ExpandedItemBundle, ItemBundleError> {
    let mut items = Envelope::parse_items_bytes(data)
        .map_err(ItemBundleError::EnvelopeParsing)?
        .into_vec();

    let event = utils::take_item::<EventItem>(&mut items);
    let attachments = utils::take_items::<AttachmentItem>(&mut items).collect();
    let session_updates = utils::take_items::<SessionItem>(&mut items).collect();

    if !items.is_empty() {
        // Ignore unsupported items instead of failing to keep forward compatibility.
        relay_log::debug!(
            "Ignoring {} unsupported items in the item bundle",
            items.len()
        );
    }

    Ok(ExpandedItemBundle {
        event,
        attachments,
        session_updates,
    })
}

fn decompress_data(
    payload: Bytes,
    compressed_length: usize,
    compression: u8,
    compression_arg: u8,
) -> Result<Bytes, ItemBundleError> {
    if payload.len() < compressed_length {
        return Err(ItemBundleError::InvalidValue(
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
            .map_err(ItemBundleError::Zstandard),
        _ => Err(ItemBundleError::InvalidValue(
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

    use relay_config::{Config, ConfigSnapshot, OverridableConfig};
    use relay_dynamic_config::Feature;
    use relay_event_schema::protocol::Level;
    use relay_protocol::assert_annotated_snapshot;
    use std::io::Write;
    use zstd::bulk::Compressor as ZstdCompressor;

    use crate::envelope::{ContentType, Item};
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

        Context {
            processing: processing::Context {
                config: &CONFIG,
                ..processing::Context::for_test()
            },
            ..Context::for_test()
        }
    }

    fn create_envelope_items(item_bundle: Bytes) -> Vec<Item> {
        // Note: the attachment length specified in the "outer" envelope attachment is very important.
        //       Otherwise parsing would fail because the inner one can contain line-breaks.
        let envelope =
        r#"{"event_id":"9ec79c33ec9942ab8353589fcb2e04dc","dsn":"https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42"}
{"type":"event"}
{"message":"hello world","level":"error","map":{"a":"val"}}
{"type":"attachment","attachment_type":"event.item_bundle","filename":"item_bundle.dat","length":<len>}
"#.replace("<len>", &item_bundle.len().to_string());

        let mut envelope =
            Envelope::parse_bytes([Bytes::from(envelope), item_bundle].concat().into()).unwrap();

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
        assert_eq!(attachment.attachment_type(), AttachmentType::ItemBundle);
        assert_eq!(items[1].data().payload().len(), 106);

        let parsed = ItemBundle::try_expand(&mut items, ctx()).unwrap().unwrap();

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
        let item_bundle = create_compressed_item_bundle(0x10);
        let mut items = create_envelope_items(item_bundle.into());

        let parsed = ItemBundle::try_expand(&mut items, ctx()).unwrap().unwrap();

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

    fn create_compressed_item_bundle(encoding: u8) -> Vec<u8> {
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
        let mut item_bundle: Vec<u8> = Vec::new();
        item_bundle.write_all(b"sntr\0").unwrap();
        item_bundle.write_all(&[encoding]).unwrap();
        item_bundle
            .write_all(&(compressed_data.len() as u16).to_be_bytes())
            .unwrap();
        item_bundle.write_all(&compressed_data).unwrap();
        item_bundle
    }

    #[test]
    fn test_expand_fails_with_unknown_dictioary() {
        // encoding 0x10 - i.e. envelope items, Zstandard compressed, dictionary ID 1
        let item_bundle = create_compressed_item_bundle(0b0001_0001);
        let mut items = create_envelope_items(item_bundle.into());

        assert!(ItemBundle::try_expand(&mut items, ctx()).is_err());
    }

    #[test]
    fn test_expand_fails_on_invalid_data() {
        let mut items = create_envelope_items(Bytes::from(
            "sntr\0\0\0\x62\
            {\"type\":\"event\"}\n\
            ",
        ));
        assert!(ItemBundle::try_expand(&mut items, ctx()).is_err());
    }

    #[test]
    fn test_expand_works_with_empty_data() {
        let mut items = create_envelope_items(Bytes::from("sntr\0\0\0\0"));

        let _ = ItemBundle::try_expand(&mut items, ctx()).unwrap().unwrap();
    }

    #[test]
    fn test_expand_item_bundle_rejects_short_or_invalid_magic() {
        // A payload shorter than the 4-byte magic must return an error rather than panic in
        // `advance` (reachable when the attachment type is set explicitly in the item header).
        for payload in ["", "s", "sn", "snt"] {
            assert!(matches!(
                expand_item_bundle(Bytes::from(payload)),
                Err(ItemBundleError::InvalidMagic)
            ));
        }

        // A long-enough payload whose magic doesn't match is rejected too.
        assert!(matches!(
            expand_item_bundle(Bytes::from("xxxx")),
            Err(ItemBundleError::InvalidMagic)
        ));
    }

    #[test]
    fn test_switch_crash_is_reshaped() {
        let mut event = EventItem::new();
        event.data_mut().set_payload(
            ContentType::Json,
            include_bytes!(
                "../../../../../tests/integration/fixtures/native/item_bundle_scope_event.json"
            )
            .as_slice(),
        );

        let mut item_bundle = AttachmentItem::new();
        item_bundle.set_filename("item_bundle.dat");
        item_bundle.set_attachment_type(AttachmentType::ItemBundle);
        item_bundle.data_mut().set_payload(
            ContentType::OctetStream,
            include_bytes!(
                "../../../../../tests/integration/fixtures/native/item_bundle_scope.dat"
            )
            .as_slice(),
        );

        let mut items = vec![event.into(), item_bundle.into()];

        let parsed = ItemBundle::try_expand(&mut items, ctx()).unwrap().unwrap();

        let event = parsed.event.value().unwrap();

        // The ItemBundle scope patch is the merge base and contributes the fields the
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
