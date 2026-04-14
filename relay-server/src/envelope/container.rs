use std::collections::BTreeMap;

use bytes::BufMut;
use relay_protocol::{
    Annotated, DeserializableAnnotated, FromValue, IntoValue, SerializableAnnotated,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize, de, ser};

use crate::envelope::{ContentType, Item, ItemType};

/// Error emitted when failing to parse an [`ItemContainer`].
#[derive(thiserror::Error, Debug)]
pub enum ContainerParseError {
    /// The item container was expected to have a different content type.
    #[error("expected item with content type {expected} but got {actual:?}")]
    MismatchedContentType {
        expected: ContentType,
        actual: Option<ContentType>,
    },
    /// The item container was expected to have a different item type.
    #[error("expected item with item type {expected} but got {actual:?}")]
    MismatchedItemType {
        expected: ItemType,
        actual: ItemType,
    },
    /// The item container specified length does not match the amount of items contained in the
    /// container.
    #[error("container was specified with length {expected:?} but contained {actual} items")]
    MismatchedLength {
        expected: Option<u32>,
        actual: usize,
    },
    /// The container is malformed and cannot be deserialized.
    #[error("failed to deserialize item container: {0}")]
    Deserialize(#[from] serde_json::Error),
}

/// Error emitted when failing to write/serialize and [`ItemContainer`].
#[derive(thiserror::Error, Debug)]
pub enum ContainerWriteError {
    /// The item container is too large to serialize.
    #[error("failed to serialize item container, item count overflow")]
    Overflow,
    /// The contained items cannot be serialized.
    #[error("failed to serialize item container: {0}")]
    Serialize(#[from] serde_json::Error),
}

/// Any item contained in an [`ItemContainer`] needs to implement this trait.
pub trait ContainerItem: FromValue + IntoValue {
    /// The expected item type of the container for this type.
    const ITEM_TYPE: ItemType;
    /// The expected content type of the container for this type.
    const CONTENT_TYPE: ContentType;

    /// Header associated with the item.
    ///
    /// The header will be automatically serialized and de-serialized by the [`ItemContainer`].
    /// All headers must be de-serialized from and into an object keyed with a string.
    ///
    /// To ensure compatibility, Relay should almost always make sure each header is optional
    /// or has a default defined.
    ///
    /// Use [`NoHeader`] when there are no explicit headers defined.
    type Header: DeserializeOwned + Serialize + std::fmt::Debug;
}

/// A header implementation for [`container items`](ContainerItem) which currently do not have any
/// headers defined.
///
/// The implementation makes sure headers are forward compatible and passed a long.
#[derive(Debug, Deserialize, Serialize)]
#[serde(transparent)]
pub struct NoHeader(BTreeMap<String, relay_protocol::Value>);

#[derive(Debug)]
pub struct WithHeader<T: ContainerItem> {
    /// Optionally associated header with the item/value.
    pub header: Option<T::Header>,
    /// The value contained in a item container.
    pub value: Annotated<T>,
}

impl<T: ContainerItem> WithHeader<T> {
    /// Creates a [`Self`] from just a value with no associated header.
    ///
    /// This should only be used when creating new items, in most cases existing headers should be
    /// respected and explicitly handled and passed along.
    ///
    /// Prefer using [`WithHeader::new`] where possible.
    pub fn just(value: Annotated<T>) -> Self {
        Self {
            header: None,
            value,
        }
    }
}

impl<T: ContainerItem<Header = NoHeader>> WithHeader<T> {
    /// Creates a new [`Self`].
    ///
    /// Like [`Self::just`], but providing a type safe way to ensure `T::Header` is always explicitly
    /// set by only implementing [`Self::new`] for items which have a [`NoHeader`] as header.
    pub fn new(value: Annotated<T>) -> Self {
        Self::just(value)
    }
}

impl<T: ContainerItem> std::ops::Deref for WithHeader<T> {
    type Target = Annotated<T>;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<T: ContainerItem> std::ops::DerefMut for WithHeader<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}

impl<'de, T: ContainerItem> Deserialize<'de> for WithHeader<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(bound(deserialize = "T: ContainerItem"))]
        struct Inner<T: ContainerItem> {
            #[serde(rename = "__header")]
            header: Option<T::Header>,
            #[serde(flatten)]
            value: DeserializableAnnotated<T>,
        }

        let Inner { header, value } = Inner::<T>::deserialize(deserializer)?;
        Ok(Self {
            header,
            value: value.0,
        })
    }
}

impl<T: ContainerItem> Serialize for WithHeader<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        #[derive(Serialize)]
        #[serde(bound(serialize = "T: ContainerItem"))]
        struct Inner<'a, T: ContainerItem> {
            #[serde(rename = "__header", skip_serializing_if = "Option::is_none")]
            header: Option<&'a T::Header>,
            #[serde(flatten)]
            value: SerializableAnnotated<'a, T>,
        }

        let inner = Inner {
            header: self.header.as_ref(),
            value: SerializableAnnotated(&self.value),
        };

        inner.serialize(serializer)
    }
}

/// A list of items in an item container.
pub type ContainerItems<T> = Vec<WithHeader<T>>;

/// A container for multiple homogeneous envelope items.
///
/// Item containers are used to minimize the amount of single envelope items contained in an
/// envelope. They massively improve parsing speed of envelopes in Relay but are also used
/// to minimize metadata duplication on item headers.
///
/// Especially for small envelope items with high quantities (e.g. logs), this drastically
/// improves fast path parsing speeds and minimizes serialization overheads, by minimizing
/// the amount of items in an envelope.
///
/// An item container does not have a special [`super::ItemType`], but is identified by the
/// content type of the item.
#[derive(Debug)]
pub struct ItemContainer<T: ContainerItem> {
    items: ContainerItems<T>,
}

impl<T: ContainerItem> ItemContainer<T> {
    /// Returns all contained items.
    ///
    /// The container can be reconstructed using [`ItemContainer::from`].
    pub fn into_items(self) -> ContainerItems<T> {
        self.items
    }

    /// Parses an [`ItemContainer`] from an envelope [`Item`].
    ///
    /// This function also validates metadata of the container, specifically the content type
    /// and amount of contained items.
    pub fn parse(item: &Item) -> Result<Self, ContainerParseError> {
        if item.content_type() != Some(T::CONTENT_TYPE) {
            return Err(ContainerParseError::MismatchedContentType {
                expected: T::CONTENT_TYPE,
                actual: item.content_type(),
            });
        }

        if item.ty() != &T::ITEM_TYPE {
            return Err(ContainerParseError::MismatchedItemType {
                expected: T::ITEM_TYPE,
                actual: item.ty().clone(),
            });
        }

        let payload = item.payload();
        // Currently we assume every payload is JSON, but in the future we may allow other formats.
        let mut de = serde_json::Deserializer::from_slice(&payload);
        let container = Self::deserialize(&mut de)?;

        if Some(container.items.len()) != item.item_count().map(|u| u as usize) {
            return Err(ContainerParseError::MismatchedLength {
                expected: item.item_count(),
                actual: container.items.len(),
            });
        }

        Ok(container)
    }

    /// Serializes the [`ItemContainer`] into an envelope [`Item`].
    ///
    /// This will serialize the contained items into the [`Item::payload`] as well as
    /// update the [Item::content_type] and [`Item::item_count`].
    pub fn write_to(&self, item: &mut Item) -> Result<(), ContainerWriteError> {
        let mut payload = bytes::BytesMut::with_capacity(256).writer();
        let mut ser = serde_json::Serializer::new(&mut payload);

        self.serialize(&mut ser)?;

        item.set_payload_with_item_count(
            T::CONTENT_TYPE,
            payload.into_inner(),
            u32::try_from(self.items.len()).map_err(|_| ContainerWriteError::Overflow)?,
        );

        Ok(())
    }

    /// Checks whether a given item is a container of `T`s, according to its item and content types.
    pub fn is_container(item: &Item) -> bool {
        item.ty() == &T::ITEM_TYPE && item.content_type() == Some(T::CONTENT_TYPE)
    }

    fn deserialize<'de, D: de::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(bound(deserialize = "T: ContainerItem"))]
        struct Layout<T: ContainerItem> {
            items: ContainerItems<T>,
        }

        let Layout { items } = Layout::<T>::deserialize(deserializer)?;

        Ok(Self { items })
    }

    fn serialize<S: ser::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        #[serde(bound(serialize = "T: ContainerItem"))]
        struct Layout<'a, T: ContainerItem> {
            items: &'a ContainerItems<T>,
        }

        let layout = Layout { items: &self.items };
        Serialize::serialize(&layout, serializer)
    }
}

/// Sanitizes lone Unicode surrogates in JSON-escaped form within raw bytes.
///
/// JSON payloads may contain escaped lone surrogates (`\uD800`–`\uDFFF`) that are not part of
/// a valid surrogate pair. These are rejected by strict JSON parsers like `serde_json`.
///
/// This function scans for such sequences and replaces them with the Unicode replacement
/// character escape (`\uFFFD`), which is the same byte length (6 bytes), allowing in-place
/// replacement without shifting offsets.
///
/// Valid surrogate pairs (a high surrogate `\uD800`–`\uDBFF` immediately followed by a low
/// surrogate `\uDC00`–`\uDFFF`) are left intact.
///
/// Note: this function does not track whether a `\uDxxx` sequence appears inside a JSON string
/// value or a key — it replaces lone surrogates everywhere. This is safe because lone surrogates
/// are equally invalid in both contexts, and no real SDK emits surrogate-containing key names.
///
/// This function also does not handle escaped backslashes (`\\uD800` representing the literal
/// text `\uD800`). This is safe because such sequences are valid JSON and would not cause
/// `serde_json` to fail — meaning this function would never be called for such payloads.
///
/// Returns a `Cow::Borrowed` reference to the original slice if no replacements were needed,
/// avoiding allocation on the happy path.
pub(crate) fn sanitize_lone_surrogates(input: &[u8]) -> std::borrow::Cow<'_, [u8]> {
    use std::borrow::Cow;

    const REPLACEMENT: &[u8] = b"\\uFFFD";

    // Minimum length for a `\uXXXX` escape is 6 bytes.
    if input.len() < 6 {
        return Cow::Borrowed(input);
    }

    let mut result: Option<Vec<u8>> = None;
    let mut i = 0;

    while i + 5 < input.len() {
        if let Some(surrogate) = parse_unicode_escape(input, i) {
            if is_high_surrogate(surrogate) {
                // Check if followed by a low surrogate (valid pair).
                if i + 11 < input.len()
                    && let Some(next) = parse_unicode_escape(input, i + 6)
                    && is_low_surrogate(next)
                {
                    // Valid surrogate pair — keep both escapes as-is.
                    if let Some(ref mut buf) = result {
                        buf.extend_from_slice(&input[i..i + 12]);
                    }
                    i += 12;
                    continue;
                }
                // Lone high surrogate — replace.
                let buf = result.get_or_insert_with(|| input[..i].to_vec());
                buf.extend_from_slice(REPLACEMENT);
                i += 6;
                continue;
            } else if is_low_surrogate(surrogate) {
                // Lone low surrogate (not preceded by a high surrogate we would have consumed).
                let buf = result.get_or_insert_with(|| input[..i].to_vec());
                buf.extend_from_slice(REPLACEMENT);
                i += 6;
                continue;
            }
        }

        if let Some(ref mut buf) = result {
            buf.push(input[i]);
        }
        i += 1;
    }

    // Copy remaining bytes.
    match result {
        Some(mut buf) => {
            buf.extend_from_slice(&input[i..]);
            Cow::Owned(buf)
        }
        None => Cow::Borrowed(input),
    }
}

/// Attempts to parse a `\uXXXX` escape sequence starting at position `i`.
///
/// Returns the parsed 16-bit code point if the bytes at `input[i..i+6]` form a valid
/// JSON unicode escape (`\u` followed by exactly 4 hex digits), or `None` otherwise.
fn parse_unicode_escape(input: &[u8], i: usize) -> Option<u16> {
    if i + 5 >= input.len() {
        return None;
    }
    if input[i] != b'\\' || input[i + 1] != b'u' {
        return None;
    }
    let hex = std::str::from_utf8(&input[i + 2..i + 6]).ok()?;
    u16::from_str_radix(hex, 16).ok()
}

fn is_high_surrogate(code: u16) -> bool {
    (0xD800..=0xDBFF).contains(&code)
}

fn is_low_surrogate(code: u16) -> bool {
    (0xDC00..=0xDFFF).contains(&code)
}

impl<T: ContainerItem> From<ContainerItems<T>> for ItemContainer<T> {
    fn from(items: ContainerItems<T>) -> Self {
        Self { items }
    }
}

impl ContainerItem for relay_event_schema::protocol::OurLog {
    const ITEM_TYPE: ItemType = ItemType::Log;
    const CONTENT_TYPE: ContentType = ContentType::LogContainer;

    type Header = relay_event_schema::protocol::OurLogHeader;
}

impl ContainerItem for relay_event_schema::protocol::SpanV2 {
    const ITEM_TYPE: ItemType = ItemType::Span;
    const CONTENT_TYPE: ContentType = ContentType::SpanV2Container;

    type Header = NoHeader;
}

impl ContainerItem for relay_event_schema::protocol::TraceMetric {
    const ITEM_TYPE: ItemType = ItemType::TraceMetric;
    const CONTENT_TYPE: ContentType = ContentType::TraceMetricContainer;

    type Header = relay_event_schema::protocol::TraceMetricHeader;
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use insta::assert_debug_snapshot;
    use relay_protocol::{Empty, Object, Value};

    use crate::envelope::ItemType;

    use super::*;

    macro_rules! container {
        ($header:literal, $($item:literal),*) => {
            concat!($header, "\n", r#"{"items":["#, $($item),*, r#"]}"#).as_bytes()
        }
    }

    #[derive(Debug, Empty, IntoValue, FromValue)]
    struct TestLog {
        level: Annotated<String>,
        message: Annotated<String>,

        #[metastructure(additional_properties)]
        other: Object<Value>,
    }

    impl ContainerItem for TestLog {
        const ITEM_TYPE: ItemType = ItemType::Log;
        const CONTENT_TYPE: ContentType = ContentType::LogContainer;

        type Header = NoHeader;
    }

    fn logs<'a>(logs: impl IntoIterator<Item = (&'a str, &'a str)>) -> ItemContainer<TestLog> {
        let items = logs
            .into_iter()
            .map(|(level, message)| TestLog {
                level: Annotated::new(level.to_owned()),
                message: Annotated::new(message.to_owned()),
                other: Default::default(),
            })
            .map(Annotated::new)
            .map(WithHeader::just)
            .collect::<Vec<_>>();

        ItemContainer::from(items)
    }

    #[test]
    fn test_container_serialize() {
        let container = logs([("info", "foobar"), ("error", "ohno")]);

        let mut item = Item::new(ItemType::Log);
        container.write_to(&mut item).unwrap();

        assert_eq!(item.content_type(), Some(ContentType::LogContainer));
        assert_eq!(item.item_count(), Some(2));

        let payload = item.payload();
        let s = std::str::from_utf8(&payload).unwrap();
        insta::assert_snapshot!(s, @r###"{"items":[{"level":"info","message":"foobar"},{"level":"error","message":"ohno"}]}"###);
    }

    #[test]
    fn test_container_deserialize_invalid_item_count() {
        let (item, _) = Item::parse(Bytes::from_static(container!(
            r#"{"type":"log","content_type":"application/vnd.sentry.items.log+json","item_count":2}"#,
            r#"{"level":"info","message":"foobar"}"#
        )))
        .unwrap();

        assert_eq!(item.item_count(), Some(2));
        assert!(matches!(
            ItemContainer::<TestLog>::parse(&item),
            Err(ContainerParseError::MismatchedLength {
                expected: Some(2),
                actual: 1
            })
        ));
    }

    #[test]
    fn test_container_deserialize_invalid_content_type() {
        let (item, _) = Item::parse(Bytes::from_static(container!(
            r#"{"type":"log","content_type":"application/json","item_count":1}"#,
            r#"{"level":"info","message":"foobar"},"#
        )))
        .unwrap();

        assert_eq!(item.item_count(), Some(1));
        assert!(matches!(
            ItemContainer::<TestLog>::parse(&item),
            Err(ContainerParseError::MismatchedContentType {
                expected: ContentType::LogContainer,
                actual: Some(ContentType::Json),
            })
        ));
    }

    #[test]
    fn test_container_deserialize_invalid_item_type() {
        let (item, _) = Item::parse(Bytes::from_static(container!(
            r#"{"type":"span","content_type":"application/vnd.sentry.items.log+json","item_count":1}"#,
            r#"{"level":"info","message":"foobar"},"#
        )))
        .unwrap();

        assert_eq!(item.item_count(), Some(1));
        assert!(matches!(
            ItemContainer::<TestLog>::parse(&item),
            Err(ContainerParseError::MismatchedItemType {
                expected: ItemType::Log,
                actual: ItemType::Span,
            })
        ));
    }

    #[test]
    fn test_container_deserialize_missing_content_type() {
        let (item, _) = Item::parse(Bytes::from_static(container!(
            r#"{"type":"log","item_count":1}"#,
            r#"{"level":"info","message":"foobar"},"#
        )))
        .unwrap();

        assert_eq!(item.item_count(), Some(1));
        assert!(matches!(
            ItemContainer::<TestLog>::parse(&item),
            Err(ContainerParseError::MismatchedContentType {
                expected: ContentType::LogContainer,
                actual: None,
            })
        ));
    }

    #[test]
    fn test_container_deserialize_missing_items() {
        let (item, _) = Item::parse(Bytes::from_static(
            br#"{"type":"log","content_type":"application/vnd.sentry.items.log+json","item_count":1}
{"items2":[{"level":"info","message":"foobar"}]}
        "#,
        ))
        .unwrap();

        assert_eq!(item.item_count(), Some(1));
        assert!(matches!(
            ItemContainer::<TestLog>::parse(&item),
            Err(ContainerParseError::Deserialize(_))
        ));
    }

    #[test]
    fn test_container_deserialize_unexpected_type() {
        let (item, _) = Item::parse(Bytes::from_static(container!(
            r#"{"type":"log","content_type":"application/vnd.sentry.items.log+json","item_count":1}"#,
            r#"{"level":"info","message":"foobar"},"#
        )))
        .unwrap();

        assert_eq!(item.item_count(), Some(1));
        assert!(matches!(
            ItemContainer::<TestLog>::parse(&item),
            Err(ContainerParseError::Deserialize(_))
        ));
    }

    #[test]
    fn test_container_deserialize_successful() {
        let (item, _) = Item::parse(Bytes::from_static(container!(
            r#"{"type":"log","content_type":"application/vnd.sentry.items.log+json","item_count":2}"#,
            r#"{"level":"info","message":"foobar"},"#,
            r#"{"level":"error","message":"ohno"}"#
        )))
        .unwrap();

        assert_eq!(item.item_count(), Some(2));

        let container = ItemContainer::<TestLog>::parse(&item).unwrap();
        assert_debug_snapshot!(container, @r###"
        ItemContainer {
            items: [
                WithHeader {
                    header: None,
                    value: TestLog {
                        level: "info",
                        message: "foobar",
                        other: {},
                    },
                },
                WithHeader {
                    header: None,
                    value: TestLog {
                        level: "error",
                        message: "ohno",
                        other: {},
                    },
                },
            ],
        }
        "###);
    }

    #[test]
    fn test_container_roundtrip() {
        let (item, _) = Item::parse(Bytes::from_static(container!(
            r#"{"type":"log","content_type":"application/vnd.sentry.items.log+json","item_count":2}"#,
            r#"{"level":"info","message":"foobar"},"#,
            r#"{"level":"error","message":"ohno"}"#
        )))
        .unwrap();

        let container = ItemContainer::<TestLog>::parse(&item).unwrap();
        let mut new_item = Item::new(ItemType::Log);
        container.write_to(&mut new_item).unwrap();

        let container = ItemContainer::<TestLog>::parse(&new_item).unwrap();
        assert_debug_snapshot!(container, @r###"
        ItemContainer {
            items: [
                WithHeader {
                    header: None,
                    value: TestLog {
                        level: "info",
                        message: "foobar",
                        other: {},
                    },
                },
                WithHeader {
                    header: None,
                    value: TestLog {
                        level: "error",
                        message: "ohno",
                        other: {},
                    },
                },
            ],
        }
        "###);
    }

    #[test]
    fn test_container_with_headers() {
        let (item, _) = Item::parse(Bytes::from_static(container!(
            r#"{"type":"log","content_type":"application/vnd.sentry.items.log+json","item_count":2}"#,
            r#"{"__header":{},"level":"info","message":"foobar"},"#,
            r#"{"__header":{"foo":[1,"bar"]},"level":"error","message":"ohno"}"#
        )))
        .unwrap();

        let container = ItemContainer::<TestLog>::parse(&item).unwrap();
        let mut new_item = Item::new(ItemType::Log);
        container.write_to(&mut new_item).unwrap();

        let container = ItemContainer::<TestLog>::parse(&new_item).unwrap();
        assert_debug_snapshot!(container, @r###"
        ItemContainer {
            items: [
                WithHeader {
                    header: Some(
                        NoHeader(
                            {},
                        ),
                    ),
                    value: TestLog {
                        level: "info",
                        message: "foobar",
                        other: {},
                    },
                },
                WithHeader {
                    header: Some(
                        NoHeader(
                            {
                                "foo": Array(
                                    [
                                        I64(
                                            1,
                                        ),
                                        String(
                                            "bar",
                                        ),
                                    ],
                                ),
                            },
                        ),
                    ),
                    value: TestLog {
                        level: "error",
                        message: "ohno",
                        other: {},
                    },
                },
            ],
        }
        "###);

        let mut new_item = Item::new(ItemType::Log);
        container.write_to(&mut new_item).unwrap();

        // Make sure the headers serialize back in the original format.
        //
        // The test is engineered to have a matching serialization as the original test input,
        // e.g. correct order of fields.
        assert_eq!(new_item.payload(), item.payload());
    }

    #[test]
    fn test_sanitize_no_surrogates() {
        let input = br#"{"items":[{"level":"info","message":"hello world"}]}"#;
        let result = sanitize_lone_surrogates(input);
        assert!(matches!(result, std::borrow::Cow::Borrowed(_)));
        assert_eq!(result.as_ref(), input.as_slice());
    }

    #[test]
    fn test_sanitize_lone_high_surrogate() {
        let input = br#"{"items":[{"level":"info","message":"bad \uD800 char"}]}"#;
        let expected = br#"{"items":[{"level":"info","message":"bad \uFFFD char"}]}"#;
        let result = sanitize_lone_surrogates(input);
        assert!(matches!(result, std::borrow::Cow::Owned(_)));
        assert_eq!(result.as_ref(), expected.as_slice());
    }

    #[test]
    fn test_sanitize_lone_low_surrogate() {
        let input = br#"{"message":"\uDC00"}"#;
        let expected = br#"{"message":"\uFFFD"}"#;
        let result = sanitize_lone_surrogates(input);
        assert_eq!(result.as_ref(), expected.as_slice());
    }

    #[test]
    fn test_sanitize_preserves_valid_surrogate_pair() {
        // \uD83D\uDE00 is the surrogate pair for 😀
        let input = br#"{"message":"\uD83D\uDE00"}"#;
        let result = sanitize_lone_surrogates(input);
        assert!(matches!(result, std::borrow::Cow::Borrowed(_)));
        assert_eq!(result.as_ref(), input.as_slice());
    }

    #[test]
    fn test_sanitize_high_surrogate_followed_by_non_surrogate_escape() {
        // High surrogate followed by a non-surrogate \u escape — both should be handled.
        let input = br#"{"message":"\uD800\u0041"}"#;
        let expected = br#"{"message":"\uFFFD\u0041"}"#;
        let result = sanitize_lone_surrogates(input);
        assert_eq!(result.as_ref(), expected.as_slice());
    }

    #[test]
    fn test_sanitize_multiple_lone_surrogates() {
        let input = br#"{"a":"\uD800","b":"\uDBFF","c":"\uDC00"}"#;
        let expected = br#"{"a":"\uFFFD","b":"\uFFFD","c":"\uFFFD"}"#;
        let result = sanitize_lone_surrogates(input);
        assert_eq!(result.as_ref(), expected.as_slice());
    }

    #[test]
    fn test_sanitize_surrogate_at_end_of_input() {
        let input = br#"{"m":"\uD800"}"#;
        let expected = br#"{"m":"\uFFFD"}"#;
        let result = sanitize_lone_surrogates(input);
        assert_eq!(result.as_ref(), expected.as_slice());
    }

    #[test]
    fn test_sanitize_mixed_lone_and_valid_pair() {
        // Lone surrogate followed later by a valid pair — lone gets replaced, pair preserved.
        let input = br#"{"a":"\uD800","b":"\uD83D\uDE00"}"#;
        let expected = br#"{"a":"\uFFFD","b":"\uD83D\uDE00"}"#;
        let result = sanitize_lone_surrogates(input);
        assert_eq!(result.as_ref(), expected.as_slice());
    }

    #[test]
    fn test_sanitize_empty_and_short_inputs() {
        assert_eq!(sanitize_lone_surrogates(b"").as_ref(), b"");
        assert_eq!(sanitize_lone_surrogates(b"{}").as_ref(), b"{}");
        assert_eq!(sanitize_lone_surrogates(b"hello").as_ref(), b"hello");
    }
}
