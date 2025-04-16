use std::marker::PhantomData;

use bytes::BufMut;
use relay_protocol::{
    Annotated, DeserializableAnnotated, FromValue, IntoValue, SerializableAnnotated,
};
use serde::ser::SerializeSeq;
use serde::{de, ser, Deserialize, Serialize};
use smallvec::SmallVec;

use crate::envelope::{ContentType, Item};

/// Error emitted when failing to parse an [`ItemContainer`].
#[derive(thiserror::Error, Debug)]
pub enum ContainerParseError {
    /// The item container was expected to have a different content type.
    #[error("expected item with content type {expected} but got {actual:?}")]
    MismatchedContentType {
        expected: ContentType,
        actual: Option<ContentType>,
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
    /// The expected content type of the container for this type.
    const CONTENT_TYPE: ContentType;
}

/// A list of items in an item container.
pub type ContainerItems<T> = SmallVec<[Annotated<T>; 3]>;

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
pub struct ItemContainer<T> {
    items: ContainerItems<T>,
}

impl<T> ItemContainer<T> {
    /// Returns all contained items.
    ///
    /// The container can be reconstructed using [`ItemContainer::from`].
    pub fn into_items(self) -> ContainerItems<T> {
        self.items
    }
}

impl<T: ContainerItem> ItemContainer<T> {
    /// Parses an [`ItemContainer`] from an envelope [`Item`].
    ///
    /// This function also validates metadata of the container, specifically the content type
    /// and amount of contained items.
    pub fn parse(item: &Item) -> Result<Self, ContainerParseError> {
        if item.content_type() != Some(&T::CONTENT_TYPE) {
            return Err(ContainerParseError::MismatchedContentType {
                expected: T::CONTENT_TYPE,
                actual: item.content_type().cloned(),
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

    fn deserialize<'de, D: de::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Layout<T> {
            #[serde(bound(deserialize = "T: FromValue"))]
            items: AnnotatedItems<T>,
        }

        let Layout {
            items: AnnotatedItems(items),
        } = Layout::<T>::deserialize(deserializer)?;

        Ok(Self { items })
    }

    fn serialize<S: ser::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        struct Layout<'a, T> {
            #[serde(bound(serialize = "T: IntoValue"))]
            items: AnnotatedItemsRef<'a, T>,
        }

        let layout = Layout {
            items: AnnotatedItemsRef(&self.items),
        };

        Serialize::serialize(&layout, serializer)
    }
}

impl<T> From<ContainerItems<T>> for ItemContainer<T> {
    fn from(items: ContainerItems<T>) -> Self {
        Self { items }
    }
}

impl ContainerItem for relay_event_schema::protocol::OurLog {
    const CONTENT_TYPE: ContentType = ContentType::LogContainer;
}

/// (De-)Serializes a list of Annotated items with metadata.
#[derive(Debug)]
struct AnnotatedItems<T>(ContainerItems<T>);

impl<'de, T> Deserialize<'de> for AnnotatedItems<T>
where
    T: FromValue,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct Visitor<T>(PhantomData<T>);

        impl<'de, T> de::Visitor<'de> for Visitor<T>
        where
            T: FromValue,
        {
            type Value = AnnotatedItems<T>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a list of envelope items")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: de::SeqAccess<'de>,
            {
                let mut items = ContainerItems::new();
                if let Some(size) = seq.size_hint() {
                    items.reserve_exact(size);
                }

                while let Some(DeserializableAnnotated(item)) = seq.next_element()? {
                    items.push(item);
                }

                Ok(AnnotatedItems(items))
            }
        }

        deserializer.deserialize_seq(Visitor(Default::default()))
    }
}

struct AnnotatedItemsRef<'a, T>(&'a ContainerItems<T>);

impl<T> Serialize for AnnotatedItemsRef<'_, T>
where
    T: IntoValue,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(self.0.len()))?;
        for item in self.0 {
            seq.serialize_element(&SerializableAnnotated(item))?;
        }
        seq.end()
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use insta::assert_debug_snapshot;
    use relay_protocol::Empty;

    use crate::envelope::ItemType;

    use super::*;

    #[derive(Debug, Empty, IntoValue, FromValue)]
    struct TestLog {
        level: Annotated<String>,
        message: Annotated<String>,
    }
    impl ContainerItem for TestLog {
        const CONTENT_TYPE: ContentType = ContentType::LogContainer;
    }

    fn logs<'a>(logs: impl IntoIterator<Item = (&'a str, &'a str)>) -> ItemContainer<TestLog> {
        let items: ContainerItems<_> = logs
            .into_iter()
            .map(|(level, message)| TestLog {
                level: Annotated::new(level.to_owned()),
                message: Annotated::new(message.to_owned()),
            })
            .map(Annotated::new)
            .collect();

        ItemContainer::from(items)
    }

    #[test]
    fn test_container_serialize() {
        let container = logs([("info", "foobar"), ("error", "ohno")]);

        let mut item = Item::new(ItemType::Log);
        container.write_to(&mut item).unwrap();

        assert_eq!(item.content_type(), Some(&ContentType::LogContainer));
        assert_eq!(item.item_count(), Some(2));

        let payload = item.payload();
        let s = std::str::from_utf8(&payload).unwrap();
        insta::assert_snapshot!(s, @r###"{"items":[{"level":"info","message":"foobar"},{"level":"error","message":"ohno"}]}"###);
    }

    #[test]
    fn test_container_deserialize_invalid_item_count() {
        let (item, _) = Item::parse(Bytes::from_static(
            br#"{"type":"log","content_type":"application/vnd.sentry.items.log+json","item_count":2}
{"items":[{"level":"info","message":"foobar"}]}
        "#,
        ))
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
        let (item, _) = Item::parse(Bytes::from_static(
            br#"{"type":"log","content_type":"application/json","item_count":1}
{"items":[{"level":"info","message":"foobar"}]}
        "#,
        ))
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
    fn test_container_deserialize_missing_content_type() {
        let (item, _) = Item::parse(Bytes::from_static(
            br#"{"type":"log","item_count":1}
{"items":[{"level":"info","message":"foobar"}]}
        "#,
        ))
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
        let (item, _) = Item::parse(Bytes::from_static(
            br#"{"type":"log","content_type":"application/vnd.sentry.items.log+json","item_count":1}
{"items":{"level":"info","message":"foobar"}}
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
    fn test_container_deserialize_successful() {
        let (item, _) = Item::parse(Bytes::from_static(
            br#"{"type":"log","content_type":"application/vnd.sentry.items.log+json","item_count":2}
{"items":[{"level":"info","message":"foobar"},{"level":"error","message":"ohno"}]}
        "#,
        ))
        .unwrap();

        assert_eq!(item.item_count(), Some(2));

        let container = ItemContainer::<TestLog>::parse(&item).unwrap();
        assert_debug_snapshot!(container, @r###"
        ItemContainer {
            items: [
                TestLog {
                    level: "info",
                    message: "foobar",
                },
                TestLog {
                    level: "error",
                    message: "ohno",
                },
            ],
        }
        "###);
    }

    #[test]
    fn test_container_roundtrip() {
        let (item, _) = Item::parse(Bytes::from_static(
            br#"{"type":"log","content_type":"application/vnd.sentry.items.log+json","item_count":2}
{"items":[{"level":"info","message":"foobar"},{"level":"error","message":"ohno"}]}
        "#,
        ))
        .unwrap();

        let container = ItemContainer::<TestLog>::parse(&item).unwrap();
        let mut new_item = Item::new(ItemType::Log);
        container.write_to(&mut new_item).unwrap();

        let container = ItemContainer::<TestLog>::parse(&new_item).unwrap();
        assert_debug_snapshot!(container, @r###"
        ItemContainer {
            items: [
                TestLog {
                    level: "info",
                    message: "foobar",
                },
                TestLog {
                    level: "error",
                    message: "ohno",
                },
            ],
        }
        "###);
    }
}
