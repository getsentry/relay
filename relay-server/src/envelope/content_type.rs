use std::fmt;

use serde::Serialize;

use crate::integrations::Integration;

pub const CONTENT_TYPE: &str = "application/x-sentry-envelope";

/// Payload content types.
///
/// This is an optimized enum intended to reduce allocations for common content types.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ContentType {
    /// `text/plain`
    Text,
    /// `application/json`
    Json,
    /// `application/x-msgpack`
    MsgPack,
    /// `application/octet-stream`
    OctetStream,
    /// `application/x-dmp`
    Minidump,
    /// `text/xml` and `application/xml`
    Xml,
    /// `application/x-sentry-envelope`
    Envelope,
    /// `application/x-protobuf`
    Protobuf,
    /// `application/vnd.sentry.items.log+json`
    LogContainer,
    /// `application/vnd.sentry.items.span.v2+json`
    SpanV2Container,
    /// `application/vnd.sentry.items.metric+json`
    TraceMetricContainer,
    /// Internal, not serialized.
    CompatSpan,
    /// All integration content types.
    Integration(Integration),
    /// Any arbitrary content type not listed explicitly.
    Other(String),
}

impl ContentType {
    #[inline]
    pub fn as_str(&self) -> &str {
        match self {
            Self::Text => "text/plain",
            Self::Json => "application/json",
            Self::MsgPack => "application/x-msgpack",
            Self::OctetStream => "application/octet-stream",
            Self::Minidump => "application/x-dmp",
            Self::Xml => "text/xml",
            Self::Envelope => CONTENT_TYPE,
            Self::Protobuf => "application/x-protobuf",
            Self::LogContainer => "application/vnd.sentry.items.log+json",
            Self::SpanV2Container => "application/vnd.sentry.items.span.v2+json",
            Self::TraceMetricContainer => "application/vnd.sentry.items.trace-metric+json",
            Self::CompatSpan => panic!("must not be serialized"),
            Self::Integration(integration) => integration.as_content_type(),
            Self::Other(other) => other,
        }
    }

    /// Returns `true` if this is the content type of an [`ItemContainer`](crate::envelope::ItemContainer).
    pub fn is_container(&self) -> bool {
        matches!(
            self,
            ContentType::LogContainer
                | ContentType::SpanV2Container
                | ContentType::TraceMetricContainer
        )
    }

    fn from_str(ct: &str) -> Option<Self> {
        if ct.eq_ignore_ascii_case(Self::Text.as_str()) {
            Some(Self::Text)
        } else if ct.eq_ignore_ascii_case(Self::Json.as_str()) {
            Some(Self::Json)
        } else if ct.eq_ignore_ascii_case(Self::MsgPack.as_str()) {
            Some(Self::MsgPack)
        } else if ct.eq_ignore_ascii_case(Self::OctetStream.as_str()) {
            Some(Self::OctetStream)
        } else if ct.eq_ignore_ascii_case(Self::Minidump.as_str()) {
            Some(Self::Minidump)
        } else if ct.eq_ignore_ascii_case(Self::Xml.as_str())
            || ct.eq_ignore_ascii_case("application/xml")
        {
            Some(Self::Xml)
        } else if ct.eq_ignore_ascii_case(Self::Envelope.as_str()) {
            Some(Self::Envelope)
        } else if ct.eq_ignore_ascii_case(Self::Protobuf.as_str()) {
            Some(Self::Protobuf)
        } else if ct.eq_ignore_ascii_case(Self::LogContainer.as_str()) {
            Some(Self::LogContainer)
        } else if ct.eq_ignore_ascii_case(Self::SpanV2Container.as_str()) {
            Some(Self::SpanV2Container)
        } else if ct.eq_ignore_ascii_case(Self::TraceMetricContainer.as_str()) {
            Some(Self::TraceMetricContainer)
        } else {
            Integration::from_content_type(ct).map(Self::Integration)
        }
    }
}

impl fmt::Display for ContentType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl From<String> for ContentType {
    fn from(mut content_type: String) -> Self {
        Self::from_str(&content_type).unwrap_or_else(|| {
            content_type.make_ascii_lowercase();
            ContentType::Other(content_type)
        })
    }
}

impl From<&'_ str> for ContentType {
    fn from(content_type: &str) -> Self {
        Self::from_str(content_type)
            .unwrap_or_else(|| ContentType::Other(content_type.to_ascii_lowercase()))
    }
}

impl From<Integration> for ContentType {
    fn from(value: Integration) -> Self {
        Self::Integration(value)
    }
}

impl std::str::FromStr for ContentType {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(s.into())
    }
}

impl PartialEq<str> for ContentType {
    fn eq(&self, other: &str) -> bool {
        // Take an indirection via ContentType::from_str to also check aliases. Do not allocate in
        // case there is no mapping.
        match ContentType::from_str(other) {
            Some(ct) => ct == *self,
            None => other.eq_ignore_ascii_case(self.as_str()),
        }
    }
}

impl PartialEq<&'_ str> for ContentType {
    fn eq(&self, other: &&'_ str) -> bool {
        *self == **other
    }
}

impl PartialEq<String> for ContentType {
    fn eq(&self, other: &String) -> bool {
        *self == other.as_str()
    }
}

impl PartialEq<ContentType> for &'_ str {
    fn eq(&self, other: &ContentType) -> bool {
        *other == *self
    }
}

impl PartialEq<ContentType> for str {
    fn eq(&self, other: &ContentType) -> bool {
        *other == *self
    }
}

impl PartialEq<ContentType> for String {
    fn eq(&self, other: &ContentType) -> bool {
        *other == *self
    }
}

impl Serialize for ContentType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

relay_common::impl_str_de!(ContentType, "a content type string");
