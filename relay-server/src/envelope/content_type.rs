use std::fmt;

use crate::integrations::Integration;

pub const CONTENT_TYPE: &str = "application/x-sentry-envelope";

/// Payload content types.
///
/// This is an optimized enum intended to reduce allocations for common content types used by Relay.
/// When dealing with generic/user provided content types, use a different type instead, e.g. `String`.
#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ContentType {
    /// `text/plain`
    Text,
    /// `application/json`
    Json,
    /// `application/x-ndjson`
    NdJson,
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
    /// `application/vnd.sentry.items.trace-metric+json`
    TraceMetricContainer,
    /// `application/vnd.sentry.trace-attachment`
    TraceAttachment,
    /// `application/vnd.sentry.attachment-ref+json`
    AttachmentRef,
    /// `application/x-perfetto-trace`
    PerfettoTrace,
    /// All integration content types.
    Integration(Integration),
}

impl ContentType {
    #[inline]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Text => "text/plain",
            Self::Json => "application/json",
            Self::NdJson => "application/x-ndjson",
            Self::MsgPack => "application/x-msgpack",
            Self::OctetStream => "application/octet-stream",
            Self::Minidump => "application/x-dmp",
            Self::Xml => "text/xml",
            Self::Envelope => CONTENT_TYPE,
            Self::Protobuf => "application/x-protobuf",
            Self::LogContainer => "application/vnd.sentry.items.log+json",
            Self::SpanV2Container => "application/vnd.sentry.items.span.v2+json",
            Self::TraceMetricContainer => "application/vnd.sentry.items.trace-metric+json",
            Self::TraceAttachment => "application/vnd.sentry.trace-attachment",
            Self::AttachmentRef => "application/vnd.sentry.attachment-ref+json",
            Self::PerfettoTrace => "application/x-perfetto-trace",
            Self::Integration(integration) => integration.as_content_type(),
        }
    }

    /// Returns `true` if this is the content type of an [`ItemContainer`](crate::envelope::ItemContainer).
    pub fn is_container(self) -> bool {
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
        } else if match_content_type_and_charset(ct, Self::Json.as_str(), "utf-8") {
            Some(Self::Json)
        } else if match_content_type_and_charset(ct, Self::NdJson.as_str(), "utf-8") {
            Some(Self::NdJson)
        } else if ct.eq_ignore_ascii_case(Self::MsgPack.as_str()) {
            Some(Self::MsgPack)
        } else if ct.eq_ignore_ascii_case(Self::OctetStream.as_str()) {
            Some(Self::OctetStream)
        } else if ct.eq_ignore_ascii_case(Self::Minidump.as_str()) {
            Some(Self::Minidump)
        } else if match_content_type_and_charset(ct, Self::Xml.as_str(), "utf-8")
            || match_content_type_and_charset(ct, "application/xml", "utf-8")
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
        } else if ct.eq_ignore_ascii_case(Self::TraceAttachment.as_str())
            || ct.eq_ignore_ascii_case("application/vnd.sentry.attachment.v2")
        {
            Some(Self::TraceAttachment)
        } else if ct.eq_ignore_ascii_case(Self::AttachmentRef.as_str())
            || ct.eq_ignore_ascii_case("application/vnd.sentry.attachment-ref")
        {
            Some(Self::AttachmentRef)
        } else if ct.eq_ignore_ascii_case(Self::PerfettoTrace.as_str()) {
            Some(Self::PerfettoTrace)
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

/// Matches a `Content-Type` header value against an expected media type per RFC 9110.
///
/// Returns `true` if the media type equals `expected` and the only parameter is
/// the allowed `charset`. Any other parameter is not allowed.
fn match_content_type_and_charset(ct: &str, expected: &str, charset: &str) -> bool {
    // RFC whitespace characters.
    const OWS: [char; 2] = [' ', '\t'];

    let mut segments = ct.split(';');

    let media_type = segments.next().unwrap_or_default();
    if !media_type.trim_matches(OWS).eq_ignore_ascii_case(expected) {
        return false;
    }

    segments.all(|parameter| {
        let parameter = parameter.trim_matches(OWS);
        if parameter.is_empty() {
            return true;
        }

        let Some((name, value)) = parameter.split_once('=') else {
            return false;
        };
        let value = value
            .strip_prefix('"')
            .and_then(|v| v.strip_suffix('"'))
            .unwrap_or(value);

        name.eq_ignore_ascii_case("charset") && value.eq_ignore_ascii_case(charset)
    })
}

impl From<Integration> for ContentType {
    fn from(value: Integration) -> Self {
        Self::Integration(value)
    }
}

#[derive(Debug)]
pub struct UnknownContentType;

impl std::str::FromStr for ContentType {
    type Err = UnknownContentType;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_str(s).ok_or(UnknownContentType)
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

impl serde::Serialize for ContentType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

relay_common::impl_str_de!(ContentType, "a content type string");

#[cfg(test)]
mod tests {
    use similar_asserts::assert_eq;

    use super::*;

    #[test]
    fn test_attachment_ref_roundtrip() {
        let canonical_name = "application/vnd.sentry.attachment-ref+json";
        let ct = ContentType::from_str(canonical_name).unwrap();
        assert_eq!(ct, ContentType::AttachmentRef);
        assert_eq!(canonical_name, ct.as_str());

        let legacy_alias = "application/vnd.sentry.attachment-ref";
        let ct = ContentType::from_str(legacy_alias).unwrap();
        assert_eq!(ct, ContentType::AttachmentRef);
    }

    #[test]
    fn test_json_charset_parameter() {
        for accepted in [
            "application/json",
            "application/json; charset=utf-8",
            "application/json;charset=utf-8",
            "application/json ;  charset=UTF-8",
            "application/json;\tcharset=\"utf-8\"",
            "APPLICATION/JSON; CHARSET=utf-8;",
        ] {
            assert_eq!(ContentType::from_str(accepted), Some(ContentType::Json));
        }

        for rejected in [
            "application/json; charset=utf-16",
            "application/json; charset",
            "application/json; version=1",
            "application/json2; charset=utf-8",
            "application/ json; charset=utf-8",
        ] {
            assert_eq!(ContentType::from_str(rejected), None);
        }
    }

    #[test]
    fn test_xml_charset_parameter() {
        for accepted in [
            "text/xml",
            "application/xml",
            "text/xml; charset=utf-8",
            "application/xml; charset=utf-8",
        ] {
            assert_eq!(ContentType::from_str(accepted), Some(ContentType::Xml));
        }
    }
}
