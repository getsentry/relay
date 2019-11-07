//! Contains definitions for the security report interfaces.
//!
//! The security interfaces are CSP, HPKP, ExpectCT and ExpectStaple.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fmt::{self, Write};
use std::str::FromStr;

use chrono::{DateTime, Utc};
use serde::de::{Error, IgnoredAny};
use serde::{Deserialize, Deserializer, Serialize};
use url::Url;

use crate::protocol::{
    Event, HeaderName, HeaderValue, Headers, LogEntry, PairList, Request, TagEntry, Tags,
};
use crate::types::{Annotated, Array, Object, Value};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct InvalidSecurityError;

impl fmt::Display for InvalidSecurityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid security report")
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CspDirective {
    BaseUri,
    ChildSrc,
    ConnectSrc,
    DefaultSrc,
    FontSrc,
    FormAction,
    FrameAncestors,
    FrameSrc,
    ImgSrc,
    ManifestSrc,
    MediaSrc,
    ObjectSrc,
    PluginTypes,
    PrefetchSrc,
    Referrer,
    ScriptSrc,
    ScriptSrcAttr,
    ScriptSrcElem,
    StyleSrc,
    StyleSrcElem,
    StyleSrcAttr,
    UpgradeInsecureRequests,
    WorkerSrc,
    // Sandbox , // unsupported
}

impl fmt::Display for CspDirective {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Self::BaseUri => write!(f, "base-uri"),
            Self::ChildSrc => write!(f, "child-src"),
            Self::ConnectSrc => write!(f, "connect-src"),
            Self::DefaultSrc => write!(f, "default-src"),
            Self::FontSrc => write!(f, "font-src"),
            Self::FormAction => write!(f, "form-action"),
            Self::FrameAncestors => write!(f, "frame-ancestors"),
            Self::FrameSrc => write!(f, "frame-src"),
            Self::ImgSrc => write!(f, "img-src"),
            Self::ManifestSrc => write!(f, "manifest-src"),
            Self::MediaSrc => write!(f, "media-src"),
            Self::ObjectSrc => write!(f, "object-src"),
            Self::PluginTypes => write!(f, "plugin-types"),
            Self::PrefetchSrc => write!(f, "prefetch-src"),
            Self::Referrer => write!(f, "referrer"),
            Self::ScriptSrc => write!(f, "script-src"),
            Self::ScriptSrcAttr => write!(f, "script-src-attr"),
            Self::ScriptSrcElem => write!(f, "script-src-elem"),
            Self::StyleSrc => write!(f, "style-src"),
            Self::StyleSrcElem => write!(f, "style-src-elem"),
            Self::StyleSrcAttr => write!(f, "style-src-attr"),
            Self::UpgradeInsecureRequests => write!(f, "upgrade-insecure-requests"),
            Self::WorkerSrc => write!(f, "worker-src"),
        }
    }
}

impl FromStr for CspDirective {
    type Err = InvalidSecurityError;

    fn from_str(string: &str) -> Result<Self, Self::Err> {
        Ok(match string {
            "base-uri" => Self::BaseUri,
            "child-src" => Self::ChildSrc,
            "connect-src" => Self::ConnectSrc,
            "default-src" => Self::DefaultSrc,
            "font-src" => Self::FontSrc,
            "form-action" => Self::FormAction,
            "frame-ancestors" => Self::FrameAncestors,
            "frame-src" => Self::FrameSrc,
            "img-src" => Self::ImgSrc,
            "manifest-src" => Self::ManifestSrc,
            "media-src" => Self::MediaSrc,
            "object-src" => Self::ObjectSrc,
            "plugin-types" => Self::PluginTypes,
            "prefetch-src" => Self::PrefetchSrc,
            "referrer" => Self::Referrer,
            "script-src" => Self::ScriptSrc,
            "script-src-attr" => Self::ScriptSrcAttr,
            "script-src-elem" => Self::ScriptSrcElem,
            "style-src" => Self::StyleSrc,
            "style-src-elem" => Self::StyleSrcElem,
            "style-src-attr" => Self::StyleSrcAttr,
            "upgrade-insecure-requests" => Self::UpgradeInsecureRequests,
            "worker-src" => Self::WorkerSrc,
            _ => return Err(InvalidSecurityError),
        })
    }
}

impl_str_serde!(CspDirective);

fn is_local(uri: &str) -> bool {
    match uri {
        "" | "self" | "'self'" => true,
        _ => false,
    }
}

fn schema_uses_host(schema: &str) -> bool {
    // List of schemas with host (netloc) from Python's urlunsplit:
    // see https://github.com/python/cpython/blob/1eac437e8da106a626efffe9fce1cb47dbf5be35/Lib/urllib/parse.py#L51
    //
    // Only modification: "" is set to false, since there is a separate check in the urlunsplit
    // implementation that omits the leading "//" in that case.
    match schema {
        "ftp" | "http" | "gopher" | "nntp" | "telnet" | "imap" | "wais" | "file" | "mms"
        | "https" | "shttp" | "snews" | "prospero" | "rtsp" | "rtspu" | "rsync" | "svn"
        | "svn+ssh" | "sftp" | "nfs" | "git" | "git+ssh" | "ws" | "wss" => true,
        _ => false,
    }
}

/// Mimicks Python's urlunsplit with all its quirks.
fn unsplit_uri(schema: &str, host: &str) -> String {
    if !host.is_empty() || schema_uses_host(schema) {
        format!("{}://{}", schema, host)
    } else if !schema.is_empty() {
        format!("{}:{}", schema, host)
    } else {
        String::new()
    }
}

fn normalize_uri(value: &str) -> Cow<'_, str> {
    if is_local(value) {
        return Cow::Borrowed("'self'");
    }

    // A lot of these values get reported as literally just the scheme. So a value like 'data'
    // or 'blob', which are valid schemes, just not a uri. So we want to normalize it into a
    // URI.

    if !value.contains(':') {
        return Cow::Owned(unsplit_uri(value, ""));
    }

    let url = match Url::parse(value) {
        Ok(url) => url,
        Err(_) => return Cow::Borrowed(value),
    };

    let normalized = match url.scheme() {
        "http" | "https" => Cow::Borrowed(url.host_str().unwrap_or_default()),
        scheme => Cow::Owned(unsplit_uri(scheme, url.host_str().unwrap_or_default())),
    };

    Cow::Owned(match url.port() {
        Some(port) => format!("{}:{}", normalized, port),
        None => normalized.into_owned(),
    })
}

/// Inner (useful) part of a CSP report.
///
/// See `Csp` for meaning of fields.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
struct CspRaw {
    #[serde(skip_serializing_if = "Option::is_none")]
    effective_directive: Option<CspDirective>,
    #[serde(default = "CspRaw::default_blocked_uri")]
    blocked_uri: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    document_uri: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    original_policy: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    referrer: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    status_code: Option<u64>,
    #[serde(default = "String::new")]
    violated_directive: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    source_file: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    line_number: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    column_number: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    script_sample: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    disposition: Option<String>,

    #[serde(flatten)]
    other: BTreeMap<String, serde_json::Value>,
}

impl CspRaw {
    fn default_blocked_uri() -> String {
        "self".to_string()
    }

    fn effective_directive(&self) -> Result<CspDirective, InvalidSecurityError> {
        // Firefox doesn't send effective-directive, so parse it from
        // violated-directive but prefer effective-directive when present
        //
        // refs: https://bugzil.la/1192684#c8

        if let Some(directive) = self.effective_directive {
            return Ok(directive);
        }

        self.violated_directive
            .splitn(2, ' ')
            .next()
            .and_then(|v| v.parse().ok())
            .ok_or(InvalidSecurityError)
    }

    fn get_message(&self, effective_directive: CspDirective) -> String {
        if is_local(&self.blocked_uri) {
            match effective_directive {
                CspDirective::ChildSrc => "Blocked inline 'child'".to_string(),
                CspDirective::ConnectSrc => "Blocked inline 'connect'".to_string(),
                CspDirective::FontSrc => "Blocked inline 'font'".to_string(),
                CspDirective::ImgSrc => "Blocked inline 'image'".to_string(),
                CspDirective::ManifestSrc => "Blocked inline 'manifest'".to_string(),
                CspDirective::MediaSrc => "Blocked inline 'media'".to_string(),
                CspDirective::ObjectSrc => "Blocked inline 'object'".to_string(),
                CspDirective::ScriptSrcAttr => "Blocked unsafe 'script' element".to_string(),
                CspDirective::ScriptSrcElem => "Blocked inline script attribute".to_string(),
                CspDirective::StyleSrc => "Blocked inline 'style'".to_string(),
                CspDirective::StyleSrcElem => "Blocked 'style' or 'link' element".to_string(),
                CspDirective::StyleSrcAttr => "Blocked style attribute".to_string(),
                CspDirective::ScriptSrc => {
                    if self.violated_directive.contains("'unsafe-inline'") {
                        "Blocked unsafe inline 'script'".to_string()
                    } else if self.violated_directive.contains("'unsafe-eval'") {
                        "Blocked unsafe eval() 'script'".to_string()
                    } else {
                        "Blocked unsafe (eval() or inline) 'script'".to_string()
                    }
                }
                directive => format!("Blocked inline '{}'", directive),
            }
        } else {
            let uri = normalize_uri(&self.blocked_uri);

            match effective_directive {
                CspDirective::ChildSrc => format!("Blocked 'child' from '{}'", uri),
                CspDirective::ConnectSrc => format!("Blocked 'connect' from '{}'", uri),
                CspDirective::FontSrc => format!("Blocked 'font' from '{}'", uri),
                CspDirective::FormAction => format!("Blocked 'form' action to '{}'", uri),
                CspDirective::ImgSrc => format!("Blocked 'image' from '{}'", uri),
                CspDirective::ManifestSrc => format!("Blocked 'manifest' from '{}'", uri),
                CspDirective::MediaSrc => format!("Blocked 'media' from '{}'", uri),
                CspDirective::ObjectSrc => format!("Blocked 'object' from '{}'", uri),
                CspDirective::ScriptSrc => format!("Blocked 'script' from '{}'", uri),
                CspDirective::ScriptSrcAttr => {
                    format!("Blocked inline script attribute from '{}'", uri)
                }
                CspDirective::ScriptSrcElem => format!("Blocked 'script' from '{}'", uri),
                CspDirective::StyleSrc => format!("Blocked 'style' from '{}'", uri),
                CspDirective::StyleSrcElem => format!("Blocked 'style' from '{}'", uri),
                CspDirective::StyleSrcAttr => format!("Blocked style attribute from '{}'", uri),
                directive => format!("Blocked '{}' from '{}'", directive, uri),
            }
        }
    }

    fn into_protocol(self, effective_directive: CspDirective) -> Csp {
        Csp {
            effective_directive: Annotated::from(effective_directive.to_string()),
            blocked_uri: Annotated::from(self.blocked_uri),
            document_uri: Annotated::from(self.document_uri),
            original_policy: Annotated::from(self.original_policy),
            referrer: Annotated::from(self.referrer),
            status_code: Annotated::from(self.status_code),
            violated_directive: Annotated::from(self.violated_directive),
            source_file: Annotated::from(self.source_file),
            line_number: Annotated::from(self.line_number),
            column_number: Annotated::from(self.column_number),
            script_sample: Annotated::from(self.script_sample),
            disposition: Annotated::from(self.disposition),
            other: self
                .other
                .into_iter()
                .map(|(k, v)| (k, Annotated::from(v)))
                .collect(),
        }
    }

    fn sanitized_blocked_uri(&self) -> String {
        // HACK: This is 100% to work around Stripe urls that will casually put extremely sensitive
        // information in querystrings. The real solution is to apply data scrubbing to all tags
        // generically.
        //
        //    if netloc == 'api.stripe.com':
        //      query = '' fragment = ''

        let mut uri = self.blocked_uri.clone();

        if uri.starts_with("https://api.stripe.com/") {
            if let Some(index) = uri.find(&['#', '?'][..]) {
                uri.truncate(index);
            }
        }

        uri
    }

    fn normalize_value<'a>(&self, value: &'a str, document_uri: &str) -> Cow<'a, str> {
        // > If no scheme is specified, the same scheme as the one used to access the protected
        // > document is assumed.
        // Source: https://developer.mozilla.org/en-US/docs/Web/Security/CSP/CSP_policy_directives
        if let "'none'" | "'self'" | "'unsafe-inline'" | "'unsafe-eval'" = value {
            return Cow::Borrowed(value);
        }

        // Normalize a value down to 'self' if it matches the origin of document-uri FireFox
        // transforms a 'self' value into the spelled out origin, so we want to reverse this and
        // bring it back.
        if value.starts_with("data:")
            || value.starts_with("mediastream:")
            || value.starts_with("blob:")
            || value.starts_with("filesystem:")
            || value.starts_with("http:")
            || value.starts_with("https:")
            || value.starts_with("file:")
        {
            if document_uri == normalize_uri(value) {
                return Cow::Borrowed("'self'");
            }

            // Their rule had an explicit scheme, so let's respect that
            return Cow::Borrowed(value);
        }

        // Value doesn't have a scheme, but let's see if their hostnames match at least, if so,
        // they're the same.
        if value == document_uri {
            return Cow::Borrowed("'self'");
        }

        // Now we need to stitch on a scheme to the value, but let's not stitch on the boring
        // values.
        let original_uri = match self.document_uri {
            Some(ref u) => &u,
            None => "",
        };

        match original_uri.splitn(2, ':').next().unwrap_or_default() {
            "http" | "https" => Cow::Borrowed(value),
            scheme => Cow::Owned(unsplit_uri(scheme, value)),
        }
    }

    fn get_culprit(&self) -> String {
        if self.violated_directive.is_empty() {
            return String::new();
        }

        let mut bits = self.violated_directive.split_ascii_whitespace();
        let mut culprit = bits.next().unwrap_or_default().to_owned();

        let document_uri = self
            .document_uri
            .as_ref()
            .map(String::as_str)
            .unwrap_or_default();

        let normalized_uri = normalize_uri(document_uri);

        for bit in bits {
            write!(culprit, " {}", self.normalize_value(bit, &normalized_uri)).ok();
        }

        culprit
    }

    fn get_tags(&self, effective_directive: CspDirective) -> Tags {
        Tags(PairList::from(vec![
            Annotated::new(TagEntry(
                Annotated::new("effective-directive".to_string()),
                Annotated::new(effective_directive.to_string()),
            )),
            Annotated::new(TagEntry(
                Annotated::new("blocked-uri".to_string()),
                Annotated::new(self.sanitized_blocked_uri()),
            )),
        ]))
    }

    fn get_request(&self) -> Request {
        let headers = match self.referrer {
            Some(ref referrer) if !referrer.is_empty() => {
                Annotated::new(Headers(PairList(vec![Annotated::new((
                    Annotated::new(HeaderName::new("Referer")),
                    Annotated::new(HeaderValue::new(referrer.clone())),
                ))])))
            }
            Some(_) | None => Annotated::empty(),
        };

        Request {
            url: Annotated::from(self.document_uri.clone()),
            headers,
            ..Request::default()
        }
    }
}

/// Defines external, RFC-defined schema we accept, while `Csp` defines our own schema.
///
/// See `Csp` for meaning of fields.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
struct CspReportRaw {
    csp_report: CspRaw,
}

/// Models the content of a CSP report.
///
/// Note this models the older CSP reports (report-uri policy directive).
/// The new CSP reports (using report-to policy directive) are different.
///
/// NOTE: This is the structure used inside the Event (serialization is based on Annotated
/// infrastructure). We also use a version of this structure to deserialize from raw JSON
/// via serde.
///
///
/// See https://www.w3.org/TR/CSP3/
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, ToValue, ProcessValue)]
pub struct Csp {
    /// The directive whose enforcement caused the violation.
    #[metastructure(pii = "true")]
    pub effective_directive: Annotated<String>,
    /// The URI of the resource that was blocked from loading by the Content Security Policy.
    #[metastructure(pii = "true")]
    pub blocked_uri: Annotated<String>,
    /// The URI of the document in which the violation occurred.
    #[metastructure(pii = "true")]
    pub document_uri: Annotated<String>,
    /// The original policy as specified by the Content-Security-Policy HTTP header.
    pub original_policy: Annotated<String>,
    /// The referrer of the document in which the violation occurred.
    #[metastructure(pii = "true")]
    pub referrer: Annotated<String>,
    /// The HTTP status code of the resource on which the global object was instantiated.
    pub status_code: Annotated<u64>,
    /// The name of the policy section that was violated.
    pub violated_directive: Annotated<String>,
    /// The URL of the resource where the violation occurred.
    pub source_file: Annotated<String>,
    /// The line number in source-file on which the violation occurred.
    pub line_number: Annotated<u64>,
    /// The column number in source-file on which the violation occurred.
    pub column_number: Annotated<u64>,
    /// The first 40 characters of the inline script, event handler, or style that caused the
    /// violation.
    pub script_sample: Annotated<String>,
    /// Policy disposition (enforce or report).
    pub disposition: Annotated<String>,
    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(pii = "true", additional_properties)]
    pub other: Object<Value>,
}

impl Csp {
    pub fn parse_event(data: &[u8]) -> Result<Event, serde_json::Error> {
        let raw_report = serde_json::from_slice::<CspReportRaw>(data)?;
        let raw_csp = raw_report.csp_report;

        let effective_directive = raw_csp
            .effective_directive()
            .map_err(serde::de::Error::custom)?;

        Ok(Event {
            logentry: Annotated::new(LogEntry::from(raw_csp.get_message(effective_directive))),
            culprit: Annotated::new(raw_csp.get_culprit()),
            tags: Annotated::new(raw_csp.get_tags(effective_directive)),
            request: Annotated::new(raw_csp.get_request()),
            csp: Annotated::new(raw_csp.into_protocol(effective_directive)),
            ..Event::default()
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ExpectCtStatus {
    Unknown,
    Valid,
    Invalid,
}

impl fmt::Display for ExpectCtStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Self::Unknown => write!(f, "unknown"),
            Self::Valid => write!(f, "valid"),
            Self::Invalid => write!(f, "invalid"),
        }
    }
}

impl FromStr for ExpectCtStatus {
    type Err = InvalidSecurityError;

    fn from_str(string: &str) -> Result<Self, Self::Err> {
        Ok(match string {
            "unknown" => Self::Unknown,
            "valid" => Self::Valid,
            "invalid" => Self::Invalid,
            _ => return Err(InvalidSecurityError),
        })
    }
}

impl_str_serde!(ExpectCtStatus);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ExpectCtSource {
    TlsExtension,
    Ocsp,
    Embedded,
}

impl fmt::Display for ExpectCtSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Self::TlsExtension => write!(f, "tls-extension"),
            Self::Ocsp => write!(f, "ocsp"),
            Self::Embedded => write!(f, "embedded"),
        }
    }
}

impl FromStr for ExpectCtSource {
    type Err = InvalidSecurityError;

    fn from_str(string: &str) -> Result<Self, Self::Err> {
        Ok(match string {
            "tls-extension" => Self::TlsExtension,
            "ocsp" => Self::Ocsp,
            "embedded" => Self::Embedded,
            _ => return Err(InvalidSecurityError),
        })
    }
}

impl_str_serde!(ExpectCtSource);

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
struct SingleCertificateTimestampRaw {
    version: Option<i64>,
    status: Option<ExpectCtStatus>,
    source: Option<ExpectCtSource>,
    serialized_sct: Option<String>, // NOT kebab-case!
}

impl SingleCertificateTimestampRaw {
    fn into_protocol(self) -> SingleCertificateTimestamp {
        SingleCertificateTimestamp {
            version: Annotated::from(self.version),
            status: Annotated::from(self.status.map(|s| s.to_string())),
            source: Annotated::from(self.source.map(|s| s.to_string())),
            serialized_sct: Annotated::from(self.serialized_sct),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
struct ExpectCtRaw {
    #[serde(with = "serde_date_time_3339")]
    date_time: Option<DateTime<Utc>>,
    hostname: String,
    port: Option<i64>,
    effective_expiration_date: Option<DateTime<Utc>>,
    served_certificate_chain: Option<Vec<String>>,
    validated_certificate_chain: Option<Vec<String>>,
    scts: Option<Vec<SingleCertificateTimestampRaw>>,
}

mod serde_date_time_3339 {
    use super::*;
    use serde::de::Visitor;

    pub fn serialize<S>(date_time: &Option<DateTime<Utc>>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match date_time {
            None => serializer.serialize_none(),
            Some(d) => serializer.serialize_str(&d.to_rfc3339()),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<DateTime<Utc>>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct DateTimeVisitor;

        impl<'de> Visitor<'de> for DateTimeVisitor {
            type Value = Option<DateTime<Utc>>;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("expected a date-time in RFC 3339 format")
            }

            fn visit_str<E>(self, s: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                DateTime::parse_from_rfc3339(s)
                    .map(|d| Some(d.with_timezone(&Utc)))
                    .map_err(serde::de::Error::custom)
            }

            fn visit_none<E>(self) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(None)
            }
        }

        deserializer.deserialize_any(DateTimeVisitor)
    }
}

impl ExpectCtRaw {
    fn get_message(&self) -> String {
        format!("Expect-CT failed for '{}'", self.hostname)
    }

    fn into_protocol(self) -> ExpectCt {
        ExpectCt {
            date_time: Annotated::from(self.date_time.map(|d| d.to_rfc3339())),
            hostname: Annotated::from(self.hostname),
            port: Annotated::from(self.port),
            effective_expiration_date: Annotated::from(
                self.effective_expiration_date.map(|d| d.to_rfc3339()),
            ),
            served_certificate_chain: Annotated::new(
                self.served_certificate_chain
                    .map(|s| s.into_iter().map(Annotated::from).collect())
                    .unwrap_or_default(),
            ),

            validated_certificate_chain: Annotated::new(
                self.validated_certificate_chain
                    .map(|v| v.into_iter().map(Annotated::from).collect())
                    .unwrap_or_default(),
            ),
            scts: Annotated::from(self.scts.map(|scts| {
                scts.into_iter()
                    .map(|elm| Annotated::from(elm.into_protocol()))
                    .collect()
            })),
        }
    }

    fn get_culprit(&self) -> String {
        self.hostname.clone()
    }

    fn get_tags(&self) -> Tags {
        let mut tags = vec![Annotated::new(TagEntry(
            Annotated::new("hostname".to_string()),
            Annotated::new(self.hostname.clone()),
        ))];

        if let Some(port) = self.port {
            tags.push(Annotated::new(TagEntry(
                Annotated::new("port".to_string()),
                Annotated::new(port.to_string()),
            )));
        }

        Tags(PairList::from(tags))
    }

    fn get_request(&self) -> Request {
        Request {
            url: Annotated::from(self.hostname.clone()),
            ..Request::default()
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
struct ExpectCtReportRaw {
    expect_ct_report: ExpectCtRaw,
}

/// Object used in ExpectCt reports
///
/// See https://tools.ietf.org/html/draft-ietf-httpbis-expect-ct-07#section-3.1
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, ToValue, ProcessValue)]
pub struct SingleCertificateTimestamp {
    pub version: Annotated<i64>,
    pub status: Annotated<String>,
    pub source: Annotated<String>,
    pub serialized_sct: Annotated<String>,
}

/// Expect CT security report sent by user agent (browser).
///
/// See https://tools.ietf.org/html/draft-ietf-httpbis-expect-ct-07#section-3.1
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, ToValue, ProcessValue)]
pub struct ExpectCt {
    /// Date time in rfc3339 format YYYY-MM-DDTHH:MM:DD{.FFFFFF}(Z|+/-HH:MM)
    /// UTC time that the UA observed the CT compliance failure
    pub date_time: Annotated<String>,
    /// The hostname to which the UA made the original request that failed the CT compliance check.
    pub hostname: Annotated<String>,
    pub port: Annotated<i64>,
    /// Date time in rfc3339 format
    pub effective_expiration_date: Annotated<String>,
    pub served_certificate_chain: Annotated<Array<String>>,
    pub validated_certificate_chain: Annotated<Array<String>>,
    pub scts: Annotated<Array<SingleCertificateTimestamp>>,
}

impl ExpectCt {
    pub fn parse_event(data: &[u8]) -> Result<Event, serde_json::Error> {
        let raw_report = serde_json::from_slice::<ExpectCtReportRaw>(data)?;
        let raw_expect_ct = raw_report.expect_ct_report;

        Ok(Event {
            logentry: Annotated::new(LogEntry::from(raw_expect_ct.get_message())),
            culprit: Annotated::new(raw_expect_ct.get_culprit()),
            tags: Annotated::new(raw_expect_ct.get_tags()),
            request: Annotated::new(raw_expect_ct.get_request()),
            expectct: Annotated::new(raw_expect_ct.into_protocol()),
            ..Event::default()
        })
    }
}

/// Defines external, RFC-defined schema we accept, while `Hpkp` defines our own schema.
///
/// See `Hpkp` for meaning of fields.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
struct HpkpRaw {
    #[serde(skip_serializing_if = "Option::is_none")]
    date_time: Option<DateTime<Utc>>,
    hostname: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    port: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    effective_expiration_date: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    include_subdomains: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    noted_hostname: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    served_certificate_chain: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    validated_certificate_chain: Option<Vec<String>>,
    known_pins: Vec<String>,
    #[serde(flatten)]
    other: BTreeMap<String, serde_json::Value>,
}

impl HpkpRaw {
    fn get_message(&self) -> String {
        format!(
            "Public key pinning validation failed for '{}'",
            self.hostname
        )
    }

    fn into_protocol(self) -> Hpkp {
        Hpkp {
            date_time: Annotated::from(self.date_time.map(|d| d.to_rfc3339())),
            hostname: Annotated::new(self.hostname),
            port: Annotated::from(self.port),
            effective_expiration_date: Annotated::from(
                self.effective_expiration_date.map(|d| d.to_rfc3339()),
            ),
            include_subdomains: Annotated::from(self.include_subdomains),
            noted_hostname: Annotated::from(self.noted_hostname),
            served_certificate_chain: Annotated::from(
                self.served_certificate_chain
                    .map(|chain| chain.into_iter().map(Annotated::from).collect()),
            ),
            validated_certificate_chain: Annotated::from(
                self.validated_certificate_chain
                    .map(|chain| chain.into_iter().map(Annotated::from).collect()),
            ),
            known_pins: Annotated::new(self.known_pins.into_iter().map(Annotated::from).collect()),
            other: self
                .other
                .into_iter()
                .map(|(k, v)| (k, Annotated::from(v)))
                .collect(),
        }
    }

    fn get_tags(&self) -> Tags {
        let mut tags = vec![Annotated::new(TagEntry(
            Annotated::new("hostname".to_string()),
            Annotated::new(self.hostname.clone()),
        ))];

        if let Some(port) = self.port {
            tags.push(Annotated::new(TagEntry(
                Annotated::new("port".to_string()),
                Annotated::new(port.to_string()),
            )));
        }

        if let Some(include_subdomains) = self.include_subdomains {
            tags.push(Annotated::new(TagEntry(
                Annotated::new("include-subdomains".to_string()),
                Annotated::new(include_subdomains.to_string()),
            )));
        }

        Tags(PairList::from(tags))
    }

    fn get_request(&self) -> Request {
        Request {
            url: Annotated::from(self.hostname.clone()),
            ..Request::default()
        }
    }
}

/// Schema as defined in RFC7469, Section 3
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, ToValue, ProcessValue)]
pub struct Hpkp {
    /// > Indicates the time the UA observed the Pin Validation failure.
    pub date_time: Annotated<String>,
    /// > Hostname to which the UA made the original request that failed Pin Validation.
    pub hostname: Annotated<String>,
    /// > The port to which the UA made the original request that failed Pin Validation.
    pub port: Annotated<u64>,
    /// > Effective Expiration Date for the noted pins.
    pub effective_expiration_date: Annotated<String>,
    /// > Indicates whether or not the UA has noted the includeSubDomains directive for the Known
    /// Pinned Host.
    pub include_subdomains: Annotated<bool>,

    /// > Indicates the hostname that the UA noted when it noted the Known Pinned Host.  This field
    /// > allows operators to understand why Pin Validation was performed for, e.g., foo.example.com
    /// > when the noted Known Pinned Host was example.com with includeSubDomains set.
    pub noted_hostname: Annotated<String>,
    /// > The certificate chain, as served by the Known Pinned Host during TLS session setup.  It
    /// > is provided as an array of strings; each string pem1, ... pemN is the Privacy-Enhanced Mail
    /// > (PEM) representation of each X.509 certificate as described in [RFC7468].
    pub served_certificate_chain: Annotated<Array<String>>,
    /// > The certificate chain, as constructed by the UA during certificate chain verification.
    pub validated_certificate_chain: Annotated<Array<String>>,

    /// > Pins that the UA has noted for the Known Pinned Host.
    // TODO: regex ths string for 'pin-sha256="ABC123"' syntax
    #[metastructure(required = "true")]
    pub known_pins: Annotated<Array<String>>,

    #[metastructure(pii = "true", additional_properties)]
    pub other: Object<Value>,
}

impl Hpkp {
    pub fn parse_event(data: &[u8]) -> Result<Event, serde_json::Error> {
        let raw_hpkp = serde_json::from_slice::<HpkpRaw>(data)?;

        Ok(Event {
            logentry: Annotated::new(LogEntry::from(raw_hpkp.get_message())),
            tags: Annotated::new(raw_hpkp.get_tags()),
            request: Annotated::new(raw_hpkp.get_request()),
            hpkp: Annotated::new(raw_hpkp.into_protocol()),
            ..Event::default()
        })
    }
}

/// Defines external, RFC-defined schema we accept, while `ExpectStaple` defines our own schema.
///
/// See `ExpectStaple` for meaning of fields.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
struct ExpectStapleReportRaw {
    expect_staple_report: ExpectStapleRaw,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExpectStapleResponseStatus {
    Missing,
    Provided,
    ErrorResponse,
    BadProducedAt,
    NoMatchingResponse,
    InvalidDate,
    ParseResponseError,
    ParseResponseDataError,
}

impl fmt::Display for ExpectStapleResponseStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Self::Missing => write!(f, "MISSING"),
            Self::Provided => write!(f, "PROVIDED"),
            Self::ErrorResponse => write!(f, "ERROR_RESPONSE"),
            Self::BadProducedAt => write!(f, "BAD_PRODUCED_AT"),
            Self::NoMatchingResponse => write!(f, "NO_MATCHING_RESPONSE"),
            Self::InvalidDate => write!(f, "INVALID_DATE"),
            Self::ParseResponseError => write!(f, "PARSE_RESPONSE_ERROR"),
            Self::ParseResponseDataError => write!(f, "PARSE_RESPONSE_DATA_ERROR"),
        }
    }
}

impl FromStr for ExpectStapleResponseStatus {
    type Err = InvalidSecurityError;

    fn from_str(string: &str) -> Result<Self, Self::Err> {
        Ok(match string {
            "MISSING" => Self::Missing,
            "PROVIDED" => Self::Provided,
            "ERROR_RESPONSE" => Self::ErrorResponse,
            "BAD_PRODUCED_AT" => Self::BadProducedAt,
            "NO_MATCHING_RESPONSE" => Self::NoMatchingResponse,
            "INVALID_DATE" => Self::InvalidDate,
            "PARSE_RESPONSE_ERROR" => Self::ParseResponseError,
            "PARSE_RESPONSE_DATA_ERROR" => Self::ParseResponseDataError,
            _ => return Err(InvalidSecurityError),
        })
    }
}

impl_str_serde!(ExpectStapleResponseStatus);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExpectStapleCertStatus {
    Good,
    Revoked,
    Unknown,
}

impl fmt::Display for ExpectStapleCertStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Self::Good => write!(f, "GOOD"),
            Self::Revoked => write!(f, "REVOKED"),
            Self::Unknown => write!(f, "UNKNOWN"),
        }
    }
}

impl FromStr for ExpectStapleCertStatus {
    type Err = InvalidSecurityError;

    fn from_str(string: &str) -> Result<Self, Self::Err> {
        Ok(match string {
            "GOOD" => Self::Good,
            "REVOKED" => Self::Revoked,
            "UNKNOWN" => Self::Unknown,
            _ => return Err(InvalidSecurityError),
        })
    }
}

impl_str_serde!(ExpectStapleCertStatus);

/// Inner (useful) part of a Expect Stable report as sent by a user agent ( browser)
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
struct ExpectStapleRaw {
    #[serde(skip_serializing_if = "Option::is_none")]
    date_time: Option<DateTime<Utc>>,
    hostname: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    port: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    effective_expiration_date: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    response_status: Option<ExpectStapleResponseStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    ocsp_response: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cert_status: Option<ExpectStapleCertStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    served_certificate_chain: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    validated_certificate_chain: Option<Vec<String>>,
}

impl ExpectStapleRaw {
    fn get_message(&self) -> String {
        format!("Expect-Staple failed for '{}'", self.hostname)
    }

    fn into_protocol(self) -> ExpectStaple {
        ExpectStaple {
            date_time: Annotated::from(self.date_time.map(|d| d.to_rfc3339())),
            hostname: Annotated::from(self.hostname),
            port: Annotated::from(self.port),
            effective_expiration_date: Annotated::from(
                self.effective_expiration_date.map(|d| d.to_rfc3339()),
            ),
            response_status: Annotated::from(self.response_status.map(|rs| rs.to_string())),
            cert_status: Annotated::from(self.cert_status.map(|cs| cs.to_string())),
            served_certificate_chain: Annotated::from(
                self.served_certificate_chain
                    .map(|cert_chain| cert_chain.into_iter().map(Annotated::from).collect()),
            ),
            validated_certificate_chain: Annotated::from(
                self.validated_certificate_chain
                    .map(|cert_chain| cert_chain.into_iter().map(Annotated::from).collect()),
            ),
            ocsp_response: Annotated::from(self.ocsp_response),
        }
    }

    fn get_culprit(&self) -> String {
        self.hostname.clone()
    }

    fn get_tags(&self) -> Tags {
        let mut tags = vec![Annotated::new(TagEntry(
            Annotated::new("hostname".to_string()),
            Annotated::new(self.hostname.clone()),
        ))];

        if let Some(port) = self.port {
            tags.push(Annotated::new(TagEntry(
                Annotated::new("port".to_string()),
                Annotated::new(port.to_string()),
            )));
        }

        if let Some(response_status) = self.response_status {
            tags.push(Annotated::new(TagEntry(
                Annotated::new("response_status".to_string()),
                Annotated::new(response_status.to_string()),
            )));
        }

        if let Some(cert_status) = self.cert_status {
            tags.push(Annotated::new(TagEntry(
                Annotated::new("cert_status".to_string()),
                Annotated::new(cert_status.to_string()),
            )));
        }

        Tags(PairList::from(tags))
    }

    fn get_request(&self) -> Request {
        Request {
            url: Annotated::from(self.hostname.clone()),
            ..Request::default()
        }
    }
}

/// Represents an Expect Staple security report
/// See https://scotthelme.co.uk/ocsp-expect-staple/ for specification
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, ToValue, ProcessValue)]
pub struct ExpectStaple {
    date_time: Annotated<String>,
    hostname: Annotated<String>,
    port: Annotated<i64>,
    effective_expiration_date: Annotated<String>,
    response_status: Annotated<String>,
    cert_status: Annotated<String>,
    served_certificate_chain: Annotated<Array<String>>,
    validated_certificate_chain: Annotated<Array<String>>,
    ocsp_response: Annotated<Value>,
}

impl ExpectStaple {
    pub fn parse_event(data: &[u8]) -> Result<Event, serde_json::Error> {
        let raw_report = serde_json::from_slice::<ExpectStapleReportRaw>(data)?;
        let raw_expect_staple = raw_report.expect_staple_report;

        Ok(Event {
            logentry: Annotated::new(LogEntry::from(raw_expect_staple.get_message())),
            culprit: Annotated::new(raw_expect_staple.get_culprit()),
            tags: Annotated::new(raw_expect_staple.get_tags()),
            request: Annotated::new(raw_expect_staple.get_request()),
            expectstaple: Annotated::new(raw_expect_staple.into_protocol()),
            ..Event::default()
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SecurityReportType {
    Csp,
    ExpectCt,
    ExpectStaple,
    Hpkp,
}

impl SecurityReportType {
    /// Infers the type of a security report from its payload.
    ///
    /// This looks into the JSON payload and tries to infer the type from keys. If no report
    /// matches, an error is returned.
    pub fn from_json(data: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(data)
    }
}

impl<'de> Deserialize<'de> for SecurityReportType {
    fn deserialize<D>(deserializer: D) -> Result<Self, <D as Deserializer<'de>>::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(rename_all = "kebab-case")]
        struct SecurityReport {
            csp_report: Option<IgnoredAny>,
            known_pins: Option<IgnoredAny>,
            expect_staple_report: Option<IgnoredAny>,
            expect_ct_report: Option<IgnoredAny>,
        }

        let helper = SecurityReport::deserialize(deserializer)?;

        if helper.csp_report.is_some() {
            Ok(SecurityReportType::Csp)
        } else if helper.known_pins.is_some() {
            Ok(SecurityReportType::Hpkp)
        } else if helper.expect_staple_report.is_some() {
            Ok(SecurityReportType::ExpectStaple)
        } else if helper.expect_ct_report.is_some() {
            Ok(SecurityReportType::ExpectCt)
        } else {
            Err(D::Error::custom("Invalid security message type"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unsplit_uri() {
        assert_eq!(unsplit_uri("", ""), "");
        assert_eq!(unsplit_uri("data", ""), "data:");
        assert_eq!(unsplit_uri("data", "foo"), "data://foo");
        assert_eq!(unsplit_uri("http", ""), "http://");
        assert_eq!(unsplit_uri("http", "foo"), "http://foo");
    }

    #[test]
    fn test_normalize_uri() {
        // Special handling for self URIs
        assert_eq!(normalize_uri(""), "'self'");
        assert_eq!(normalize_uri("self"), "'self'");

        // Special handling for schema-only URIs
        assert_eq!(normalize_uri("data"), "data:");
        assert_eq!(normalize_uri("http"), "http://");

        // URIs without port
        assert_eq!(normalize_uri("http://notlocalhost/"), "notlocalhost");
        assert_eq!(normalize_uri("https://notlocalhost/"), "notlocalhost");
        assert_eq!(normalize_uri("data://notlocalhost/"), "data://notlocalhost");
        assert_eq!(normalize_uri("http://notlocalhost/lol.css"), "notlocalhost");

        // URIs with port
        assert_eq!(
            normalize_uri("http://notlocalhost:8000/"),
            "notlocalhost:8000"
        );
        assert_eq!(
            normalize_uri("http://notlocalhost:8000/lol.css"),
            "notlocalhost:8000"
        );

        // Invalid URIs
        assert_eq!(normalize_uri("xyz://notlocalhost/"), "xyz://notlocalhost");
    }

    #[test]
    fn test_csp_basic() {
        let json = r#"{
            "csp-report": {
                "document-uri": "http://example.com",
                "violated-directive": "style-src cdn.example.com",
                "blocked-uri": "http://example.com/lol.css",
                "effective-directive": "style-src"
            }
        }"#;

        let event = Csp::parse_event(json.as_bytes()).unwrap();

        assert_annotated_snapshot!(Annotated::new(event), @r###"
        {
          "culprit": "style-src cdn.example.com",
          "logentry": {
            "formatted": "Blocked 'style' from 'example.com'"
          },
          "request": {
            "url": "http://example.com"
          },
          "tags": [
            [
              "effective-directive",
              "style-src"
            ],
            [
              "blocked-uri",
              "http://example.com/lol.css"
            ]
          ],
          "csp": {
            "effective_directive": "style-src",
            "blocked_uri": "http://example.com/lol.css",
            "document_uri": "http://example.com",
            "violated_directive": "style-src cdn.example.com"
          }
        }
        "###);
    }

    #[test]
    fn test_csp_coerce_blocked_uri_if_missing() {
        let json = r#"{
            "csp-report": {
                "document-uri": "http://example.com",
                "effective-directive": "script-src"
            }
        }"#;

        let event = Csp::parse_event(json.as_bytes()).unwrap();

        assert_annotated_snapshot!(Annotated::new(event), @r###"
        {
          "culprit": "",
          "logentry": {
            "formatted": "Blocked unsafe (eval() or inline) 'script'"
          },
          "request": {
            "url": "http://example.com"
          },
          "tags": [
            [
              "effective-directive",
              "script-src"
            ],
            [
              "blocked-uri",
              "self"
            ]
          ],
          "csp": {
            "effective_directive": "script-src",
            "blocked_uri": "self",
            "document_uri": "http://example.com",
            "violated_directive": ""
          }
        }
        "###);
    }

    #[test]
    fn test_csp_msdn() {
        let json = r#"{
            "csp-report": {
                "document-uri": "https://example.com/foo/bar",
                "referrer": "https://www.google.com/",
                "violated-directive": "default-src self",
                "original-policy": "default-src self; report-uri /csp-hotline.php",
                "blocked-uri": "http://evilhackerscripts.com"
            }
        }"#;

        let event = Csp::parse_event(json.as_bytes()).unwrap();

        assert_annotated_snapshot!(Annotated::new(event), @r###"
       ⋮{
       ⋮  "culprit": "default-src self",
       ⋮  "logentry": {
       ⋮    "formatted": "Blocked 'default-src' from 'evilhackerscripts.com'"
       ⋮  },
       ⋮  "request": {
       ⋮    "url": "https://example.com/foo/bar",
       ⋮    "headers": [
       ⋮      [
       ⋮        "Referer",
       ⋮        "https://www.google.com/"
       ⋮      ]
       ⋮    ]
       ⋮  },
       ⋮  "tags": [
       ⋮    [
       ⋮      "effective-directive",
       ⋮      "default-src"
       ⋮    ],
       ⋮    [
       ⋮      "blocked-uri",
       ⋮      "http://evilhackerscripts.com"
       ⋮    ]
       ⋮  ],
       ⋮  "csp": {
       ⋮    "effective_directive": "default-src",
       ⋮    "blocked_uri": "http://evilhackerscripts.com",
       ⋮    "document_uri": "https://example.com/foo/bar",
       ⋮    "original_policy": "default-src self; report-uri /csp-hotline.php",
       ⋮    "referrer": "https://www.google.com/",
       ⋮    "violated_directive": "default-src self"
       ⋮  }
       ⋮}
        "###);
    }

    #[test]
    fn test_csp_real() {
        let json = r#"{
            "csp-report": {
                "document-uri": "https://sentry.io/sentry/csp/issues/88513416/",
                "referrer": "https://sentry.io/sentry/sentry/releases/7329107476ff14cfa19cf013acd8ce47781bb93a/",
                "violated-directive": "script-src",
                "effective-directive": "script-src",
                "original-policy": "default-src *; script-src 'make_csp_snapshot' 'unsafe-eval' 'unsafe-inline' e90d271df3e973c7.global.ssl.fastly.net cdn.ravenjs.com assets.zendesk.com ajax.googleapis.com ssl.google-analytics.com www.googleadservices.com analytics.twitter.com platform.twitter.com *.pingdom.net js.stripe.com api.stripe.com statuspage-production.s3.amazonaws.com s3.amazonaws.com *.google.com www.gstatic.com aui-cdn.atlassian.com *.atlassian.net *.jira.com *.zopim.com; font-src * data:; connect-src * wss://*.zopim.com; style-src 'make_csp_snapshot' 'unsafe-inline' e90d271df3e973c7.global.ssl.fastly.net s3.amazonaws.com aui-cdn.atlassian.com fonts.googleapis.com; img-src * data: blob:; report-uri https://sentry.io/api/54785/csp-report/?sentry_key=f724a8a027db45f5b21507e7142ff78e&sentry_release=39662eb9734f68e56b7f202260bb706be2f4cee7",
                "disposition": "enforce",
                "blocked-uri": "http://baddomain.com/test.js?_=1515535030116",
                "line-number": 24,
                "column-number": 66270,
                "source-file": "https://e90d271df3e973c7.global.ssl.fastly.net/_static/f0c7c026a4b2a3d2b287ae2d012c9924/sentry/dist/vendor.js",
                "status-code": 0,
                "script-sample": ""
            }
        }"#;

        let event = Csp::parse_event(json.as_bytes()).unwrap();

        assert_annotated_snapshot!(Annotated::new(event), @r###"
        {
          "culprit": "script-src",
          "logentry": {
            "formatted": "Blocked 'script' from 'baddomain.com'"
          },
          "request": {
            "url": "https://sentry.io/sentry/csp/issues/88513416/",
            "headers": [
              [
                "Referer",
                "https://sentry.io/sentry/sentry/releases/7329107476ff14cfa19cf013acd8ce47781bb93a/"
              ]
            ]
          },
          "tags": [
            [
              "effective-directive",
              "script-src"
            ],
            [
              "blocked-uri",
              "http://baddomain.com/test.js?_=1515535030116"
            ]
          ],
          "csp": {
            "effective_directive": "script-src",
            "blocked_uri": "http://baddomain.com/test.js?_=1515535030116",
            "document_uri": "https://sentry.io/sentry/csp/issues/88513416/",
            "original_policy": "default-src *; script-src 'make_csp_snapshot' 'unsafe-eval' 'unsafe-inline' e90d271df3e973c7.global.ssl.fastly.net cdn.ravenjs.com assets.zendesk.com ajax.googleapis.com ssl.google-analytics.com www.googleadservices.com analytics.twitter.com platform.twitter.com *.pingdom.net js.stripe.com api.stripe.com statuspage-production.s3.amazonaws.com s3.amazonaws.com *.google.com www.gstatic.com aui-cdn.atlassian.com *.atlassian.net *.jira.com *.zopim.com; font-src * data:; connect-src * wss://*.zopim.com; style-src 'make_csp_snapshot' 'unsafe-inline' e90d271df3e973c7.global.ssl.fastly.net s3.amazonaws.com aui-cdn.atlassian.com fonts.googleapis.com; img-src * data: blob:; report-uri https://sentry.io/api/54785/csp-report/?sentry_key=f724a8a027db45f5b21507e7142ff78e&sentry_release=39662eb9734f68e56b7f202260bb706be2f4cee7",
            "referrer": "https://sentry.io/sentry/sentry/releases/7329107476ff14cfa19cf013acd8ce47781bb93a/",
            "status_code": 0,
            "violated_directive": "script-src",
            "source_file": "https://e90d271df3e973c7.global.ssl.fastly.net/_static/f0c7c026a4b2a3d2b287ae2d012c9924/sentry/dist/vendor.js",
            "line_number": 24,
            "column_number": 66270,
            "script_sample": "",
            "disposition": "enforce"
          }
        }
        "###);
    }

    #[test]
    fn test_csp_culprit_0() {
        let json = r#"{
            "csp-report": {
                "document-uri": "http://example.com/foo",
                "violated-directive": "style-src http://cdn.example.com",
                "effective-directive": "style-src"
            }
        }"#;

        let event = Csp::parse_event(json.as_bytes()).unwrap();
        insta::assert_debug_snapshot!(event.culprit, @r###""style-src http://cdn.example.com""###);
    }

    #[test]
    fn test_csp_culprit_1() {
        let json = r#"{
            "csp-report": {
                "document-uri": "http://example.com/foo",
                "violated-directive": "style-src cdn.example.com",
                "effective-directive": "style-src"
            }
        }"#;

        let event = Csp::parse_event(json.as_bytes()).unwrap();
        insta::assert_debug_snapshot!(event.culprit, @r###""style-src cdn.example.com""###);
    }

    #[test]
    fn test_csp_culprit_2() {
        let json = r#"{
            "csp-report": {
                "document-uri": "https://example.com/foo",
                "violated-directive": "style-src cdn.example.com",
                "effective-directive": "style-src"
            }
        }"#;

        let event = Csp::parse_event(json.as_bytes()).unwrap();
        insta::assert_debug_snapshot!(event.culprit, @r###""style-src cdn.example.com""###);
    }

    #[test]
    fn test_csp_culprit_3() {
        let json = r#"{
            "csp-report": {
                "document-uri": "http://example.com/foo",
                "violated-directive": "style-src https://cdn.example.com",
                "effective-directive": "style-src"
            }
        }"#;

        let event = Csp::parse_event(json.as_bytes()).unwrap();
        insta::assert_debug_snapshot!(event.culprit, @r###""style-src https://cdn.example.com""###);
    }

    #[test]
    fn test_csp_culprit_4() {
        let json = r#"{
            "csp-report": {
                "document-uri": "http://example.com/foo",
                "violated-directive": "style-src http://example.com",
                "effective-directive": "style-src"
            }
        }"#;

        let event = Csp::parse_event(json.as_bytes()).unwrap();
        insta::assert_debug_snapshot!(event.culprit, @r###""style-src \'self\'""###);
    }

    #[test]
    fn test_csp_culprit_5() {
        let json = r#"{
            "csp-report": {
                "document-uri": "http://example.com/foo",
                "violated-directive": "style-src http://example2.com example.com",
                "effective-directive": "style-src"
            }
        }"#;

        let event = Csp::parse_event(json.as_bytes()).unwrap();
        insta::assert_debug_snapshot!(event.culprit, @r###""style-src http://example2.com \'self\'""###);
    }

    #[test]
    fn test_csp_tags_stripe() {
        // This is a regression test for potential PII in stripe URLs. PII stripping used to skip
        // report interfaces, which is why there is special handling.

        let json = r#"{
            "csp-report": {
                "document-uri": "https://example.com",
                "blocked-uri": "https://api.stripe.com/v1/tokens?card[number]=xxx",
                "effective-directive": "script-src"
            }
        }"#;

        let event = Csp::parse_event(json.as_bytes()).unwrap();
        insta::assert_debug_snapshot!(event.tags, @r###"
        Tags(
            PairList(
                [
                    TagEntry(
                        "effective-directive",
                        "script-src",
                    ),
                    TagEntry(
                        "blocked-uri",
                        "https://api.stripe.com/v1/tokens",
                    ),
                ],
            ),
        )
        "###);
    }

    #[test]
    fn test_csp_get_message_0() {
        let json = r#"{
            "csp-report": {
                "document-uri": "http://example.com/foo",
                "effective-directive": "img-src",
                "blocked-uri": "http://google.com/foo"
            }
        }"#;

        let event = Csp::parse_event(json.as_bytes()).unwrap();
        let message = &event.logentry.value().unwrap().formatted;
        insta::assert_debug_snapshot!(message, @r###""Blocked \'image\' from \'google.com\'""###);
    }

    #[test]
    fn test_csp_get_message_1() {
        let json = r#"{
            "csp-report": {
                "document-uri": "http://example.com/foo",
                "effective-directive": "style-src",
                "blocked-uri": ""
            }
        }"#;

        let event = Csp::parse_event(json.as_bytes()).unwrap();
        let message = &event.logentry.value().unwrap().formatted;
        insta::assert_debug_snapshot!(message, @r###""Blocked inline \'style\'""###);
    }

    #[test]
    fn test_csp_get_message_2() {
        let json = r#"{
            "csp-report": {
                "document-uri": "http://example.com/foo",
                "effective-directive": "script-src",
                "blocked-uri": "",
                "violated-directive": "script-src 'unsafe-inline'"
            }
        }"#;

        let event = Csp::parse_event(json.as_bytes()).unwrap();
        let message = &event.logentry.value().unwrap().formatted;
        insta::assert_debug_snapshot!(message, @r###""Blocked unsafe inline \'script\'""###);
    }

    #[test]
    fn test_csp_get_message_3() {
        let json = r#"{
            "csp-report": {
                "document-uri": "http://example.com/foo",
                "effective-directive": "script-src",
                "blocked-uri": "",
                "violated-directive": "script-src 'unsafe-eval'"
            }
        }"#;

        let event = Csp::parse_event(json.as_bytes()).unwrap();
        let message = &event.logentry.value().unwrap().formatted;
        insta::assert_debug_snapshot!(message, @r###""Blocked unsafe eval() \'script\'""###);
    }

    #[test]
    fn test_csp_get_message_4() {
        let json = r#"{
            "csp-report": {
                "document-uri": "http://example.com/foo",
                "effective-directive": "script-src",
                "blocked-uri": "",
                "violated-directive": "script-src example.com"
            }
        }"#;

        let event = Csp::parse_event(json.as_bytes()).unwrap();
        let message = &event.logentry.value().unwrap().formatted;
        insta::assert_debug_snapshot!(message, @r###""Blocked unsafe (eval() or inline) \'script\'""###);
    }

    #[test]
    fn test_csp_get_message_5() {
        let json = r#"{
            "csp-report": {
                "document-uri": "http://example.com/foo",
                "effective-directive": "script-src",
                "blocked-uri": "data:text/plain;base64,SGVsbG8sIFdvcmxkIQ%3D%3D"
            }
        }"#;

        let event = Csp::parse_event(json.as_bytes()).unwrap();
        let message = &event.logentry.value().unwrap().formatted;
        insta::assert_debug_snapshot!(message, @r###""Blocked \'script\' from \'data:\'""###);
    }

    #[test]
    fn test_csp_get_message_6() {
        let json = r#"{
            "csp-report": {
                "document-uri": "http://example.com/foo",
                "effective-directive": "script-src",
                "blocked-uri": "data"
            }
        }"#;

        let event = Csp::parse_event(json.as_bytes()).unwrap();
        let message = &event.logentry.value().unwrap().formatted;
        insta::assert_debug_snapshot!(message, @r###""Blocked \'script\' from \'data:\'""###);
    }

    #[test]
    fn test_csp_get_message_7() {
        let json = r#"{
            "csp-report": {
                "document-uri": "http://example.com/foo",
                "effective-directive": "style-src-elem",
                "blocked-uri": "http://fonts.google.com/foo"
            }
        }"#;

        let event = Csp::parse_event(json.as_bytes()).unwrap();
        let message = &event.logentry.value().unwrap().formatted;
        insta::assert_debug_snapshot!(message, @r###""Blocked \'style\' from \'fonts.google.com\'""###);
    }

    #[test]
    fn test_csp_get_message_8() {
        let json = r#"{
            "csp-report": {
                "document-uri": "http://example.com/foo",
                "effective-directive": "script-src-elem",
                "blocked-uri": "http://cdn.ajaxapis.com/foo"
            }
        }"#;

        let event = Csp::parse_event(json.as_bytes()).unwrap();
        let message = &event.logentry.value().unwrap().formatted;
        insta::assert_debug_snapshot!(message, @r###""Blocked \'script\' from \'cdn.ajaxapis.com\'""###);
    }

    #[test]
    fn test_csp_get_message_9() {
        let json = r#"{
            "csp-report": {
                "document-uri": "http://notlocalhost:8000/",
                "effective-directive": "style-src",
                "blocked-uri": "http://notlocalhost:8000/lol.css"
            }
        }"#;

        let event = Csp::parse_event(json.as_bytes()).unwrap();
        let message = &event.logentry.value().unwrap().formatted;
        insta::assert_debug_snapshot!(message, @r###""Blocked \'style\' from \'notlocalhost:8000\'""###);
    }

    #[test]
    fn test_expectct_basic() {
        let json = r#"{
            "expect-ct-report": {
                "date-time": "2014-04-06T13:00:50Z",
                "hostname": "www.example.com",
                "port": 443,
                "effective-expiration-date": "2014-05-01T12:40:50Z",
                "served-certificate-chain": ["-----BEGIN CERTIFICATE-----\n-----END CERTIFICATE-----"],
                "validated-certificate-chain": ["-----BEGIN CERTIFICATE-----\n-----END CERTIFICATE-----"],
                "scts": [
                    {
                        "version": 1,
                        "status": "invalid",
                        "source": "embedded",
                        "serialized_sct": "ABCD=="
                    }
                ]
            }
        }"#;

        let event = ExpectCt::parse_event(json.as_bytes()).unwrap();
        assert_annotated_snapshot!(Annotated::new(event), @r###"
        {
          "culprit": "www.example.com",
          "logentry": {
            "formatted": "Expect-CT failed for 'www.example.com'"
          },
          "request": {
            "url": "www.example.com"
          },
          "tags": [
            [
              "hostname",
              "www.example.com"
            ],
            [
              "port",
              "443"
            ]
          ],
          "expectct": {
            "date_time": "2014-04-06T13:00:50+00:00",
            "hostname": "www.example.com",
            "port": 443,
            "effective_expiration_date": "2014-05-01T12:40:50+00:00",
            "served_certificate_chain": [
              "-----BEGIN CERTIFICATE-----\n-----END CERTIFICATE-----"
            ],
            "validated_certificate_chain": [
              "-----BEGIN CERTIFICATE-----\n-----END CERTIFICATE-----"
            ],
            "scts": [
              {
                "version": 1,
                "status": "invalid",
                "source": "embedded",
                "serialized_sct": "ABCD=="
              }
            ]
          }
        }
        "###);
    }

    #[test]
    fn test_expectct_invalid() {
        let json = r#"{
            "hostname": "www.example.com",
            "date_time": "Not an RFC3339 datetime"
        }"#;

        ExpectCt::parse_event(json.as_bytes()).expect_err("date_time should fail to parse");
    }

    #[test]
    fn test_expectstaple_basic() {
        let json = r#"{
            "expect-staple-report": {
                "date-time": "2014-04-06T13:00:50Z",
                "hostname": "www.example.com",
                "port": 443,
                "response-status": "ERROR_RESPONSE",
                "cert-status": "REVOKED",
                "effective-expiration-date": "2014-05-01T12:40:50Z",
                "served-certificate-chain": ["-----BEGIN CERTIFICATE-----\n-----END CERTIFICATE-----"],
                "validated-certificate-chain": ["-----BEGIN CERTIFICATE-----\n-----END CERTIFICATE-----"]
            }
        }"#;

        let event = ExpectStaple::parse_event(json.as_bytes()).unwrap();
        assert_annotated_snapshot!(Annotated::new(event), @r###"
        {
          "culprit": "www.example.com",
          "logentry": {
            "formatted": "Expect-Staple failed for 'www.example.com'"
          },
          "request": {
            "url": "www.example.com"
          },
          "tags": [
            [
              "hostname",
              "www.example.com"
            ],
            [
              "port",
              "443"
            ],
            [
              "response_status",
              "ERROR_RESPONSE"
            ],
            [
              "cert_status",
              "REVOKED"
            ]
          ],
          "expectstaple": {
            "date_time": "2014-04-06T13:00:50+00:00",
            "hostname": "www.example.com",
            "port": 443,
            "effective_expiration_date": "2014-05-01T12:40:50+00:00",
            "response_status": "ERROR_RESPONSE",
            "cert_status": "REVOKED",
            "served_certificate_chain": [
              "-----BEGIN CERTIFICATE-----\n-----END CERTIFICATE-----"
            ],
            "validated_certificate_chain": [
              "-----BEGIN CERTIFICATE-----\n-----END CERTIFICATE-----"
            ]
          }
        }
        "###);
    }

    #[test]
    fn test_hpkp_basic() {
        let json = r#"{
            "date-time": "2014-04-06T13:00:50Z",
            "hostname": "example.com",
            "port": 443,
            "effective-expiration-date": "2014-05-01T12:40:50Z",
            "include-subdomains": false,
            "served-certificate-chain": ["-----BEGIN CERTIFICATE-----\n-----END CERTIFICATE-----"],
            "validated-certificate-chain": ["-----BEGIN CERTIFICATE-----\n-----END CERTIFICATE-----"],
            "known-pins": ["pin-sha256=\"E9CZ9INDbd+2eRQozYqqbQ2yXLVKB9+xcprMF+44U1g=\""]
        }"#;

        let event = Hpkp::parse_event(json.as_bytes()).unwrap();
        assert_annotated_snapshot!(Annotated::new(event), @r###"
        {
          "logentry": {
            "formatted": "Public key pinning validation failed for 'example.com'"
          },
          "request": {
            "url": "example.com"
          },
          "tags": [
            [
              "hostname",
              "example.com"
            ],
            [
              "port",
              "443"
            ],
            [
              "include-subdomains",
              "false"
            ]
          ],
          "hpkp": {
            "date_time": "2014-04-06T13:00:50+00:00",
            "hostname": "example.com",
            "port": 443,
            "effective_expiration_date": "2014-05-01T12:40:50+00:00",
            "include_subdomains": false,
            "served_certificate_chain": [
              "-----BEGIN CERTIFICATE-----\n-----END CERTIFICATE-----"
            ],
            "validated_certificate_chain": [
              "-----BEGIN CERTIFICATE-----\n-----END CERTIFICATE-----"
            ],
            "known_pins": [
              "pin-sha256=\"E9CZ9INDbd+2eRQozYqqbQ2yXLVKB9+xcprMF+44U1g=\""
            ]
          }
        }
        "###);
    }

    #[test]
    fn test_security_report_type_deserializer_recognizes_csp_reports() {
        let csp_report_text = r#"{
            "csp-report": {
                "document-uri": "https://example.com/foo/bar",
                "referrer": "https://www.google.com/",
                "violated-directive": "default-src self",
                "original-policy": "default-src self; report-uri /csp-hotline.php",
                "blocked-uri": "http://evilhackerscripts.com"
            }
        }"#;

        let report_type: SecurityReportType = serde_json::from_str(csp_report_text).unwrap();
        assert_eq!(report_type, SecurityReportType::Csp);
    }

    #[test]
    fn test_security_report_type_deserializer_recognizes_expect_ct_reports() {
        let expect_ct_report_text = r#"{
            "expect-ct-report": {
                "date-time": "2014-04-06T13:00:50Z",
                "hostname": "www.example.com",
                "port": 443,
                "effective-expiration-date": "2014-05-01T12:40:50Z",
                "served-certificate-chain": [
                    "-----BEGIN CERTIFICATE-----\n-----END CERTIFICATE-----"
                ],
                "validated-certificate-chain": [
                    "-----BEGIN CERTIFICATE-----\n-----END CERTIFICATE-----"
                ],
                "scts": [
                    {
                        "version": 1,
                        "status": "invalid",
                        "source": "embedded",
                        "serialized_sct": "ABCD=="
                    }
                ]
            }
        }"#;

        let report_type: SecurityReportType = serde_json::from_str(expect_ct_report_text).unwrap();
        assert_eq!(report_type, SecurityReportType::ExpectCt);
    }

    #[test]
    fn test_security_report_type_deserializer_recognizes_expect_staple_reports() {
        let expect_staple_report_text = r#"{
             "expect-staple-report": {
                "date-time": "2014-04-06T13:00:50Z",
                "hostname": "www.example.com",
                "port": 443,
                "response-status": "ERROR_RESPONSE",
                "cert-status": "REVOKED",
                "effective-expiration-date": "2014-05-01T12:40:50Z",
                "served-certificate-chain": ["-----BEGIN CERTIFICATE-----\n-----END CERTIFICATE-----"],
                "validated-certificate-chain": ["-----BEGIN CERTIFICATE-----\n-----END CERTIFICATE-----"]
            }
        }"#;
        let report_type: SecurityReportType =
            serde_json::from_str(expect_staple_report_text).unwrap();
        assert_eq!(report_type, SecurityReportType::ExpectStaple);
    }

    #[test]
    fn test_security_report_type_deserializer_recognizes_hpkp_reports() {
        let hpkp_report_text = r#"{
            "date-time": "2014-04-06T13:00:50Z",
            "hostname": "www.example.com",
            "port": 443,
            "effective-expiration-date": "2014-05-01T12:40:50Z",
            "include-subdomains": false,
            "served-certificate-chain": [
              "-----BEGIN CERTIFICATE-----\n MIIEBDCCAuygAwIBAgIDAjppMA0GCSqGSIb3DQEBBQUAMEIxCzAJBgNVBAYTAlVT\n... -----END CERTIFICATE-----"
            ],
            "validated-certificate-chain": [
              "-----BEGIN CERTIFICATE-----\n MIIEBDCCAuygAwIBAgIDAjppMA0GCSqGSIb3DQEBBQUAMEIxCzAJBgNVBAYTAlVT\n... -----END CERTIFICATE-----"
            ],
            "known-pins": [
              "pin-sha256=\"d6qzRu9zOECb90Uez27xWltNsj0e1Md7GkYYkVoZWmM=\"",
              "pin-sha256=\"E9CZ9INDbd+2eRQozYqqbQ2yXLVKB9+xcprMF+44U1g=\""
            ]
          }"#;

        let report_type: SecurityReportType = serde_json::from_str(hpkp_report_text).unwrap();
        assert_eq!(report_type, SecurityReportType::Hpkp);
    }
}
