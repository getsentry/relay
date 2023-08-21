//! Contains definitions for the Network Error Logging (NEL) interface.

use std::borrow::Cow;
use std::fmt::{self, Write};

use serde::{Deserialize, Serialize};
use url::Url;

use crate::macros::derive_fromstr_and_display;
use crate::protocol::{
    Event, HeaderName, HeaderValue, Headers, LogEntry, PairList, Request, TagEntry, Tags,
};
use crate::types::{Annotated, Array, Object, Value};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct InvalidNelError;

impl fmt::Display for InvalidNelError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid nel report")
    }
}

// #[derive(Clone, Copy, Debug, PartialEq, Eq)]
// pub enum NelReportField {
//     Age,
//     Type,
//     Url,
//     UserAgent,

//     Body,
// }

// derive_fromstr_and_display!(NelReportField, InvalidNelError, {
//     NelReportField::Age => "age",
//     NelReportField::Type => "type",
//     NelReportField::Url => "url",
//     NelReportField::UserAgent => "user_agent",

//     NelReportField::Body => "body",
// });

// relay_common::impl_str_serde!(NelReportField, "a nel report field");

// #[derive(Clone, Copy, Debug, PartialEq, Eq)]
// pub enum NelBodyField {
//     ElapsedTime,
//     Method,
//     Phase,
//     Protocol,
//     Referrer,
//     SamplingFraction,
//     ServerIp,
//     StatusCode,
//     Type,
// }

// derive_fromstr_and_display!(NelBodyField, InvalidNelError, {
//     NelBodyField::ElapsedTime => "elapsed_time",
//     NelBodyField::Method => "method",
//     NelBodyField::Phase => "phase",
//     NelBodyField::Protocol => "protocol",
//     NelBodyField::Referrer => "referrer",
//     NelBodyField::SamplingFraction => "sampling_fraction",
//     NelBodyField::ServerIp => "server_ip",
//     NelBodyField::StatusCode => "status_code",
//     NelBodyField::Type => "type",
// });

// relay_common::impl_str_serde!(NelBodyField, "a nel body field");

// #[derive(Clone, Copy, Debug, PartialEq, Eq)]
// pub enum NelPhase {
//     Dns,
//     Connection,
//     Application,
// }

// derive_fromstr_and_display!(NelPhase, InvalidNelError, {
//     NelPhase::Dns => "dns",
//     NelPhase::Connection => "connection",
//     NelPhase::Application => "application",
// });

// relay_common::impl_str_serde!(NelPhase, "a nel phase");

// #[derive(Clone, Copy, Debug, PartialEq, Eq)]
// pub enum NelType {
//     // https://w3c.github.io/network-error-logging/#dns-resolution-errors
//     DnsUnreachable,
//     DnsNameNotResolved,
//     DnsFailed,
//     DnsAddressChanged,

//     // https://w3c.github.io/network-error-logging/#secure-connection-establishment-errors
//     TcpTimedOut,
//     TcpClosed,
//     TcpReset,
//     TcpRefused,
//     TcpAborted,
//     TcpAddressInvalid,
//     TcpAddressUnreachable,
//     TcpFailed,
//     TlsVersionOrCipherMismatch,
//     TlsBadClientAuthCert,
//     TlsCertNameInvalid,
//     TlsCertDateInvalid,
//     TlsCertAuthorityInvalid,
//     TlsCertInvalid,
//     TlsCertRevoked,
//     TlsCertPinnedKeyNotInCertChain,
//     TlsProtocolError,
//     TlsFailed,

//     // https://w3c.github.io/network-error-logging/#transmission-of-request-and-response-errors
//     HttpError,
//     HttpProtocolError,
//     HttpResponseInvalid,
//     HttpResponseRedirectLoop,
//     HttpFailed,

//     Abandoned,
//     Unknown,
// }

// derive_fromstr_and_display!(NelType, InvalidNelError, {
//     // https://w3c.github.io/network-error-logging/#dns-resolution-errors
//     NelType::DnsUnreachable => "dns.unreachable",
//     NelType::DnsNameNotResolved => "dns.name_not_resolved",
//     NelType::DnsFailed => "dns.failed",
//     NelType::DnsAddressChanged => "dns.address_changed",

//     // https://w3c.github.io/network-error-logging/#secure-connection-establishment-errors
//     NelType::TcpTimedOut => "tcp.timed_out",
//     NelType::TcpClosed => "tcp.closed",
//     NelType::TcpReset => "tcp.reset",
//     NelType::TcpRefused => "tcp.refused",
//     NelType::TcpAborted => "tcp.aborted",
//     NelType::TcpAddressInvalid => "tcp.address_invalid",
//     NelType::TcpAddressUnreachable => "tcp.address_unreachable",
//     NelType::TcpFailed => "tcp.failed",
//     NelType::TlsVersionOrCipherMismatch => "tls.version_or_cipher_mismatch",
//     NelType::TlsBadClientAuthCert => "tls.bad_client_auth_cert",
//     NelType::TlsCertNameInvalid => "tls.cert.name_invalid",
//     NelType::TlsCertDateInvalid => "tls.cert.date_invalid",
//     NelType::TlsCertAuthorityInvalid => "tls.cert.authority_invalid",
//     NelType::TlsCertInvalid => "tls.cert.invalid",
//     NelType::TlsCertRevoked => "tls.cert.revoked",
//     NelType::TlsCertPinnedKeyNotInCertChain => "tls.cert.pinned_key_not_in_cert_chain",
//     NelType::TlsProtocolError => "tls.protocol.error",
//     NelType::TlsFailed => "tls.failed",

//     // https://w3c.github.io/network-error-logging/#transmission-of-request-and-response-errors
//     NelType::HttpError => "http.error",
//     NelType::HttpProtocolError => "http.protocol.error",
//     NelType::HttpResponseInvalid => "http.response.invalid",
//     NelType::HttpResponseRedirectLoop => "http.response.redirect_loop",
//     NelType::HttpFailed => "http.failed",
//     NelType::Abandoned => "abandoned",
//     NelType::Unknown => "unknown",
// });

// relay_common::impl_str_serde!(NelType, "a nel type");

// fn is_local(uri: &str) -> bool {
//     matches!(uri, "" | "self" | "'self'")
// }

// fn schema_uses_host(schema: &str) -> bool {
//     // List of schemas with host (netloc) from Python's urlunsplit:
//     // see <https://github.com/python/cpython/blob/1eac437e8da106a626efffe9fce1cb47dbf5be35/Lib/urllib/parse.py#L51>
//     //
//     // Only modification: "" is set to false, since there is a separate check in the urlunsplit
//     // implementation that omits the leading "//" in that case.
//     matches!(
//         schema,
//         "ftp"
//             | "http"
//             | "gopher"
//             | "nntp"
//             | "telnet"
//             | "imap"
//             | "wais"
//             | "file"
//             | "mms"
//             | "https"
//             | "shttp"
//             | "snews"
//             | "prospero"
//             | "rtsp"
//             | "rtspu"
//             | "rsync"
//             | "svn"
//             | "svn+ssh"
//             | "sftp"
//             | "nfs"
//             | "git"
//             | "git+ssh"
//             | "ws"
//             | "wss"
//     )
// }

// /// Mimicks Python's urlunsplit with all its quirks.
// fn unsplit_uri(schema: &str, host: &str) -> String {
//     if !host.is_empty() || schema_uses_host(schema) {
//         format!("{schema}://{host}")
//     } else if !schema.is_empty() {
//         format!("{schema}:{host}")
//     } else {
//         String::new()
//     }
// }

// fn normalize_uri(value: &str) -> Cow<'_, str> {
//     if is_local(value) {
//         return Cow::Borrowed("'self'");
//     }

//     // A lot of these values get reported as literally just the scheme. So a value like 'data'
//     // or 'blob', which are valid schemes, just not a uri. So we want to normalize it into a
//     // URI.

//     if !value.contains(':') {
//         return Cow::Owned(unsplit_uri(value, ""));
//     }

//     let url = match Url::parse(value) {
//         Ok(url) => url,
//         Err(_) => return Cow::Borrowed(value),
//     };

//     let normalized = match url.scheme() {
//         "http" | "https" => Cow::Borrowed(url.host_str().unwrap_or_default()),
//         scheme => Cow::Owned(unsplit_uri(scheme, url.host_str().unwrap_or_default())),
//     };

//     Cow::Owned(match url.port() {
//         Some(port) => format!("{normalized}:{port}"),
//         None => normalized.into_owned(),
//     })
// }

// /// Inner (useful) part of a NEL report.
// ///
// /// See `Nel` for meaning of fields.
// #[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
// #[serde(rename_all = "kebab-case")]
// struct NelBodyRaw {
//     // TODO: use of "Annotated<" ?
//     elapsed_time: u64, // TODO: not sure if "Option<u64>"
//     method: String,
//     phase: NelPhase,
//     protocol: String,
//     referrer: String,
//     sampling_fraction: f32,
//     server_ip: String, // TODO: IpAddr
//     status_code: u64,  // TODO: Annotated<u64>
//     r#type: NelType,
// }

// impl NelBodyRaw {
//     // fn default_blocked_uri() -> String {
//     //     "self".to_string()
//     // }

//     // fn effective_directive(&self) -> Result<NelField, InvalidNelError> {
//     //     // Firefox doesn't send effective-directive, so parse it from
//     //     // violated-directive but prefer effective-directive when present.
//     //     // refs: https://bugzil.la/1192684#c8

//     //     if let Some(directive) = &self.effective_directive {
//     //         // In C2P1 and CSP2, violated_directive and possibly effective_directive might contain
//     //         // more information than just the CSP-directive.
//     //         if let Ok(parsed_directive) = directive
//     //             .split_once(' ')
//     //             .map_or(directive.as_str(), |s| s.0)
//     //             .parse()
//     //         {
//     //             return Ok(parsed_directive);
//     //         }
//     //     }

//     //     if let Ok(parsed_directive) = self
//     //         .violated_directive
//     //         .split_once(' ')
//     //         .map_or(self.violated_directive.as_str(), |s| s.0)
//     //         .parse()
//     //     {
//     //         Ok(parsed_directive)
//     //     } else {
//     //         Err(InvalidNelError)
//     //     }
//     // }

//     // fn get_message(&self, effective_directive: NelField) -> String {
//     //     if is_local(&self.blocked_uri) {
//     //         match effective_directive {
//     //             NelField::ChildSrc => "Blocked inline 'child'".to_string(),
//     //             NelField::ConnectSrc => "Blocked inline 'connect'".to_string(),
//     //             NelField::FontSrc => "Blocked inline 'font'".to_string(),
//     //             NelField::ImgSrc => "Blocked inline 'image'".to_string(),
//     //             NelField::ManifestSrc => "Blocked inline 'manifest'".to_string(),
//     //             NelField::MediaSrc => "Blocked inline 'media'".to_string(),
//     //             NelField::ObjectSrc => "Blocked inline 'object'".to_string(),
//     //             NelField::ScriptSrcAttr => "Blocked unsafe 'script' element".to_string(),
//     //             NelField::ScriptSrcElem => "Blocked inline script attribute".to_string(),
//     //             NelField::StyleSrc => "Blocked inline 'style'".to_string(),
//     //             NelField::StyleSrcElem => "Blocked 'style' or 'link' element".to_string(),
//     //             NelField::StyleSrcAttr => "Blocked style attribute".to_string(),
//     //             NelField::ScriptSrc => {
//     //                 if self.violated_directive.contains("'unsafe-inline'") {
//     //                     "Blocked unsafe inline 'script'".to_string()
//     //                 } else if self.violated_directive.contains("'unsafe-eval'") {
//     //                     "Blocked unsafe eval() 'script'".to_string()
//     //                 } else {
//     //                     "Blocked unsafe (eval() or inline) 'script'".to_string()
//     //                 }
//     //             }
//     //             directive => format!("Blocked inline '{directive}'"),
//     //         }
//     //     } else {
//     //         let uri = normalize_uri(&self.blocked_uri);

//     //         match effective_directive {
//     //             NelField::ChildSrc => format!("Blocked 'child' from '{uri}'"),
//     //             NelField::ConnectSrc => format!("Blocked 'connect' from '{uri}'"),
//     //             NelField::FontSrc => format!("Blocked 'font' from '{uri}'"),
//     //             NelField::FormAction => format!("Blocked 'form' action to '{uri}'"),
//     //             NelField::ImgSrc => format!("Blocked 'image' from '{uri}'"),
//     //             NelField::ManifestSrc => format!("Blocked 'manifest' from '{uri}'"),
//     //             NelField::MediaSrc => format!("Blocked 'media' from '{uri}'"),
//     //             NelField::ObjectSrc => format!("Blocked 'object' from '{uri}'"),
//     //             NelField::ScriptSrc => format!("Blocked 'script' from '{uri}'"),
//     //             NelField::ScriptSrcAttr => {
//     //                 format!("Blocked inline script attribute from '{uri}'")
//     //             }
//     //             NelField::ScriptSrcElem => format!("Blocked 'script' from '{uri}'"),
//     //             NelField::StyleSrc => format!("Blocked 'style' from '{uri}'"),
//     //             NelField::StyleSrcElem => format!("Blocked 'style' from '{uri}'"),
//     //             NelField::StyleSrcAttr => format!("Blocked style attribute from '{uri}'"),
//     //             directive => format!("Blocked '{directive}' from '{uri}'"),
//     //         }
//     //     }
//     // }

//     // fn into_protocol(self, effective_directive: NelField) -> Nel {
//     //     Nel {
//     //         effective_directive: Annotated::from(effective_directive.to_string()),
//     //         blocked_uri: Annotated::from(self.blocked_uri),
//     //         document_uri: Annotated::from(self.document_uri),
//     //         original_policy: Annotated::from(self.original_policy),
//     //         referrer: Annotated::from(self.referrer),
//     //         status_code: Annotated::from(self.status_code),
//     //         violated_directive: Annotated::from(self.violated_directive),
//     //         source_file: Annotated::from(self.source_file),
//     //         line_number: Annotated::from(self.line_number),
//     //         column_number: Annotated::from(self.column_number),
//     //         script_sample: Annotated::from(self.script_sample),
//     //         disposition: Annotated::from(self.disposition),
//     //         other: self
//     //             .other
//     //             .into_iter()
//     //             .map(|(k, v)| (k, Annotated::from(v)))
//     //             .collect(),
//     //     }
//     // }

//     // fn sanitized_blocked_uri(&self) -> String {
//     //     // HACK: This is 100% to work around Stripe urls that will casually put extremely sensitive
//     //     // information in querystrings. The real solution is to apply data scrubbing to all tags
//     //     // generically.
//     //     //
//     //     //    if netloc == 'api.stripe.com':
//     //     //      query = '' fragment = ''

//     //     let mut uri = self.blocked_uri.clone();

//     //     if uri.starts_with("https://api.stripe.com/") {
//     //         if let Some(index) = uri.find(&['#', '?'][..]) {
//     //             uri.truncate(index);
//     //         }
//     //     }

//     //     uri
//     // }

//     // fn normalize_value<'a>(&self, value: &'a str, document_uri: &str) -> Cow<'a, str> {
//     //     // > If no scheme is specified, the same scheme as the one used to access the protected
//     //     // > document is assumed.
//     //     // Source: https://developer.mozilla.org/en-US/docs/Web/Security/CSP/CSP_policy_directives
//     //     if let "'none'" | "'self'" | "'unsafe-inline'" | "'unsafe-eval'" = value {
//     //         return Cow::Borrowed(value);
//     //     }

//     //     // Normalize a value down to 'self' if it matches the origin of document-uri FireFox
//     //     // transforms a 'self' value into the spelled out origin, so we want to reverse this and
//     //     // bring it back.
//     //     if value.starts_with("data:")
//     //         || value.starts_with("mediastream:")
//     //         || value.starts_with("blob:")
//     //         || value.starts_with("filesystem:")
//     //         || value.starts_with("http:")
//     //         || value.starts_with("https:")
//     //         || value.starts_with("file:")
//     //     {
//     //         if document_uri == normalize_uri(value) {
//     //             return Cow::Borrowed("'self'");
//     //         }

//     //         // Their rule had an explicit scheme, so let's respect that
//     //         return Cow::Borrowed(value);
//     //     }

//     //     // Value doesn't have a scheme, but let's see if their hostnames match at least, if so,
//     //     // they're the same.
//     //     if value == document_uri {
//     //         return Cow::Borrowed("'self'");
//     //     }

//     //     // Now we need to stitch on a scheme to the value, but let's not stitch on the boring
//     //     // values.
//     //     let original_uri = match self.document_uri {
//     //         Some(ref u) => u,
//     //         None => "",
//     //     };

//     //     match original_uri.split_once(':').map(|x| x.0) {
//     //         None | Some("http" | "https") => Cow::Borrowed(value),
//     //         Some(scheme) => Cow::Owned(unsplit_uri(scheme, value)),
//     //     }
//     // }

//     // fn get_culprit(&self) -> String {
//     //     if self.violated_directive.is_empty() {
//     //         return String::new();
//     //     }

//     //     let mut bits = self.violated_directive.split_ascii_whitespace();
//     //     let mut culprit = bits.next().unwrap_or_default().to_owned();

//     //     let document_uri = self.document_uri.as_deref().unwrap_or("");
//     //     let normalized_uri = normalize_uri(document_uri);

//     //     for bit in bits {
//     //         write!(culprit, " {}", self.normalize_value(bit, &normalized_uri)).ok();
//     //     }

//     //     culprit
//     // }

//     // fn get_tags(&self, effective_directive: NelField) -> Tags {
//     //     Tags(PairList::from(vec![
//     //         Annotated::new(TagEntry(
//     //             Annotated::new("effective-directive".to_string()),
//     //             Annotated::new(effective_directive.to_string()),
//     //         )),
//     //         Annotated::new(TagEntry(
//     //             Annotated::new("blocked-uri".to_string()),
//     //             Annotated::new(self.sanitized_blocked_uri()),
//     //         )),
//     //     ]))
//     // }

//     // fn get_request(&self) -> Request {
//     //     let headers = match self.referrer {
//     //         Some(ref referrer) if !referrer.is_empty() => {
//     //             Annotated::new(Headers(PairList(vec![Annotated::new((
//     //                 Annotated::new(HeaderName::new("Referer")),
//     //                 Annotated::new(HeaderValue::new(referrer.clone())),
//     //             ))])))
//     //         }
//     //         Some(_) | None => Annotated::empty(),
//     //     };

//     //     Request {
//     //         url: Annotated::from(self.document_uri.clone()),
//     //         headers,
//     //         ..Request::default()
//     //     }
//     // }
// }

/// Defines external, RFC-defined schema we accept, while `Nel` defines our own schema.
///
/// See `Nel` for meaning of fields.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
struct NelReportRaw {
    age: Option<u64>,
    #[serde(rename = "type")]
    ty: Option<String>, // always "network-error"
    url: Option<String>,
    user_agent: Option<String>,
    // body: NelBodyRaw,
}

impl NelReportRaw {
    fn into_protocol(self) -> Nel {
        Nel {
            age: Annotated::from(self.age),
            ty: Annotated::from(self.ty),
            url: Annotated::from(self.url),
            user_agent: Annotated::from(self.user_agent),
            // other: Annotated::empty(),
        }
    }
}

/// Models the content of a NEL report.
///
/// NOTE: This is the structure used inside the Event (serialization is based on Annotated
/// infrastructure). We also use a version of this structure to deserialize from raw JSON
/// via serde.
///
///
/// See <https://www.w3.org/TR/CSP3/>
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct Nel {
    /// TODO
    #[metastructure(pii = "true")]
    pub age: Annotated<u64>,
    /// TODO
    #[metastructure(pii = "true")]
    pub ty: Annotated<String>,
    /// The URL of the document in which the error occurred.
    #[metastructure(pii = "true")]
    pub url: Annotated<String>,
    /// The User-Agent HTTP header.
    pub user_agent: Annotated<String>,
    // Additional arbitrary fields for forwards compatibility.
    // #[metastructure(pii = "true", additional_properties)]
    // pub other: Annotated<Object<Value>>,
}

// derive_fromstr_and_display!(Nel, InvalidNelError, {
//     Nel::Age => "age",
//     Nel::Type => "type",
//     Nel::Url => "url",
//     Nel::UserAgent => "user_agent",

//     NelReportField::Body => "body",
// });

impl Nel {
    pub fn apply_to_event(data: &[u8], event: &mut Event) -> Result<(), serde_json::Error> {
        let raw_report = serde_json::from_slice::<NelReportRaw>(data)?;
        // let raw_body = raw_report.body;

        event.logentry = Annotated::new(LogEntry::from(String::from("hello world")));

        event.nel = Annotated::from(raw_report.into_protocol());

        // let effective_directive = raw_csp
        //     .effective_directive()
        //     .map_err(serde::de::Error::custom)?;

        // event.logentry = Annotated::new(LogEntry::from(raw_csp.get_message(effective_directive)));
        // event.culprit = Annotated::new(raw_csp.get_culprit());
        // event.tags = Annotated::new(raw_csp.get_tags(effective_directive));
        // event.request = Annotated::new(raw_csp.get_request());

        // event.nel = Annotated::new(raw_report.into_protocol(effective_directive));

        Ok(())
    }
}

// relay_common::impl_str_serde!(Nel, "a nel ");

// TODO: tests
