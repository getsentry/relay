//! Transforms Heroku Logplex syslog messages to Sentry Logs.
//!
//! Heroku's [HTTPS log drains](https://devcenter.heroku.com/articles/log-drains#https-drains)
//! send batches of syslog messages framed with the octet counting method of
//! [RFC 6587](https://tools.ietf.org/html/rfc6587#section-3.4.1):
//!
//! ```text
//! 83 <40>1 2012-11-30T06:45:29+00:00 host app web.3 - State changed from starting to up
//! ```
//!
//! The messages themselves are almost [RFC 5424](https://tools.ietf.org/html/rfc5424) syslog
//! messages, but Logplex omits the `STRUCTURED-DATA` field instead of sending a `NILVALUE`.

use std::borrow::Cow;
use std::net::{IpAddr, Ipv6Addr};

use chrono::{DateTime, Utc};
use relay_conventions::attributes::*;
use relay_event_schema::protocol::{
    AttributeValue, Attributes, OurLog, OurLogLevel, Timestamp, TraceId,
};
use relay_protocol::{Annotated, Meta, Remark, RemarkType};

/// Attribute names for Heroku logs.
///
/// TODO: Replace with the generated constants from `relay_conventions` once the attributes are
/// available in the pinned `sentry-conventions` version.
mod attributes {
    pub const HEROKU__DYNO__NAME: &str = "heroku.dyno.name";
    pub const HEROKU__ERROR__CODE: &str = "heroku.error.code";
    pub const HEROKU__ERROR__DESCRIPTION: &str = "heroku.error.description";
    pub const HEROKU__LOGPLEX__DRAIN_TOKEN: &str = "heroku.logplex.drain_token";
    pub const HEROKU__ROUTER__REQUEST_ID: &str = "heroku.router.request_id";
    pub const HEROKU__ROUTER__CONNECT: &str = "heroku.router.connect";
    pub const HEROKU__ROUTER__SERVICE: &str = "heroku.router.service";
    pub const TLS__PROTOCOL__NAME: &str = "tls.protocol.name";
    pub const TLS__PROTOCOL__VERSION: &str = "tls.protocol.version";
}

use self::attributes::*;

/// The number of messages encoded in the body of the request.
///
/// For the example body above, this value is 1. Use this field to ensure that you’ve parsed the body correctly.
pub const HEROKU_LOGPLEX_HEADER_MSG_COUNT: &str = "Logplex-Msg-Count";
/// The drain token for the log drain sending the request.
pub const HEROKU_LOGPLEX_HEADER_DRAIN_TOKEN: &str = "Logplex-Drain-Token";

/// The maximum amount of digits accepted in the octet count of a frame.
///
/// Logplex batches are around 100KB, nine digits leave plenty of room while making sure the
/// count cannot overflow.
const MAX_FRAME_LENGTH_DIGITS: usize = 9;

/// The syslog `APP-NAME` Heroku uses for messages emitted by the platform itself.
const HEROKU_SOURCE: &str = "heroku";

/// The syslog `PROCID` Heroku uses for messages emitted by the HTTP router.
const ROUTER_DYNO: &str = "router";

/// An error while parsing a Logplex payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum LogplexError {
    /// The octet count of a frame is missing or not a valid number.
    #[error("invalid frame length")]
    InvalidFrameLength,
    /// The payload ended before the end of a frame.
    #[error("truncated frame")]
    TruncatedFrame,
    /// The `PRI` part of the syslog header is missing or invalid.
    #[error("invalid syslog priority")]
    InvalidPriority,
    /// The syslog version is not supported.
    #[error("unsupported syslog version")]
    UnsupportedVersion,
    /// The syslog header ended prematurely.
    #[error("incomplete syslog header")]
    IncompleteHeader,
}

/// Iterator over the octet counted frames of a Logplex payload.
///
/// Returned by [`parse_logplex_frames`].
#[derive(Debug, Clone)]
pub struct LogplexFrames<'a> {
    rest: &'a [u8],
}

impl<'a> Iterator for LogplexFrames<'a> {
    type Item = Result<&'a [u8], LogplexError>;

    fn next(&mut self) -> Option<Self::Item> {
        // Logplex terminates every message with a newline, which is included in the octet count.
        // Tolerate additional whitespace between frames from other senders.
        let input = self.rest.trim_ascii_start();
        if input.is_empty() {
            return None;
        }

        match split_frame(input) {
            Ok((frame, rest)) => {
                self.rest = rest;
                Some(Ok(frame))
            }
            Err(err) => {
                // The frame boundaries are lost after an invalid frame, stop parsing.
                self.rest = &[];
                Some(Err(err))
            }
        }
    }
}

/// Splits the first frame (`<length> <message>`) off the input.
fn split_frame(input: &[u8]) -> Result<(&[u8], &[u8]), LogplexError> {
    let space = input
        .iter()
        .take(MAX_FRAME_LENGTH_DIGITS + 1)
        .position(|&b| b == b' ')
        .ok_or(LogplexError::InvalidFrameLength)?;

    let (length, rest) = input.split_at(space);
    if length.is_empty() || !length.iter().all(u8::is_ascii_digit) {
        return Err(LogplexError::InvalidFrameLength);
    }

    let length = std::str::from_utf8(length)
        .ok()
        .and_then(|length| length.parse::<usize>().ok())
        .filter(|&length| length > 0)
        .ok_or(LogplexError::InvalidFrameLength)?;

    // Skip the space separating the length from the message.
    let rest = rest.get(1..).unwrap_or_default();
    rest.split_at_checked(length)
        .ok_or(LogplexError::TruncatedFrame)
}

/// Splits a Logplex payload into its octet counted frames.
///
/// Frames are returned as bytes, as Logplex does not guarantee they are valid UTF-8. Parsing
/// stops after the first invalid frame.
pub fn parse_logplex_frames(payload: &[u8]) -> LogplexFrames<'_> {
    LogplexFrames { rest: payload }
}

/// A syslog message sent by Logplex.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogplexMessage<'a> {
    /// The syslog severity (0-7) from the `PRI` header.
    pub severity: u8,
    /// The timestamp of the message, `None` if missing or invalid.
    pub timestamp: Option<DateTime<Utc>>,
    /// The `APP-NAME` header, which Heroku sets to the source of the message: `app` for
    /// messages emitted by the application, `heroku` for messages emitted by the platform.
    pub source: Option<&'a str>,
    /// The `PROCID` header, which Heroku sets to the dyno or platform component, e.g. `web.1` or
    /// `router`.
    pub dyno: Option<&'a str>,
    /// The message body.
    pub msg: &'a str,
}

/// Parses a single syslog message from a Logplex frame.
///
/// The `HOSTNAME` and `MSGID` headers are discarded, Logplex always sends `host` and `-`.
pub fn parse_logplex_message(frame: &str) -> Result<LogplexMessage<'_>, LogplexError> {
    let rest = frame.strip_suffix('\n').unwrap_or(frame);
    let rest = rest.strip_suffix('\r').unwrap_or(rest);

    let (priority, rest) = rest
        .strip_prefix('<')
        .and_then(|rest| rest.split_once('>'))
        .ok_or(LogplexError::InvalidPriority)?;
    let severity = parse_priority(priority)? & 0x07;

    let (version, rest) = rest.split_once(' ').ok_or(LogplexError::IncompleteHeader)?;
    if version != "1" {
        return Err(LogplexError::UnsupportedVersion);
    }

    let mut parts = rest.splitn(6, ' ');
    let mut next_part = || parts.next().ok_or(LogplexError::IncompleteHeader);
    let timestamp = next_part()?;
    let _hostname = next_part()?;
    let source = next_part()?;
    let dyno = next_part()?;
    let _msg_id = next_part()?;
    let msg = parts.next().unwrap_or_default();

    Ok(LogplexMessage {
        severity,
        timestamp: nil(timestamp)
            .and_then(|ts| DateTime::parse_from_rfc3339(ts).ok())
            .map(|ts| ts.with_timezone(&Utc)),
        source: nil(source),
        dyno: nil(dyno),
        msg,
    })
}

/// Parses the numeric `PRI` value, which must be within `0..=191`.
fn parse_priority(priority: &str) -> Result<u8, LogplexError> {
    if priority.is_empty() || priority.len() > 3 || !priority.bytes().all(|b| b.is_ascii_digit()) {
        return Err(LogplexError::InvalidPriority);
    }

    priority
        .parse::<u8>()
        .ok()
        .filter(|&priority| priority <= 191)
        .ok_or(LogplexError::InvalidPriority)
}

/// Returns `None` for the syslog `NILVALUE` (`-`) and empty header values.
fn nil(value: &str) -> Option<&str> {
    (!value.is_empty() && value != "-").then_some(value)
}

/// Maps syslog severity (0-7) to Sentry log level.
///
/// Mapping follows [OpenTelemetry syslog semantic conventions](https://opentelemetry.io/docs/specs/otel/logs/data-model-appendix/#appendix-b-severitynumber-example-mappings)
fn map_syslog_severity_to_level(severity: u8) -> OurLogLevel {
    match severity {
        0 => OurLogLevel::Fatal, // Emergency
        1 => OurLogLevel::Fatal, // Alert
        2 => OurLogLevel::Fatal, // Critical
        3 => OurLogLevel::Error, // Error
        4 => OurLogLevel::Warn,  // Warning
        5 => OurLogLevel::Info,  // Notice
        6 => OurLogLevel::Info,  // Informational
        7 => OurLogLevel::Debug, // Debug
        _ => OurLogLevel::Info,
    }
}

/// Iterator over the `key=value` pairs of a [logfmt](https://brandur.org/logfmt) message.
///
/// Values may be wrapped in double quotes to contain spaces, a backslash escapes the following
/// character within quotes. Words without a `=` are skipped.
struct Logfmt<'a>(&'a str);

impl<'a> Iterator for Logfmt<'a> {
    type Item = (&'a str, Cow<'a, str>);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let input = self.0.trim_start_matches(' ');
            if input.is_empty() {
                return None;
            }

            let key_end = input.find(['=', ' ']).unwrap_or(input.len());
            let (key, rest) = input.split_at(key_end);

            let Some(rest) = rest.strip_prefix('=') else {
                // A word without a value.
                self.0 = rest;
                continue;
            };

            let (value, rest) = match rest.strip_prefix('"') {
                Some(quoted) => split_quoted(quoted),
                None => {
                    let (value, rest) = rest.split_at(rest.find(' ').unwrap_or(rest.len()));
                    (Cow::Borrowed(value), rest)
                }
            };
            self.0 = rest;

            if !key.is_empty() {
                return Some((key, value));
            }
        }
    }
}

/// Splits a quoted logfmt value at its closing quote, the opening quote is already removed.
///
/// Without a closing quote, the value extends to the end of the input.
fn split_quoted(input: &str) -> (Cow<'_, str>, &str) {
    let mut escaped = false;
    for (index, c) in input.char_indices() {
        match c {
            _ if escaped => escaped = false,
            '\\' => escaped = true,
            '"' => {
                let (value, rest) = input.split_at(index);
                return (unescape(value), rest.get(1..).unwrap_or_default());
            }
            _ => {}
        }
    }

    (unescape(input), "")
}

/// Removes the backslashes of escape sequences in a quoted logfmt value.
fn unescape(value: &str) -> Cow<'_, str> {
    if !value.contains('\\') {
        return Cow::Borrowed(value);
    }

    let mut unescaped = String::with_capacity(value.len());
    let mut chars = value.chars();
    while let Some(c) = chars.next() {
        match c {
            '\\' => unescaped.push(chars.next().unwrap_or('\\')),
            c => unescaped.push(c),
        }
    }

    Cow::Owned(unescaped)
}

/// Extracts a Heroku error code from a platform message like `Error R14 (Memory quota exceeded)`.
///
/// Returns the code and the description in parentheses, if any. See
/// <https://devcenter.heroku.com/articles/error-codes>.
fn parse_error_code(msg: &str) -> Option<(&str, Option<&str>)> {
    let rest = msg.strip_prefix("Error ")?;

    let code = rest.get(..3)?;
    let mut bytes = code.bytes();
    if !matches!(bytes.next(), Some(b'H' | b'L' | b'R')) || !bytes.all(|b| b.is_ascii_digit()) {
        return None;
    }

    let rest = rest.get(3..)?;
    if rest.starts_with(|c: char| c.is_ascii_alphanumeric()) {
        return None;
    }

    let description = rest
        .strip_prefix(" (")
        .and_then(|rest| rest.split_once(')'))
        .map(|(description, _)| description)
        .filter(|description| !description.is_empty());

    Some((code, description))
}

/// The type of a router field value.
#[derive(Debug, Clone, Copy)]
enum RouterValue {
    /// A string.
    String,
    /// An integer.
    Integer,
    /// A time measurement in ms.
    Millis,
    /// The HTTP request method.
    Method,
    /// The HTTP host.
    Host,
    /// Path and query string of the request, the query is written to `url.query`.
    Path,
    /// The `tls` boolean, converted to the URL scheme.
    Scheme,
    /// The router's `http1.1` and `http2.0`, converted to the OTel versions `1.1` and `2`.
    /// Legacy `http` and `https` values identify the protocol without specifying a version.
    HttpVersion,
    /// The router's `tls1.3`, converted to `1.3`. Heroku also reports `unknown`, which is dropped.
    TlsVersion,
    /// The `X-Forwarded-For` value, reduced to the peer address appended by the Heroku router.
    /// Earlier addresses are untrusted. Values which are not an IP address are dropped.
    ClientAddress,
}

/// Returns the attribute name and value type of a documented router field.
fn router_field(key: &str) -> Option<(&'static str, RouterValue)> {
    use RouterValue::*;

    Some(match key {
        "code" => (HEROKU__ERROR__CODE, String),
        "desc" => (HEROKU__ERROR__DESCRIPTION, String),
        "method" => (HTTP__REQUEST__METHOD, Method),
        "path" => (URL__PATH, Path),
        "host" => (SERVER__ADDRESS, Host),
        "request_id" => (HEROKU__ROUTER__REQUEST_ID, String),
        "fwd" => (CLIENT__ADDRESS, ClientAddress),
        "dyno" => (HEROKU__DYNO__NAME, String),
        "connect" => (HEROKU__ROUTER__CONNECT, Millis),
        "service" => (HEROKU__ROUTER__SERVICE, Millis),
        "status" => (HTTP__RESPONSE__STATUS_CODE, Integer),
        "protocol" => (NETWORK__PROTOCOL__VERSION, HttpVersion),
        "tls" => (URL__SCHEME, Scheme),
        "tls_version" => (TLS__PROTOCOL__VERSION, TlsVersion),
        _ => return None,
    })
}

/// Splits a router host into its address and optional port, removing IPv6 brackets.
fn parse_host(value: &str) -> Option<(&str, Option<u16>)> {
    let (address, port) = if let Some(rest) = value.strip_prefix('[') {
        let (address, rest) = rest.split_once(']')?;
        address.parse::<Ipv6Addr>().ok()?;
        let port = match rest.is_empty() {
            true => None,
            false => Some(rest.strip_prefix(':')?),
        };
        (address, port)
    } else if value.parse::<IpAddr>().is_ok() {
        (value, None)
    } else {
        let (address, port) = match value.split_once(':') {
            Some((address, port)) => (address, Some(port)),
            None => (value, None),
        };
        url::Host::parse(address).ok()?;
        (address, port)
    };

    let port = match port {
        Some(port) if !port.is_empty() && port.bytes().all(|byte| byte.is_ascii_digit()) => {
            Some(port.parse().ok()?)
        }
        Some(_) => return None,
        None => None,
    };

    Some((address, port))
}

/// Whether a Logplex process ID names an application dyno rather than a platform component.
fn is_dyno_name(value: &str) -> bool {
    value.rsplit_once('.').is_some_and(|(process, number)| {
        !process.is_empty()
            && !number.is_empty()
            && number.bytes().all(|byte| byte.is_ascii_digit())
    })
}

/// Adds the attributes of a Heroku router message and returns the log level it reports.
///
/// Only the documented [router fields](https://devcenter.heroku.com/articles/http-routing#heroku-router-log-format)
/// are extracted.
///
/// The router logs `path` and `fwd` with values chosen by the client of the app. If Heroku does
/// not escape quotes in them, they can contain fake `key=value` pairs. The router logs its fields
/// in a fixed order starting with `at`, `code`, `desc`, `method` and `path`, so:
///  - `at`, `code`, `desc` and `method` are only accepted before the first `path`, they determine
///    the log level and error code,
///  - the first `path` is accepted,
///  - for all other fields the last value wins, the genuine value follows injected ones.
///
/// `host` and `request_id`, which are logged between `path` and `fwd`, can still be replaced by
/// pairs injected through `fwd`.
///
/// `client.address` trusts only the rightmost address of `fwd`, appended by the
/// [Heroku router](https://devcenter.heroku.com/articles/http-routing#heroku-headers).
/// Earlier addresses may have been supplied by the client or by an untrusted proxy.
///
/// `bytes` is retained only in the body because its meaning depends on runtime metadata that
/// Logplex does not provide.
fn add_router_attributes(attributes: &mut Attributes, msg: &str) -> Option<OurLogLevel> {
    let mut seen_path = false;
    let mut level = None;

    for (key, value) in Logfmt(msg) {
        match key {
            "at" | "code" | "desc" | "method" | "path" if seen_path => continue,
            "at" => {
                level = match &*value {
                    "trace" => Some(OurLogLevel::Trace),
                    "debug" => Some(OurLogLevel::Debug),
                    "info" => Some(OurLogLevel::Info),
                    "error" => Some(OurLogLevel::Error),
                    "warn" | "warning" => Some(OurLogLevel::Warn),
                    "fatal" => Some(OurLogLevel::Fatal),
                    _ => None,
                };
                continue;
            }
            "path" => seen_path = true,
            _ => {}
        }

        let Some((name, ty)) = router_field(key) else {
            continue;
        };

        // Fields that cannot be converted produce `None`, like empty ones, so they still replace
        // injected values below.
        let value: Option<AttributeValue> = match ty {
            RouterValue::String | RouterValue::Method if value.is_empty() => None,
            RouterValue::String => Some(value.into_owned().into()),
            RouterValue::Integer => value.parse::<i64>().ok().map(Into::into),
            RouterValue::Millis => value
                .strip_suffix("ms")
                .unwrap_or(&value)
                .parse::<f64>()
                .ok()
                .filter(|value| value.is_finite())
                .map(Into::into),
            RouterValue::Method => Some(
                match &*value {
                    "CONNECT" | "DELETE" | "GET" | "HEAD" | "OPTIONS" | "PATCH" | "POST"
                    | "PUT" | "QUERY" | "TRACE" => value.into_owned(),
                    _ => "_OTHER".to_owned(),
                }
                .into(),
            ),
            RouterValue::Host => {
                attributes.remove(SERVER__PORT);
                parse_host(&value).map(|(address, port)| {
                    if let Some(port) = port {
                        attributes.insert(SERVER__PORT, i64::from(port));
                    }
                    address.to_owned().into()
                })
            }
            RouterValue::Path => {
                let (path, query) = value.split_once('?').unwrap_or((&value, ""));
                // Only the first `path` is accepted, the query does not have to be replaced later.
                if !query.is_empty() {
                    attributes.insert(URL__QUERY, query.to_owned());
                }
                (!path.is_empty()).then(|| path.to_owned().into())
            }
            RouterValue::Scheme => value.parse::<bool>().ok().map(|tls| {
                let scheme = match tls {
                    true => "https",
                    false => "http",
                };
                scheme.to_owned().into()
            }),
            RouterValue::HttpVersion => {
                attributes.remove(NETWORK__PROTOCOL__NAME);
                let version = match &*value {
                    "http1.0" => Some("1.0"),
                    "http1.1" => Some("1.1"),
                    "http2.0" => Some("2"),
                    _ => None,
                };
                if version.is_some() || matches!(&*value, "http" | "https") {
                    attributes.insert(NETWORK__PROTOCOL__NAME, "http".to_owned());
                }
                version.map(|version| version.to_owned().into())
            }
            RouterValue::TlsVersion => {
                attributes.remove(TLS__PROTOCOL__NAME);
                match &*value {
                    "tls1.0" | "tls1.1" | "tls1.2" | "tls1.3" => {
                        attributes.insert(TLS__PROTOCOL__NAME, "tls".to_owned());
                        value
                            .strip_prefix("tls")
                            .map(|version| version.to_owned().into())
                    }
                    _ => None,
                }
            }
            RouterValue::ClientAddress => value
                .rsplit(',')
                .next()
                .map(str::trim)
                .filter(|address| address.parse::<IpAddr>().is_ok())
                .map(|address| address.to_owned().into()),
        };

        // Later values of a field replace earlier ones, also when they are empty.
        match value {
            Some(value) => attributes.insert(name, value),
            None => drop(attributes.remove(name)),
        }
    }

    level
}

/// Converts a parsed Logplex message to a Sentry log.
///
/// `drain_token` is the `Logplex-Drain-Token` header identifying the delivery drain.
pub fn logplex_message_to_sentry_log(
    message: LogplexMessage<'_>,
    drain_token: Option<&str>,
) -> OurLog {
    let LogplexMessage {
        severity,
        timestamp,
        source,
        dyno,
        msg,
    } = message;

    let mut attributes = Attributes::default();

    macro_rules! add_optional_attribute {
        ($name:expr, $value:expr) => {{
            if let Some(value) = $value {
                attributes.insert($name, value.to_owned());
            }
        }};
    }

    attributes.insert(SENTRY__ORIGIN, "auto.log_drain.heroku".to_owned());
    attributes.insert(CLOUD__PROVIDER, "heroku".to_owned());
    add_optional_attribute!(HEROKU__DYNO__NAME, dyno.filter(|dyno| is_dyno_name(dyno)));
    add_optional_attribute!(HEROKU__LOGPLEX__DRAIN_TOKEN, drain_token);

    let mut level = map_syslog_severity_to_level(severity);

    // Only messages emitted by the Heroku platform contain router fields and error codes,
    // application output is kept as is.
    if source == Some(HEROKU_SOURCE) {
        if dyno == Some(ROUTER_DYNO) {
            if let Some(router_level) = add_router_attributes(&mut attributes, msg) {
                level = router_level;
            }
        } else if let Some((code, description)) = parse_error_code(msg) {
            attributes.insert(HEROKU__ERROR__CODE, code.to_owned());
            add_optional_attribute!(HEROKU__ERROR__DESCRIPTION, description);
            level = OurLogLevel::Error;
        }
    }

    let timestamp = match timestamp {
        Some(timestamp) => Annotated::new(Timestamp(timestamp)),
        None => {
            let mut meta = Meta::default();
            meta.add_remark(Remark::new(RemarkType::Substituted, "timestamp.invalid"));
            Annotated(Some(Timestamp(Utc::now())), meta)
        }
    };

    OurLog {
        timestamp,
        // Logplex messages carry no trace context.
        trace_id: TraceId::try_from_str_or_random(""),
        level: Annotated::new(level),
        body: Annotated::new(msg.to_owned()),
        attributes: Annotated::new(attributes),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use relay_protocol::{SerializableAnnotated, Value};

    use super::*;

    const DRAIN_TOKEN: Option<&str> = Some("d.73ea7440-270a-435a-a0ea-adf50b4e5f5a");

    fn frames(payload: &str) -> Vec<Result<&str, LogplexError>> {
        parse_logplex_frames(payload.as_bytes())
            .map(|frame| frame.map(|frame| std::str::from_utf8(frame).unwrap()))
            .collect()
    }

    /// Converts a message to a log, removing the random trace id to keep snapshots stable.
    fn to_log_with_drain_token(line: &str, drain_token: Option<&str>) -> Annotated<OurLog> {
        let message = parse_logplex_message(line).unwrap();
        let mut log = logplex_message_to_sentry_log(message, drain_token);

        assert!(log.trace_id.value().is_some());
        assert!(!log.trace_id.meta().is_empty());
        log.trace_id = Annotated::empty();

        Annotated::new(log)
    }

    fn to_log(line: &str) -> Annotated<OurLog> {
        to_log_with_drain_token(line, None)
    }

    #[test]
    fn test_frames_documentation_example() {
        // https://devcenter.heroku.com/articles/log-drains#https-drains
        let payload = "83 <40>1 2012-11-30T06:45:29+00:00 host app web.3 - State changed from starting to up\n\
            119 <40>1 2012-11-30T06:45:26+00:00 host app web.3 - Starting process with command `bundle exec rackup config.ru -p 24405`\n";

        insta::assert_debug_snapshot!(frames(payload), @r#"
        [
            Ok(
                "<40>1 2012-11-30T06:45:29+00:00 host app web.3 - State changed from starting to up\n",
            ),
            Ok(
                "<40>1 2012-11-30T06:45:26+00:00 host app web.3 - Starting process with command `bundle exec rackup config.ru -p 24405`\n",
            ),
        ]
        "#);
    }

    #[test]
    fn test_frames_with_newline_in_message() {
        let frame = |msg: &str| format!("{} {msg}", msg.len());
        let payload = frame("<190>1 - - - - a\nb\n") + &frame("<190>1 - - - - c");
        assert_eq!(
            frames(&payload),
            vec![Ok("<190>1 - - - - a\nb\n"), Ok("<190>1 - - - - c")]
        );
    }

    #[test]
    fn test_frames_separated_by_whitespace() {
        let payload = "\r\n 3 abc\n\n3 def\n";
        assert_eq!(frames(payload), vec![Ok("abc"), Ok("def")]);
    }

    #[test]
    fn test_frames_invalid() {
        assert_eq!(frames(""), vec![]);
        assert_eq!(frames("abc"), vec![Err(LogplexError::InvalidFrameLength)]);
        assert_eq!(frames("0 abc"), vec![Err(LogplexError::InvalidFrameLength)]);
        assert_eq!(
            frames("-1 abc"),
            vec![Err(LogplexError::InvalidFrameLength)]
        );
        assert_eq!(
            frames("+3 abc"),
            vec![Err(LogplexError::InvalidFrameLength)]
        );
        assert_eq!(frames("3abc"), vec![Err(LogplexError::InvalidFrameLength)]);
        assert_eq!(frames("3"), vec![Err(LogplexError::InvalidFrameLength)]);
        assert_eq!(frames("4 abc"), vec![Err(LogplexError::TruncatedFrame)]);
        assert_eq!(frames("3 "), vec![Err(LogplexError::TruncatedFrame)]);
        assert_eq!(
            frames("99999999999999999999999 abc"),
            vec![Err(LogplexError::InvalidFrameLength)]
        );
        assert_eq!(
            frames("999999999 abc"),
            vec![Err(LogplexError::TruncatedFrame)]
        );
        // Parsing stops after the first invalid frame.
        assert_eq!(
            frames("3 abc x 3 def"),
            vec![Ok("abc"), Err(LogplexError::InvalidFrameLength)]
        );
    }

    #[test]
    fn test_frames_split_inside_multibyte_character() {
        // The frame ends in the middle of the `ü`, the frame is still returned as bytes and the
        // remaining byte is not a valid frame.
        let frames: Vec<_> = parse_logplex_frames("2 aü".as_bytes()).collect();
        assert_eq!(
            frames,
            vec![Ok(&b"a\xc3"[..]), Err(LogplexError::InvalidFrameLength)]
        );
    }

    #[test]
    fn test_parse_message_documentation_example() {
        let msg = parse_logplex_message(
            "<40>1 2012-11-30T06:45:29+00:00 host app web.3 - State changed from starting to up\n",
        )
        .unwrap();

        insta::assert_debug_snapshot!(msg, @r#"
        LogplexMessage {
            severity: 0,
            timestamp: Some(
                2012-11-30T06:45:29Z,
            ),
            source: Some(
                "app",
            ),
            dyno: Some(
                "web.3",
            ),
            msg: "State changed from starting to up",
        }
        "#);
    }

    #[test]
    fn test_parse_message_nil_values() {
        let msg = parse_logplex_message("<190>1 - - - - - -").unwrap();
        assert_eq!(
            msg,
            LogplexMessage {
                severity: 6,
                timestamp: None,
                source: None,
                dyno: None,
                msg: "-",
            }
        );

        let msg = parse_logplex_message("<190>1 - - - - ").unwrap();
        assert_eq!(msg.msg, "");

        let msg = parse_logplex_message("<190>1 - - - - -").unwrap();
        assert_eq!(msg.msg, "");
    }

    #[test]
    fn test_parse_message_invalid_timestamp() {
        let msg = parse_logplex_message("<190>1 yesterday host app web.1 - hi").unwrap();
        assert_eq!(msg.timestamp, None);
        assert_eq!(msg.msg, "hi");
    }

    #[test]
    fn test_parse_message_invalid() {
        let parse = |s| parse_logplex_message(s).unwrap_err();

        assert_eq!(parse(""), LogplexError::InvalidPriority);
        assert_eq!(parse("hello"), LogplexError::InvalidPriority);
        assert_eq!(parse("<>1 - - - - - hi"), LogplexError::InvalidPriority);
        assert_eq!(parse("<192>1 - - - - - hi"), LogplexError::InvalidPriority);
        assert_eq!(parse("<999>1 - - - - - hi"), LogplexError::InvalidPriority);
        assert_eq!(parse("<0190>1 - - - - - hi"), LogplexError::InvalidPriority);
        assert_eq!(parse("<+19>1 - - - - - hi"), LogplexError::InvalidPriority);
        assert_eq!(parse("<190"), LogplexError::InvalidPriority);
        assert_eq!(
            parse("<190>2 - - - - - hi"),
            LogplexError::UnsupportedVersion
        );
        assert_eq!(parse("<190>1"), LogplexError::IncompleteHeader);
        assert_eq!(parse("<190>1 - - -"), LogplexError::IncompleteHeader);
        assert_eq!(parse("<190>1 - - - -"), LogplexError::IncompleteHeader);
    }

    fn logfmt(msg: &str) -> Vec<String> {
        Logfmt(msg)
            .map(|(key, value)| format!("{key}={value}"))
            .collect()
    }

    #[test]
    fn test_logfmt() {
        assert_eq!(
            logfmt(r#"at=error  desc="Request timeout" word path="/a b" =x dyno= fwd="1.2.3.4"#),
            vec![
                "at=error",
                "desc=Request timeout",
                "path=/a b",
                "dyno=",
                "fwd=1.2.3.4",
            ]
        );

        assert_eq!(logfmt(""), Vec::<String>::new());
        assert_eq!(logfmt("   "), Vec::<String>::new());
        assert_eq!(logfmt("="), Vec::<String>::new());
        assert_eq!(logfmt("a=\""), vec!["a="]);
        assert_eq!(logfmt("ü=ö ä"), vec!["ü=ö"]);
    }

    #[test]
    fn test_logfmt_escapes() {
        assert_eq!(
            logfmt(r#"path="/x\" at=error code=H99" host=a"#),
            vec![r#"path=/x" at=error code=H99"#, "host=a"]
        );
        assert_eq!(logfmt(r#"a="b\\" c=d"#), vec![r"a=b\", "c=d"]);
        assert_eq!(logfmt(r#"a="b\"#), vec![r"a=b\"]);
        assert_eq!(logfmt(r#"a="ü\ö""#), vec!["a=üö"]);
        // Escapes have no meaning outside of quotes.
        assert_eq!(logfmt(r#"a=b\" c=d"#), vec![r#"a=b\""#, "c=d"]);
    }

    #[test]
    fn test_parse_error_code() {
        assert_eq!(
            parse_error_code("Error R14 (Memory quota exceeded)"),
            Some(("R14", Some("Memory quota exceeded")))
        );
        assert_eq!(
            parse_error_code(
                "Error R10 (Boot timeout) -> Web process failed to bind to $PORT within 60 seconds of launch"
            ),
            Some(("R10", Some("Boot timeout")))
        );
        assert_eq!(
            parse_error_code(
                "Error L10 (output buffer overflow): 500 messages dropped since 2013-04-17T19:04:46+00:00."
            ),
            Some(("L10", Some("output buffer overflow")))
        );
        assert_eq!(parse_error_code("Error H99"), Some(("H99", None)));
        assert_eq!(parse_error_code("Error R1"), None);
        assert_eq!(parse_error_code("Error R144 (x)"), None);
        assert_eq!(parse_error_code("Error X14 (x)"), None);
        assert_eq!(parse_error_code("Error Rü4"), None);
        assert_eq!(parse_error_code("error R14"), None);
        assert_eq!(parse_error_code("Process running mem=1028MB(103.3%)"), None);
    }

    #[test]
    fn test_app_log() {
        let log = to_log_with_drain_token(
            "<190>1 2024-04-26T16:58:32.943253+00:00 host app web.1 - Completed 200 OK in 12ms (Views: 3.1ms | ActiveRecord: 1.2ms) code=H12\n",
            DRAIN_TOKEN,
        );

        insta::assert_json_snapshot!(SerializableAnnotated(&log), @r#"
        {
          "timestamp": 1714150712.943253,
          "level": "info",
          "body": "Completed 200 OK in 12ms (Views: 3.1ms | ActiveRecord: 1.2ms) code=H12",
          "attributes": {
            "cloud.provider": {
              "type": "string",
              "value": "heroku"
            },
            "heroku.dyno.name": {
              "type": "string",
              "value": "web.1"
            },
            "heroku.logplex.drain_token": {
              "type": "string",
              "value": "d.73ea7440-270a-435a-a0ea-adf50b4e5f5a"
            },
            "sentry.origin": {
              "type": "string",
              "value": "auto.log_drain.heroku"
            }
          }
        }
        "#);
    }

    #[test]
    fn test_app_log_looks_like_heroku_error() {
        // Application output is never interpreted, even if it looks like a platform message.
        let log = to_log(
            "<190>1 2024-04-26T16:58:32+00:00 host app web.1 - Error R14 (Memory quota exceeded)",
        );
        let attributes = log.value().unwrap().attributes.value().unwrap();
        assert!(!attributes.contains_key(HEROKU__ERROR__CODE));
        assert_eq!(log.value().unwrap().level.value(), Some(&OurLogLevel::Info));
    }

    #[test]
    fn test_router_info() {
        // https://devcenter.heroku.com/articles/http-routing#heroku-router-log-format
        let log = to_log(
            r#"<158>1 2024-04-26T16:58:32.943253+00:00 host heroku router - at=info method=GET path=/ host=my-app.example.com request_id=6903a168-b79b-ec27-03c8-b8f64d8d8792 fwd="138.68.186.89" dyno=web.1 connect=0ms service=0ms status=200 bytes=0 protocol=http2.0 tls=true tls_version=tls1.3"#,
        );

        insta::assert_json_snapshot!(SerializableAnnotated(&log), @r#"
        {
          "timestamp": 1714150712.943253,
          "level": "info",
          "body": "at=info method=GET path=/ host=my-app.example.com request_id=6903a168-b79b-ec27-03c8-b8f64d8d8792 fwd=\"138.68.186.89\" dyno=web.1 connect=0ms service=0ms status=200 bytes=0 protocol=http2.0 tls=true tls_version=tls1.3",
          "attributes": {
            "client.address": {
              "type": "string",
              "value": "138.68.186.89"
            },
            "cloud.provider": {
              "type": "string",
              "value": "heroku"
            },
            "heroku.dyno.name": {
              "type": "string",
              "value": "web.1"
            },
            "heroku.router.connect": {
              "type": "double",
              "value": 0.0
            },
            "heroku.router.request_id": {
              "type": "string",
              "value": "6903a168-b79b-ec27-03c8-b8f64d8d8792"
            },
            "heroku.router.service": {
              "type": "double",
              "value": 0.0
            },
            "http.request.method": {
              "type": "string",
              "value": "GET"
            },
            "http.response.status_code": {
              "type": "integer",
              "value": 200
            },
            "network.protocol.name": {
              "type": "string",
              "value": "http"
            },
            "network.protocol.version": {
              "type": "string",
              "value": "2"
            },
            "sentry.origin": {
              "type": "string",
              "value": "auto.log_drain.heroku"
            },
            "server.address": {
              "type": "string",
              "value": "my-app.example.com"
            },
            "tls.protocol.name": {
              "type": "string",
              "value": "tls"
            },
            "tls.protocol.version": {
              "type": "string",
              "value": "1.3"
            },
            "url.path": {
              "type": "string",
              "value": "/"
            },
            "url.scheme": {
              "type": "string",
              "value": "https"
            }
          }
        }
        "#);
    }

    #[test]
    fn test_router_h12() {
        // https://devcenter.heroku.com/articles/error-codes#h12-request-timeout
        let log = to_log(
            r#"<158>1 2010-10-06T21:51:37-07:00 host heroku router - at=error code=H12 desc="Request timeout" method=GET path="/" host=example-app-1234567890ab.herokuapp.com fwd=17.17.17.17 dyno=web.1 connect=6ms service=30001ms status=503 bytes=0"#,
        );

        insta::assert_json_snapshot!(SerializableAnnotated(&log), @r#"
        {
          "timestamp": 1286427097.0,
          "level": "error",
          "body": "at=error code=H12 desc=\"Request timeout\" method=GET path=\"/\" host=example-app-1234567890ab.herokuapp.com fwd=17.17.17.17 dyno=web.1 connect=6ms service=30001ms status=503 bytes=0",
          "attributes": {
            "client.address": {
              "type": "string",
              "value": "17.17.17.17"
            },
            "cloud.provider": {
              "type": "string",
              "value": "heroku"
            },
            "heroku.dyno.name": {
              "type": "string",
              "value": "web.1"
            },
            "heroku.error.code": {
              "type": "string",
              "value": "H12"
            },
            "heroku.error.description": {
              "type": "string",
              "value": "Request timeout"
            },
            "heroku.router.connect": {
              "type": "double",
              "value": 6.0
            },
            "heroku.router.service": {
              "type": "double",
              "value": 30001.0
            },
            "http.request.method": {
              "type": "string",
              "value": "GET"
            },
            "http.response.status_code": {
              "type": "integer",
              "value": 503
            },
            "sentry.origin": {
              "type": "string",
              "value": "auto.log_drain.heroku"
            },
            "server.address": {
              "type": "string",
              "value": "example-app-1234567890ab.herokuapp.com"
            },
            "url.path": {
              "type": "string",
              "value": "/"
            }
          }
        }
        "#);
    }

    #[test]
    fn test_router_h10_empty_values() {
        // https://devcenter.heroku.com/articles/error-codes#h10-app-crashed
        let log = to_log(
            r#"<158>1 2010-10-06T21:51:12-07:00 host heroku router - at=error code=H10 desc="App crashed" method=GET path="/" host=example-app-1234567890ab.herokuapp.com fwd=17.17.17.17 dyno= connect= service= status=503 bytes="#,
        );

        let log = log.value().unwrap();
        assert_eq!(log.level.value(), Some(&OurLogLevel::Error));

        let attributes = log.attributes.value().unwrap();
        assert_eq!(
            attributes
                .get_value(HEROKU__ERROR__CODE)
                .and_then(|v| v.as_str()),
            Some("H10")
        );
        assert!(!attributes.contains_key(HEROKU__DYNO__NAME));
        assert!(!attributes.contains_key(HEROKU__ROUTER__CONNECT));
        assert!(!attributes.contains_key(HTTP__RESPONSE__SIZE));
    }

    #[test]
    fn test_router_ignores_unknown_and_malformed_fields() {
        let log = to_log(
            "<158>1 2024-04-26T16:58:32+00:00 host heroku router - at=info status=abc connect=fast tls=maybe protocol=spdy tls_version=unknown sample#foo=1 code=",
        );

        let attributes = log.value().unwrap().attributes.value().unwrap();
        let keys: Vec<_> = attributes.0.keys().collect();
        assert_eq!(keys, vec!["cloud.provider", "sentry.origin"]);
    }

    fn attribute<'a>(log: &'a Annotated<OurLog>, key: &str) -> Option<&'a Value> {
        log.value()?.attributes.value()?.get_value(key)
    }

    #[test]
    fn test_dyno_identity() {
        for (source, process, expected) in [
            ("app", "web.1", Some("web.1")),
            ("app", "run.1234", Some("run.1234")),
            ("heroku", "worker.2", Some("worker.2")),
            ("heroku", "router", None),
            ("heroku", "logplex", None),
            ("heroku", "api", None),
            ("app", "-", None),
        ] {
            let log = to_log(&format!(
                "<190>1 2024-04-26T16:58:32+00:00 host {source} {process} - hello"
            ));
            assert_eq!(
                attribute(&log, HEROKU__DYNO__NAME).and_then(|value| value.as_str()),
                expected,
                "{source} {process}"
            );
            for key in [
                "heroku.source",
                "heroku.dyno",
                "heroku.router.dyno",
                SERVICE__NAME,
            ] {
                assert_eq!(attribute(&log, key), None, "{key}");
            }
        }
    }

    #[test]
    fn test_router_timings_are_finite_doubles() {
        for key in ["connect", "service"] {
            let name = router_field(key).unwrap().0;
            for (value, expected) in [
                ("0ms", Some(0.0)),
                ("1ms", Some(1.0)),
                ("0.125ms", Some(0.125)),
                ("30001.5ms", Some(30001.5)),
                ("1.5", Some(1.5)),
                ("", None),
                ("fast", None),
                ("NaNms", None),
                ("infms", None),
                ("-infms", None),
                ("1e309ms", None),
            ] {
                let log = to_log(&format!(
                    "<158>1 2024-04-26T16:58:32+00:00 host heroku router - at=info path=/ {key}=1ms {key}={value}"
                ));
                assert_eq!(
                    attribute(&log, name),
                    expected.map(Value::F64).as_ref(),
                    "{key}={value}"
                );
            }
        }
    }

    #[test]
    fn test_router_host_and_port() {
        for (host, address, port) in [
            ("example.com", Some("example.com"), None),
            ("example.com:443", Some("example.com"), Some(443)),
            ("example.com:80", Some("example.com"), Some(80)),
            ("127.0.0.1:8080", Some("127.0.0.1"), Some(8080)),
            ("[2001:db8::1]", Some("2001:db8::1"), None),
            ("[2001:db8::1]:8443", Some("2001:db8::1"), Some(8443)),
            ("2001:db8::1", Some("2001:db8::1"), None),
            ("", None, None),
            (":80", None, None),
            ("example.com:", None, None),
            ("example.com:invalid", None, None),
            ("example.com:65536", None, None),
            ("example.com:+80", None, None),
            ("[2001:db8::1", None, None),
            ("[invalid]:80", None, None),
            ("[2001:db8::1]suffix", None, None),
        ] {
            let log = to_log(&format!(
                "<158>1 2024-04-26T16:58:32+00:00 host heroku router - at=info path=/ host=injected.example:123 host={host}"
            ));
            assert_eq!(
                attribute(&log, SERVER__ADDRESS).and_then(|value| value.as_str()),
                address,
                "{host}"
            );
            assert_eq!(
                attribute(&log, SERVER__PORT),
                port.map(Value::I64).as_ref(),
                "{host}"
            );
        }
    }

    #[test]
    fn test_router_methods() {
        for method in [
            "CONNECT", "DELETE", "GET", "HEAD", "OPTIONS", "PATCH", "POST", "PUT", "QUERY",
            "TRACE", "get", "CUSTOM", "_OTHER", "",
        ] {
            let expected = match method {
                "" => None,
                "get" | "CUSTOM" | "_OTHER" => Some("_OTHER"),
                _ => Some(method),
            };
            let log = to_log(&format!(
                "<158>1 2024-04-26T16:58:32+00:00 host heroku router - at=info method={method} path=/"
            ));
            assert_eq!(
                attribute(&log, HTTP__REQUEST__METHOD).and_then(|value| value.as_str()),
                expected,
                "{method}"
            );
        }
    }

    #[test]
    fn test_router_level_overrides_syslog() {
        for (level, expected) in [
            ("trace", OurLogLevel::Trace),
            ("debug", OurLogLevel::Debug),
            ("info", OurLogLevel::Info),
            ("warning", OurLogLevel::Warn),
            ("warn", OurLogLevel::Warn),
            ("error", OurLogLevel::Error),
            ("fatal", OurLogLevel::Fatal),
            ("unknown", OurLogLevel::Error),
            ("", OurLogLevel::Error),
        ] {
            let log = to_log(&format!(
                "<155>1 2024-04-26T16:58:32+00:00 host heroku router - at={level} path=/"
            ));
            assert_eq!(
                log.value().unwrap().level.value(),
                Some(&expected),
                "{level}"
            );
        }
    }

    #[test]
    fn test_router_error_code() {
        for (fields, expected) in [
            ("code=H12", Some("H12")),
            ("code=H12 code=H10", Some("H10")),
            ("code=H12 code=", None),
            ("code=", None),
        ] {
            let log = to_log(&format!(
                "<158>1 2024-04-26T16:58:32+00:00 host heroku router - at=error {fields} path=/ code=H99"
            ));
            assert_eq!(
                attribute(&log, HEROKU__ERROR__CODE).and_then(|value| value.as_str()),
                expected,
                "{fields}"
            );
            assert_eq!(attribute(&log, ERROR__TYPE), None);
        }
    }

    #[test]
    fn test_router_response_size_requires_runtime() {
        for protocol in ["http", "https", "http1.1", "http2.0"] {
            let log = to_log(&format!(
                "<158>1 2024-04-26T16:58:32+00:00 host heroku router - at=info path=/ bytes=123 protocol={protocol}"
            ));
            assert_eq!(attribute(&log, HTTP__RESPONSE__SIZE), None);
            assert_eq!(attribute(&log, HTTP__RESPONSE__BODY__SIZE), None);
            assert!(
                log.value()
                    .unwrap()
                    .body
                    .value()
                    .unwrap()
                    .contains("bytes=123")
            );
        }
    }

    #[test]
    fn test_router_escaped_path_cannot_spoof_fields() {
        let log = to_log(
            r#"<158>1 2024-04-26T16:58:32+00:00 host heroku router - at=info method=GET path="/x\" at=error code=H99 desc=\"pwned\" status=500" host=a.example.com request_id=r1 fwd="1.2.3.4" dyno=web.1 connect=0ms service=5ms status=200 bytes=10 protocol=http1.1"#,
        );

        assert_eq!(log.value().unwrap().level.value(), Some(&OurLogLevel::Info));
        assert_eq!(attribute(&log, HEROKU__ERROR__CODE), None);
        assert_eq!(
            attribute(&log, URL__PATH).and_then(|v| v.as_str()),
            Some(r#"/x" at=error code=H99 desc="pwned" status=500"#)
        );
        assert_eq!(
            attribute(&log, HTTP__RESPONSE__STATUS_CODE),
            Some(&Value::I64(200))
        );
    }

    #[test]
    fn test_router_unescaped_path_cannot_spoof_fields() {
        // If Heroku does not escape quotes, fake pairs end up as separate fields.
        let log = to_log(
            r#"<158>1 2024-04-26T16:58:32+00:00 host heroku router - at=info method=GET path="/x" at=error code=H99 desc="pwned" method=POST status=500 host=evil path="/evil" fwd="6.6.6.6" dyno=evil" host=a.example.com request_id=r1 fwd="1.2.3.4" dyno=web.1 connect=0ms service=5ms status=200 bytes=10 protocol=http1.1"#,
        );

        assert_eq!(log.value().unwrap().level.value(), Some(&OurLogLevel::Info));
        assert_eq!(attribute(&log, HEROKU__ERROR__CODE), None);
        assert_eq!(attribute(&log, HEROKU__ERROR__DESCRIPTION), None);
        let str_attr = |key| attribute(&log, key).and_then(|v| v.as_str());
        assert_eq!(str_attr(HTTP__REQUEST__METHOD), Some("GET"));
        assert_eq!(str_attr(URL__PATH), Some("/x"));
        assert_eq!(str_attr(SERVER__ADDRESS), Some("a.example.com"));
        assert_eq!(str_attr(CLIENT__ADDRESS), Some("1.2.3.4"));
        assert_eq!(str_attr(HEROKU__DYNO__NAME), Some("web.1"));
        assert_eq!(
            attribute(&log, HTTP__RESPONSE__STATUS_CODE),
            Some(&Value::I64(200))
        );
    }

    #[test]
    fn test_router_unescaped_fwd_cannot_spoof_fields() {
        // H10 lines log empty values, which must still replace values injected through `fwd`.
        let log = to_log(
            r#"<158>1 2024-04-26T16:58:32+00:00 host heroku router - at=error code=H10 desc="App crashed" method=GET path="/" host=a.example.com fwd="1.2.3.4" dyno=evil status=200 code=H99 path=/evil" dyno= connect= service= status=503 bytes="#,
        );

        assert_eq!(
            log.value().unwrap().level.value(),
            Some(&OurLogLevel::Error)
        );
        let str_attr = |key| attribute(&log, key).and_then(|v| v.as_str());
        assert_eq!(str_attr(HEROKU__ERROR__CODE), Some("H10"));
        assert_eq!(str_attr(URL__PATH), Some("/"));
        assert_eq!(str_attr(CLIENT__ADDRESS), Some("1.2.3.4"));
        assert_eq!(attribute(&log, HEROKU__DYNO__NAME), None);
        assert_eq!(
            attribute(&log, HTTP__RESPONSE__STATUS_CODE),
            Some(&Value::I64(503))
        );
    }

    #[test]
    fn test_router_fwd_with_multiple_addresses() {
        let log = to_log(
            r#"<158>1 2024-04-26T16:58:32+00:00 host heroku router - at=info method=GET path="/" host=a.example.com fwd="203.0.113.7, 10.0.0.1" dyno=web.1 status=200"#,
        );

        assert_eq!(
            attribute(&log, CLIENT__ADDRESS).and_then(|v| v.as_str()),
            Some("10.0.0.1")
        );
        assert_eq!(
            attribute(&log, HTTP__RESPONSE__STATUS_CODE),
            Some(&Value::I64(200))
        );
    }

    #[test]
    fn test_router_fwd_not_an_ip_address() {
        let client_address = |fwd: &str| {
            let log = to_log(&format!(
                r#"<158>1 2024-04-26T16:58:32+00:00 host heroku router - at=info path="/" fwd="{fwd}" status=200"#
            ));
            attribute(&log, CLIENT__ADDRESS)
                .and_then(|v| v.as_str())
                .map(str::to_owned)
        };

        assert_eq!(
            client_address("1.2.3.4, 2001:db8::1").as_deref(),
            Some("2001:db8::1")
        );
        // `{{auto}}` would be replaced with the address of the Logplex host during normalization.
        assert_eq!(client_address("1.2.3.4, {{auto}}"), None);
        assert_eq!(client_address("1.2.3.4, not an ip"), None);
        assert_eq!(client_address("1.2.3.4,"), None);
        assert_eq!(
            client_address("{{auto}}, not an ip, 1.2.3.4").as_deref(),
            Some("1.2.3.4")
        );
    }

    #[test]
    fn test_router_protocol_and_tls() {
        let log_with = |fields: &str| {
            to_log(&format!(
                r#"<158>1 2024-04-26T16:58:32+00:00 host heroku router - at=info path="/" status=200 {fields}"#
            ))
        };
        let str_attribute = |fields: &str, key: &str| {
            attribute(&log_with(fields), key)
                .and_then(|v| v.as_str())
                .map(str::to_owned)
        };
        let version = |protocol: &str| {
            str_attribute(&format!("protocol={protocol}"), NETWORK__PROTOCOL__VERSION)
        };

        assert_eq!(version("http1.1").as_deref(), Some("1.1"));
        assert_eq!(version("http2.0").as_deref(), Some("2"));
        // Older router logs have the scheme in this field.
        assert_eq!(version("http"), None);
        assert_eq!(version("https"), None);
        for protocol in ["http1.0", "http1.1", "http2.0", "http", "https"] {
            assert_eq!(
                str_attribute(&format!("protocol={protocol}"), NETWORK__PROTOCOL__NAME).as_deref(),
                Some("http")
            );
        }

        assert_eq!(
            str_attribute("tls=true", URL__SCHEME).as_deref(),
            Some("https")
        );
        assert_eq!(
            str_attribute("tls=false", URL__SCHEME).as_deref(),
            Some("http")
        );

        let tls_version =
            |value: &str| str_attribute(&format!("tls_version={value}"), TLS__PROTOCOL__VERSION);
        assert_eq!(tls_version("tls1.2").as_deref(), Some("1.2"));
        assert_eq!(tls_version("unknown"), None);
        assert_eq!(tls_version("tlsfoo"), None);
        for version in ["1.0", "1.1", "1.2", "1.3"] {
            let fields = format!("tls_version=tls{version}");
            assert_eq!(
                str_attribute(&fields, TLS__PROTOCOL__NAME).as_deref(),
                Some("tls")
            );
            assert_eq!(
                str_attribute(&fields, TLS__PROTOCOL__VERSION).as_deref(),
                Some(version)
            );
        }
        for value in [
            "",
            "unknown",
            "tls1",
            "tls1garbage",
            "tls1.3junk",
            "tls1.3.0",
            "tls2.0",
        ] {
            let fields = format!("tls_version={value}");
            assert_eq!(str_attribute(&fields, TLS__PROTOCOL__NAME), None, "{value}");
            assert_eq!(
                str_attribute(&fields, TLS__PROTOCOL__VERSION),
                None,
                "{value}"
            );
        }
    }

    #[test]
    fn test_router_empty_protocols_clear_companions() {
        let log = to_log(
            "<158>1 2024-04-26T16:58:32+00:00 host heroku router - at=info path=/ protocol=http1.1 tls_version=tls1.3 protocol= tls_version=",
        );
        for key in [
            NETWORK__PROTOCOL__NAME,
            NETWORK__PROTOCOL__VERSION,
            TLS__PROTOCOL__NAME,
            TLS__PROTOCOL__VERSION,
        ] {
            assert_eq!(attribute(&log, key), None, "{key}");
        }
    }

    #[test]
    fn test_router_path_and_query() {
        let router_log = |path: &str| {
            to_log(&format!(
                r#"<158>1 2024-04-26T16:58:32+00:00 host heroku router - at=info method=GET path="{path}" status=200"#
            ))
        };
        let parts = |path: &str| {
            let log = router_log(path);
            (
                attribute(&log, URL__PATH)
                    .and_then(|v| v.as_str())
                    .map(str::to_owned),
                attribute(&log, URL__QUERY)
                    .and_then(|v| v.as_str())
                    .map(str::to_owned),
            )
        };
        let some = |value: &str| Some(value.to_owned());

        assert_eq!(parts("/forecast"), (some("/forecast"), None));
        assert_eq!(parts("/forecast?"), (some("/forecast"), None));
        assert_eq!(
            parts("/forecast?x=1&y=?"),
            (some("/forecast"), some("x=1&y=?"))
        );
        assert_eq!(parts("?x=1"), (None, some("x=1")));
    }

    #[test]
    fn test_router_injected_query_cannot_be_kept() {
        // Only the first `path` counts, a later one does not add or replace the query.
        let log = to_log(
            r#"<158>1 2024-04-26T16:58:32+00:00 host heroku router - at=info method=GET path="/x" path="/evil?a=b" status=200"#,
        );

        assert_eq!(
            attribute(&log, URL__PATH).and_then(|v| v.as_str()),
            Some("/x")
        );
        assert_eq!(attribute(&log, URL__QUERY), None);
    }

    #[test]
    fn test_router_unconvertible_values_replace_injected_ones() {
        // The genuine values cannot be converted, but must still replace injected ones.
        let log = to_log(
            r#"<158>1 2024-04-26T16:58:32+00:00 host heroku router - at=info method=GET path="/" fwd="1.2.3.4" protocol=http1.1 tls=true tls_version=tls1.3 status=200 fwd="" protocol=spdy tls=maybe tls_version=unknown"#,
        );

        assert_eq!(attribute(&log, CLIENT__ADDRESS), None);
        assert_eq!(attribute(&log, NETWORK__PROTOCOL__VERSION), None);
        assert_eq!(attribute(&log, NETWORK__PROTOCOL__NAME), None);
        assert_eq!(attribute(&log, URL__SCHEME), None);
        assert_eq!(attribute(&log, TLS__PROTOCOL__VERSION), None);
        assert_eq!(attribute(&log, TLS__PROTOCOL__NAME), None);
    }

    #[test]
    fn test_parse_message_crlf() {
        let msg = parse_logplex_message("<190>1 - host app web.1 - hello\r\n").unwrap();
        assert_eq!(msg.msg, "hello");
    }

    #[test]
    fn test_dyno_r14() {
        // https://devcenter.heroku.com/articles/error-codes#r14-memory-quota-exceeded
        let log = to_log(
            "<45>1 2011-05-03T17:40:11+00:00 host heroku worker.1 - Error R14 (Memory quota exceeded)\n",
        );

        insta::assert_json_snapshot!(SerializableAnnotated(&log), @r#"
        {
          "timestamp": 1304444411.0,
          "level": "error",
          "body": "Error R14 (Memory quota exceeded)",
          "attributes": {
            "cloud.provider": {
              "type": "string",
              "value": "heroku"
            },
            "heroku.dyno.name": {
              "type": "string",
              "value": "worker.1"
            },
            "heroku.error.code": {
              "type": "string",
              "value": "R14"
            },
            "heroku.error.description": {
              "type": "string",
              "value": "Memory quota exceeded"
            },
            "sentry.origin": {
              "type": "string",
              "value": "auto.log_drain.heroku"
            }
          }
        }
        "#);
    }

    #[test]
    fn test_dyno_without_error_code() {
        let log = to_log(
            "<45>1 2011-05-03T17:40:10+00:00 host heroku worker.1 - Process running mem=1028MB(103.3%)",
        );

        let log = log.value().unwrap();
        assert_eq!(log.level.value(), Some(&OurLogLevel::Info));
        let attributes = log.attributes.value().unwrap();
        assert!(!attributes.contains_key(HEROKU__ERROR__CODE));
    }

    #[test]
    fn test_logplex_l10() {
        let log = to_log(
            "<172>1 2013-04-17T19:04:46+00:00 host heroku logplex - Error L10 (output buffer overflow): 500 messages dropped since 2013-04-17T19:04:46+00:00.",
        );

        let log = log.value().unwrap();
        assert_eq!(log.level.value(), Some(&OurLogLevel::Error));
        let attributes = log.attributes.value().unwrap();
        assert_eq!(
            attributes
                .get_value(HEROKU__ERROR__CODE)
                .and_then(|v| v.as_str()),
            Some("L10")
        );
        assert!(!attributes.contains_key(ERROR__TYPE));
    }

    /// Runs the full pipeline over arbitrary bytes, which must never panic.
    fn parse_all(payload: &[u8]) {
        for frame in parse_logplex_frames(payload).flatten() {
            let frame = String::from_utf8_lossy(frame);
            if let Ok(message) = parse_logplex_message(&frame) {
                logplex_message_to_sentry_log(message, DRAIN_TOKEN);
            }
        }
    }

    #[test]
    fn test_no_panic_on_malformed_input() {
        let frame = |msg: &str| format!("{} {msg}", msg.len());
        let payload = [
            frame("<190>1 2024-04-26T16:58:32+00:00 host app web.1 - h\u{e9}llo w\u{f6}rld \u{1f600}\n"),
            frame(r#"<158>1 2024-04-26T16:58:32+00:00 host heroku router - at=error code=H12 desc="Request timeout" path="/\u{fc}" fwd="1.2.3.4" connect=1ms service=30001ms status=503 bytes=0 tls=true"#),
            frame("<45>1 2024-04-26T16:58:32+00:00 host heroku web.1 - Error R14 (Memory quota exceeded)\n"),
        ]
        .concat()
        .into_bytes();

        // Every truncation of a valid payload.
        for end in 0..=payload.len() {
            parse_all(&payload[..end]);
            parse_all(&payload[end..]);
        }

        // Deterministic pseudo-random byte mutations, biased towards syntax characters.
        let mut state: u64 = 0x9e37_79b9_7f4a_7c15;
        let mut next = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        let alphabet = b" <>=\"()-:0123456789\nHLRE\xc3\xbc\xff";
        for _ in 0..20_000 {
            let mut mutated = payload.clone();
            for _ in 0..(next() % 8 + 1) {
                let pos = (next() % mutated.len() as u64) as usize;
                let byte = alphabet[(next() % alphabet.len() as u64) as usize];
                match next() % 3 {
                    0 => mutated[pos] = byte,
                    1 => mutated.insert(pos, byte),
                    _ => drop(mutated.remove(pos)),
                }
            }
            parse_all(&mutated);
        }
    }

    #[test]
    fn test_invalid_timestamp_is_substituted() {
        let log = to_log("<190>1 - host app web.1 - hello");
        let log = log.value().unwrap();
        assert!(log.timestamp.value().is_some());
        assert!(!log.timestamp.meta().is_empty());
    }
}
