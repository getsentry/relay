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

use chrono::{DateTime, Utc};
use relay_conventions::attributes::SENTRY__ORIGIN;
use relay_event_schema::protocol::{Attributes, OurLog, OurLogLevel, Timestamp, TraceId};
use relay_protocol::{Annotated, Meta, Remark, RemarkType};

/// Attribute names for Heroku logs.
///
/// TODO: Replace with the generated constants from `relay_conventions` once the attributes are
/// defined in `sentry-conventions`.
mod attributes {
    pub const HEROKU__SOURCE: &str = "heroku.source";
    pub const HEROKU__DYNO: &str = "heroku.dyno";
    pub const HEROKU__ERROR__CODE: &str = "heroku.error.code";
    pub const HEROKU__ERROR__DESCRIPTION: &str = "heroku.error.description";
    pub const HEROKU__LOGPLEX__FRAME_ID: &str = "heroku.logplex.frame_id";
    pub const HEROKU__LOGPLEX__DRAIN_TOKEN: &str = "heroku.logplex.drain_token";
    pub const HEROKU__LOGPLEX__USER_AGENT: &str = "heroku.logplex.user_agent";
    pub const HEROKU__ROUTER__AT: &str = "heroku.router.at";
    pub const HEROKU__ROUTER__METHOD: &str = "heroku.router.method";
    pub const HEROKU__ROUTER__PATH: &str = "heroku.router.path";
    pub const HEROKU__ROUTER__HOST: &str = "heroku.router.host";
    pub const HEROKU__ROUTER__REQUEST_ID: &str = "heroku.router.request_id";
    pub const HEROKU__ROUTER__FWD: &str = "heroku.router.fwd";
    pub const HEROKU__ROUTER__DYNO: &str = "heroku.router.dyno";
    pub const HEROKU__ROUTER__CONNECT: &str = "heroku.router.connect";
    pub const HEROKU__ROUTER__SERVICE: &str = "heroku.router.service";
    pub const HEROKU__ROUTER__STATUS: &str = "heroku.router.status";
    pub const HEROKU__ROUTER__BYTES: &str = "heroku.router.bytes";
    pub const HEROKU__ROUTER__PROTOCOL: &str = "heroku.router.protocol";
    pub const HEROKU__ROUTER__TLS: &str = "heroku.router.tls";
    pub const HEROKU__ROUTER__TLS_VERSION: &str = "heroku.router.tls_version";
}

use self::attributes::*;

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

/// Request metadata Logplex sends as HTTP headers with every drain request.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LogplexHeaders<'a> {
    /// The unique identifier of the request, `Logplex-Frame-Id`.
    ///
    /// It stays the same when Logplex retries a request.
    pub frame_id: Option<&'a str>,
    /// The token of the drain sending the request, `Logplex-Drain-Token`.
    pub drain_token: Option<&'a str>,
    /// The version of Logplex sending the request, `User-Agent`.
    pub user_agent: Option<&'a str>,
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
/// Values may be wrapped in double quotes to contain spaces, escape sequences are not supported.
/// Words without a `=` are skipped.
struct Logfmt<'a>(&'a str);

impl<'a> Iterator for Logfmt<'a> {
    type Item = (&'a str, &'a str);

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
                Some(quoted) => match quoted.split_once('"') {
                    Some((value, rest)) => (value, rest),
                    None => (quoted, ""),
                },
                None => rest.split_at(rest.find(' ').unwrap_or(rest.len())),
            };
            self.0 = rest;

            if !key.is_empty() {
                return Some((key, value));
            }
        }
    }
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

/// Adds the attributes of a Heroku router message and returns the log level it reports.
///
/// Only the documented [router fields](https://devcenter.heroku.com/articles/http-routing#heroku-router-log-format)
/// are extracted.
fn add_router_attributes(attributes: &mut Attributes, msg: &str) -> Option<OurLogLevel> {
    let mut level = None;

    for (key, value) in Logfmt(msg) {
        if value.is_empty() {
            continue;
        }

        match key {
            "at" => {
                level = match value {
                    "error" => Some(OurLogLevel::Error),
                    "warning" => Some(OurLogLevel::Warn),
                    _ => None,
                };
                attributes.insert(HEROKU__ROUTER__AT, value.to_owned());
            }
            "code" => attributes.insert(HEROKU__ERROR__CODE, value.to_owned()),
            "desc" => attributes.insert(HEROKU__ERROR__DESCRIPTION, value.to_owned()),
            "method" => attributes.insert(HEROKU__ROUTER__METHOD, value.to_owned()),
            "path" => attributes.insert(HEROKU__ROUTER__PATH, value.to_owned()),
            "host" => attributes.insert(HEROKU__ROUTER__HOST, value.to_owned()),
            "request_id" => attributes.insert(HEROKU__ROUTER__REQUEST_ID, value.to_owned()),
            "fwd" => attributes.insert(HEROKU__ROUTER__FWD, value.to_owned()),
            "dyno" => attributes.insert(HEROKU__ROUTER__DYNO, value.to_owned()),
            "protocol" => attributes.insert(HEROKU__ROUTER__PROTOCOL, value.to_owned()),
            "tls_version" => attributes.insert(HEROKU__ROUTER__TLS_VERSION, value.to_owned()),
            "connect" | "service" => {
                let key = match key {
                    "connect" => HEROKU__ROUTER__CONNECT,
                    _ => HEROKU__ROUTER__SERVICE,
                };
                let millis = value.strip_suffix("ms").unwrap_or(value);
                if let Ok(millis) = millis.parse::<i64>() {
                    attributes.insert(key, millis);
                }
            }
            "status" | "bytes" => {
                let key = match key {
                    "status" => HEROKU__ROUTER__STATUS,
                    _ => HEROKU__ROUTER__BYTES,
                };
                if let Ok(value) = value.parse::<i64>() {
                    attributes.insert(key, value);
                }
            }
            "tls" => {
                if let Ok(value) = value.parse::<bool>() {
                    attributes.insert(HEROKU__ROUTER__TLS, value);
                }
            }
            _ => {}
        }
    }

    level
}

/// Converts a parsed Logplex message to a Sentry log.
pub fn logplex_message_to_sentry_log(
    message: LogplexMessage<'_>,
    headers: LogplexHeaders<'_>,
) -> OurLog {
    let LogplexMessage {
        severity,
        timestamp,
        source,
        dyno,
        msg,
    } = message;

    let LogplexHeaders {
        frame_id,
        drain_token,
        user_agent,
    } = headers;

    let mut attributes = Attributes::default();

    macro_rules! add_optional_attribute {
        ($name:expr, $value:expr) => {{
            if let Some(value) = $value {
                attributes.insert($name, value.to_owned());
            }
        }};
    }

    attributes.insert(SENTRY__ORIGIN, "auto.log_drain.heroku".to_owned());
    add_optional_attribute!(HEROKU__SOURCE, source);
    add_optional_attribute!(HEROKU__DYNO, dyno);
    add_optional_attribute!(HEROKU__LOGPLEX__FRAME_ID, frame_id);
    add_optional_attribute!(HEROKU__LOGPLEX__DRAIN_TOKEN, drain_token);
    add_optional_attribute!(HEROKU__LOGPLEX__USER_AGENT, user_agent);

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
    use relay_protocol::SerializableAnnotated;

    use super::*;

    const HEADERS: LogplexHeaders<'static> = LogplexHeaders {
        frame_id: Some("09C557EAFCFB6CF2740EE62F62971098"),
        drain_token: Some("d.73ea7440-270a-435a-a0ea-adf50b4e5f5a"),
        user_agent: Some("Logplex/v72"),
    };

    fn frames(payload: &str) -> Vec<Result<&str, LogplexError>> {
        parse_logplex_frames(payload.as_bytes())
            .map(|frame| frame.map(|frame| std::str::from_utf8(frame).unwrap()))
            .collect()
    }

    /// Converts a message to a log, removing the random trace id to keep snapshots stable.
    fn to_log_with_headers(line: &str, headers: LogplexHeaders<'_>) -> Annotated<OurLog> {
        let message = parse_logplex_message(line).unwrap();
        let mut log = logplex_message_to_sentry_log(message, headers);

        assert!(log.trace_id.value().is_some());
        assert!(!log.trace_id.meta().is_empty());
        log.trace_id = Annotated::empty();

        Annotated::new(log)
    }

    fn to_log(line: &str) -> Annotated<OurLog> {
        to_log_with_headers(line, LogplexHeaders::default())
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

    #[test]
    fn test_logfmt() {
        let pairs: Vec<_> =
            Logfmt(r#"at=error  desc="Request timeout" word path="/a b" =x dyno= fwd="1.2.3.4"#)
                .collect();
        assert_eq!(
            pairs,
            vec![
                ("at", "error"),
                ("desc", "Request timeout"),
                ("path", "/a b"),
                ("dyno", ""),
                ("fwd", "1.2.3.4"),
            ]
        );

        assert_eq!(Logfmt("").count(), 0);
        assert_eq!(Logfmt("   ").count(), 0);
        assert_eq!(Logfmt("=").count(), 0);
        assert_eq!(Logfmt("a=\"").collect::<Vec<_>>(), vec![("a", "")]);
        assert_eq!(Logfmt("ü=ö ä").collect::<Vec<_>>(), vec![("ü", "ö")]);
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
        let log = to_log_with_headers(
            "<190>1 2024-04-26T16:58:32.943253+00:00 host app web.1 - Completed 200 OK in 12ms (Views: 3.1ms | ActiveRecord: 1.2ms) code=H12\n",
            HEADERS,
        );

        insta::assert_json_snapshot!(SerializableAnnotated(&log), @r#"
        {
          "timestamp": 1714150712.943253,
          "level": "info",
          "body": "Completed 200 OK in 12ms (Views: 3.1ms | ActiveRecord: 1.2ms) code=H12",
          "attributes": {
            "heroku.dyno": {
              "type": "string",
              "value": "web.1"
            },
            "heroku.logplex.drain_token": {
              "type": "string",
              "value": "d.73ea7440-270a-435a-a0ea-adf50b4e5f5a"
            },
            "heroku.logplex.frame_id": {
              "type": "string",
              "value": "09C557EAFCFB6CF2740EE62F62971098"
            },
            "heroku.logplex.user_agent": {
              "type": "string",
              "value": "Logplex/v72"
            },
            "heroku.source": {
              "type": "string",
              "value": "app"
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
            "heroku.dyno": {
              "type": "string",
              "value": "router"
            },
            "heroku.router.at": {
              "type": "string",
              "value": "info"
            },
            "heroku.router.bytes": {
              "type": "integer",
              "value": 0
            },
            "heroku.router.connect": {
              "type": "integer",
              "value": 0
            },
            "heroku.router.dyno": {
              "type": "string",
              "value": "web.1"
            },
            "heroku.router.fwd": {
              "type": "string",
              "value": "138.68.186.89"
            },
            "heroku.router.host": {
              "type": "string",
              "value": "my-app.example.com"
            },
            "heroku.router.method": {
              "type": "string",
              "value": "GET"
            },
            "heroku.router.path": {
              "type": "string",
              "value": "/"
            },
            "heroku.router.protocol": {
              "type": "string",
              "value": "http2.0"
            },
            "heroku.router.request_id": {
              "type": "string",
              "value": "6903a168-b79b-ec27-03c8-b8f64d8d8792"
            },
            "heroku.router.service": {
              "type": "integer",
              "value": 0
            },
            "heroku.router.status": {
              "type": "integer",
              "value": 200
            },
            "heroku.router.tls": {
              "type": "boolean",
              "value": true
            },
            "heroku.router.tls_version": {
              "type": "string",
              "value": "tls1.3"
            },
            "heroku.source": {
              "type": "string",
              "value": "heroku"
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
            "heroku.dyno": {
              "type": "string",
              "value": "router"
            },
            "heroku.error.code": {
              "type": "string",
              "value": "H12"
            },
            "heroku.error.description": {
              "type": "string",
              "value": "Request timeout"
            },
            "heroku.router.at": {
              "type": "string",
              "value": "error"
            },
            "heroku.router.bytes": {
              "type": "integer",
              "value": 0
            },
            "heroku.router.connect": {
              "type": "integer",
              "value": 6
            },
            "heroku.router.dyno": {
              "type": "string",
              "value": "web.1"
            },
            "heroku.router.fwd": {
              "type": "string",
              "value": "17.17.17.17"
            },
            "heroku.router.host": {
              "type": "string",
              "value": "example-app-1234567890ab.herokuapp.com"
            },
            "heroku.router.method": {
              "type": "string",
              "value": "GET"
            },
            "heroku.router.path": {
              "type": "string",
              "value": "/"
            },
            "heroku.router.service": {
              "type": "integer",
              "value": 30001
            },
            "heroku.router.status": {
              "type": "integer",
              "value": 503
            },
            "heroku.source": {
              "type": "string",
              "value": "heroku"
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
        assert!(!attributes.contains_key(HEROKU__ROUTER__DYNO));
        assert!(!attributes.contains_key(HEROKU__ROUTER__CONNECT));
        assert!(!attributes.contains_key(HEROKU__ROUTER__BYTES));
    }

    #[test]
    fn test_router_ignores_unknown_and_malformed_fields() {
        let log = to_log(
            "<158>1 2024-04-26T16:58:32+00:00 host heroku router - at=info status=abc connect=fast tls=maybe sample#foo=1 code=",
        );

        let attributes = log.value().unwrap().attributes.value().unwrap();
        let keys: Vec<_> = attributes.0.keys().collect();
        assert_eq!(
            keys,
            vec![
                "heroku.dyno",
                "heroku.router.at",
                "heroku.source",
                "sentry.origin"
            ]
        );
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
            "heroku.dyno": {
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
            "heroku.source": {
              "type": "string",
              "value": "heroku"
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
    }

    /// Runs the full pipeline over arbitrary bytes, which must never panic.
    fn parse_all(payload: &[u8]) {
        for frame in parse_logplex_frames(payload).flatten() {
            let frame = String::from_utf8_lossy(frame);
            if let Ok(message) = parse_logplex_message(&frame) {
                logplex_message_to_sentry_log(message, HEADERS);
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
