//! Transforms Heroku Logplex syslog messages to Sentry Logs.

use chrono::Utc;
use relay_conventions::ORIGIN;
use relay_event_schema::protocol::{Attributes, OurLog, OurLogLevel, Timestamp};
use relay_protocol::Annotated;
use rsyslog::parser::{DateTime, Skip};
use rsyslog::{Message, Originator, ParseMsg, ParsePart};

/// Header names for Heroku Logplex integration.
///
/// These are the keys used to store Heroku-specific metadata on envelope items.
/// The values come from HTTP headers sent by Logplex in HTTPS drain requests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HerokuHeader {
    /// The unique request identifier.
    ///
    /// Logplex sets this to a unique ID for each request. If a request is retried
    /// (e.g., due to non-2xx response or network failure), this ID enables detection
    /// of duplicate requests.
    FrameId,
    /// The drain token.
    ///
    /// This token identifies the specific log drain sending the request.
    /// It can be used to associate logs with their source drain configuration.
    DrainToken,
    /// The Logplex user agent.
    ///
    /// Describes the version of Logplex (e.g., "Logplex/v72").
    /// This changes with Logplex releases.
    UserAgent,
}

impl HerokuHeader {
    /// Returns the HTTP header name sent by Logplex.
    ///
    /// These are the original header names from the Logplex HTTPS drain request.
    pub fn http_header_name(&self) -> &'static str {
        match self {
            Self::FrameId => "Logplex-Frame-Id",
            Self::DrainToken => "Logplex-Drain-Token",
            Self::UserAgent => "User-Agent",
        }
    }

    /// Returns the string key used to store this header on envelope items.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::FrameId => "heroku-frame-id",
            Self::DrainToken => "heroku-drain-token",
            Self::UserAgent => "heroku-user-agent",
        }
    }
}

/// Parsed message body from a Logplex syslog message.
///
/// This struct stores the raw message body along with any logfmt key-value pairs
/// that were parsed from it, using Sentry's message parameterization conventions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogplexMsgBody<'a> {
    /// The raw message body.
    pub msg: &'a str,
    /// The parameterized template string (if logfmt pairs were found).
    /// Example: `"source={source} sample#db_size={sample#db_size}"`
    pub template: Option<String>,
    /// The parsed logfmt parameters as (key, value) pairs.
    /// Keys are the original logfmt keys, values are the parsed values.
    pub parameters: Vec<(&'a str, &'a str)>,
}

impl<'a> ParseMsg<'a> for LogplexMsgBody<'a> {
    fn parse(msg: &'a str, _: &Originator) -> Result<(&'a str, Self), rsyslog::Error<'a>> {
        let mut template_parts = Vec::new();
        let mut parameters = Vec::new();

        for pair in msg.split_whitespace() {
            if let Some((key, value)) = pair.split_once('=')
                && !key.is_empty()
            {
                template_parts.push(format!("{key}={{{key}}}"));
                parameters.push((key, value));
            }
        }

        Ok((
            "",
            LogplexMsgBody {
                msg,
                template: (!parameters.is_empty()).then_some(template_parts.join(" ")),
                parameters,
            },
        ))
    }
}

/// A required timestamp that errors during parsing if missing.
///
/// Unlike `Option<DateTime>`, this type rejects the syslog nil value "-"
/// and requires a valid RFC3339 timestamp.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequiredDateTime(pub DateTime);

impl<'a> ParsePart<'a> for RequiredDateTime {
    fn parse(part: &'a str) -> Result<(&'a str, Self), rsyslog::Error<'a>> {
        let (rem, timestamp) = <Option<DateTime>>::parse(part)?;
        match timestamp {
            Some(dt) => Ok((rem, RequiredDateTime(dt))),
            None => Err(rsyslog::Error::Custom("timestamp is required".to_owned())),
        }
    }
}

impl From<RequiredDateTime> for Timestamp {
    fn from(value: RequiredDateTime) -> Self {
        Timestamp(value.0.with_timezone(&Utc))
    }
}

/// A parsed Logplex syslog message.
///
/// This type uses rsyslog's generic Message type with:
/// - `RequiredDateTime` for the timestamp (must be present, not "-")
/// - `Skip` for the structured data (which Logplex omits per their docs)
/// - `LogplexMsgBody<'a>` for the message body (with logfmt parsing)
pub type LogplexMessage<'a> = Message<'a, RequiredDateTime, Skip, LogplexMsgBody<'a>>;

/// Parse a Logplex-framed syslog message.
///
/// Logplex uses octet counting framing (RFC 6587 Section 3.4.1) where messages
/// are prefixed with their length: `<length> <syslog_message>`. The rsyslog parser
/// handles this by skipping any content before the first `<` when parsing the
/// priority field.
pub fn parse_logplex(msg: &str) -> Result<LogplexMessage<'_>, rsyslog::Error<'_>> {
    LogplexMessage::parse(msg)
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

/// Converts a parsed Logplex message to a Sentry log.
pub fn logplex_message_to_sentry_log(
    logplex_message: LogplexMessage<'_>,
    frame_id: Option<&str>,
    drain_token: Option<&str>,
    user_agent: Option<&str>,
) -> OurLog {
    let LogplexMessage {
        facility,
        severity,
        version,
        timestamp,
        hostname,
        app_name,
        proc_id,
        msg_id,
        msg,
        structured_data: _,
    } = logplex_message;

    let LogplexMsgBody {
        msg,
        template,
        parameters,
    } = msg;

    let mut attributes = Attributes::default();

    macro_rules! add_optional_attribute {
        ($name:expr, $value:expr) => {{
            if let Some(value) = $value {
                attributes.insert($name.to_owned(), value.to_owned());
            }
        }};
    }

    macro_rules! add_attribute {
        ($name:expr, $value:expr) => {{
            let val = $value;
            attributes.insert($name.to_owned(), val.to_owned());
        }};
    }

    add_attribute!(ORIGIN, "auto.log_drain.heroku");

    // Add syslog fields following OpenTelemetry syslog semantic conventions
    // See: https://opentelemetry.io/docs/specs/otel/logs/data-model-appendix/#rfc5424-syslog
    add_attribute!("syslog.facility", facility as i64);
    add_attribute!("syslog.version", version as i64);
    add_optional_attribute!("syslog.procid", proc_id);
    add_optional_attribute!("syslog.msgid", msg_id);
    add_optional_attribute!("resource.host.name", hostname);
    add_optional_attribute!("resource.service.name", app_name);

    // Add Heroku Logplex specific fields from log drain
    add_optional_attribute!("heroku.logplex.frame_id", frame_id);
    add_optional_attribute!("heroku.logplex.drain_token", drain_token);
    add_optional_attribute!("heroku.logplex.version", user_agent);

    // Add pre-parsed logfmt parameters from the message body
    add_optional_attribute!("sentry.message.template", template);
    for (key, value) in parameters {
        let param_key = format!("sentry.message.parameter.{key}");
        add_attribute!(param_key, value);
    }

    OurLog {
        timestamp: Annotated::new(timestamp.into()),
        level: Annotated::new(map_syslog_severity_to_level(severity)),
        body: Annotated::new(msg.to_owned()),
        attributes: Annotated::new(attributes),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use relay_protocol::SerializableAnnotated;
    #[test]
    fn test_parse_logplex_basic_state_change() {
        let input =
            "83 <40>1 2012-11-30T06:45:29+00:00 host app web.3 - State changed from starting to up";
        let msg = parse_logplex(input).unwrap();
        insta::assert_debug_snapshot!(msg, @r#"
        Message {
            facility: 5,
            severity: 0,
            version: 1,
            timestamp: RequiredDateTime(
                2012-11-30T06:45:29+00:00,
            ),
            hostname: Some(
                "host",
            ),
            app_name: Some(
                "app",
            ),
            proc_id: Some(
                "web.3",
            ),
            msg_id: None,
            structured_data: Skip,
            msg: LogplexMsgBody {
                msg: "State changed from starting to up",
                template: None,
                parameters: [],
            },
        }
        "#);
    }

    #[test]
    fn test_parse_logplex_process_start_with_backticks() {
        let input = "119 <40>1 2012-11-30T06:45:26+00:00 host app web.3 - Starting process with command `bundle exec rackup config.ru -p 24405`";
        let msg = parse_logplex(input).unwrap();
        insta::assert_debug_snapshot!(msg, @r#"
        Message {
            facility: 5,
            severity: 0,
            version: 1,
            timestamp: RequiredDateTime(
                2012-11-30T06:45:26+00:00,
            ),
            hostname: Some(
                "host",
            ),
            app_name: Some(
                "app",
            ),
            proc_id: Some(
                "web.3",
            ),
            msg_id: None,
            structured_data: Skip,
            msg: LogplexMsgBody {
                msg: "Starting process with command `bundle exec rackup config.ru -p 24405`",
                template: None,
                parameters: [],
            },
        }
        "#);
    }

    #[test]
    fn test_parse_logplex_heroku_dyno_with_drain_token() {
        let input = "156 <40>1 2012-11-30T06:45:26+00:00 heroku web.3 d.73ea7440-270a-435a-a0ea-adf50b4e5f5a - Starting process with command `bundle exec rackup config.ru -p 24405`";
        let msg = parse_logplex(input).unwrap();
        insta::assert_debug_snapshot!(msg, @r#"
        Message {
            facility: 5,
            severity: 0,
            version: 1,
            timestamp: RequiredDateTime(
                2012-11-30T06:45:26+00:00,
            ),
            hostname: Some(
                "heroku",
            ),
            app_name: Some(
                "web.3",
            ),
            proc_id: Some(
                "d.73ea7440-270a-435a-a0ea-adf50b4e5f5a",
            ),
            msg_id: None,
            structured_data: Skip,
            msg: LogplexMsgBody {
                msg: "Starting process with command `bundle exec rackup config.ru -p 24405`",
                template: None,
                parameters: [],
            },
        }
        "#);
    }

    #[test]
    fn test_parse_logplex_heroku_postgres_metrics() {
        let input = "530 <134>1 2016-02-13T21:20:25+00:00 host app heroku-postgres - source=DATABASE sample#current_transaction=15365 sample#db_size=4347350804bytes sample#tables=43 sample#active-connections=6 sample#waiting-connections=0 sample#index-cache-hit-rate=0.97116 sample#table-cache-hit-rate=0.73958 sample#load-avg-1m=0.05 sample#load-avg-5m=0.03 sample#load-avg-15m=0.035 sample#read-iops=0 sample#write-iops=112.73 sample#memory-total=15405636.0kB sample#memory-free=214004kB sample#memory-cached=14392920.0kB sample#memory-postgres=181644kB";
        let msg = parse_logplex(input).unwrap();
        insta::assert_debug_snapshot!(msg, @r#"
        Message {
            facility: 16,
            severity: 6,
            version: 1,
            timestamp: RequiredDateTime(
                2016-02-13T21:20:25+00:00,
            ),
            hostname: Some(
                "host",
            ),
            app_name: Some(
                "app",
            ),
            proc_id: Some(
                "heroku-postgres",
            ),
            msg_id: None,
            structured_data: Skip,
            msg: LogplexMsgBody {
                msg: "source=DATABASE sample#current_transaction=15365 sample#db_size=4347350804bytes sample#tables=43 sample#active-connections=6 sample#waiting-connections=0 sample#index-cache-hit-rate=0.97116 sample#table-cache-hit-rate=0.73958 sample#load-avg-1m=0.05 sample#load-avg-5m=0.03 sample#load-avg-15m=0.035 sample#read-iops=0 sample#write-iops=112.73 sample#memory-total=15405636.0kB sample#memory-free=214004kB sample#memory-cached=14392920.0kB sample#memory-postgres=181644kB",
                template: Some(
                    "source={source} sample#current_transaction={sample#current_transaction} sample#db_size={sample#db_size} sample#tables={sample#tables} sample#active-connections={sample#active-connections} sample#waiting-connections={sample#waiting-connections} sample#index-cache-hit-rate={sample#index-cache-hit-rate} sample#table-cache-hit-rate={sample#table-cache-hit-rate} sample#load-avg-1m={sample#load-avg-1m} sample#load-avg-5m={sample#load-avg-5m} sample#load-avg-15m={sample#load-avg-15m} sample#read-iops={sample#read-iops} sample#write-iops={sample#write-iops} sample#memory-total={sample#memory-total} sample#memory-free={sample#memory-free} sample#memory-cached={sample#memory-cached} sample#memory-postgres={sample#memory-postgres}",
                ),
                parameters: [
                    (
                        "source",
                        "DATABASE",
                    ),
                    (
                        "sample#current_transaction",
                        "15365",
                    ),
                    (
                        "sample#db_size",
                        "4347350804bytes",
                    ),
                    (
                        "sample#tables",
                        "43",
                    ),
                    (
                        "sample#active-connections",
                        "6",
                    ),
                    (
                        "sample#waiting-connections",
                        "0",
                    ),
                    (
                        "sample#index-cache-hit-rate",
                        "0.97116",
                    ),
                    (
                        "sample#table-cache-hit-rate",
                        "0.73958",
                    ),
                    (
                        "sample#load-avg-1m",
                        "0.05",
                    ),
                    (
                        "sample#load-avg-5m",
                        "0.03",
                    ),
                    (
                        "sample#load-avg-15m",
                        "0.035",
                    ),
                    (
                        "sample#read-iops",
                        "0",
                    ),
                    (
                        "sample#write-iops",
                        "112.73",
                    ),
                    (
                        "sample#memory-total",
                        "15405636.0kB",
                    ),
                    (
                        "sample#memory-free",
                        "214004kB",
                    ),
                    (
                        "sample#memory-cached",
                        "14392920.0kB",
                    ),
                    (
                        "sample#memory-postgres",
                        "181644kB",
                    ),
                ],
            },
        }
        "#);
    }

    #[test]
    fn test_parse_logplex_invalid_format() {
        let msg = "invalid";
        let result = parse_logplex(msg);
        assert!(result.is_err());
    }

    #[test]
    fn test_logplex_message_to_sentry_log() {
        let msg =
            "83 <40>1 2012-11-30T06:45:29+00:00 host app web.3 - State changed from starting to up";
        let parsed = parse_logplex(msg).unwrap();

        let ourlog = logplex_message_to_sentry_log(
            parsed,
            Some("frame-123"),
            Some("d.abc123"),
            Some("Logplex/v72"),
        );

        let ourlog = Annotated::new(ourlog);
        insta::assert_json_snapshot!(SerializableAnnotated(&ourlog), @r#"
        {
          "timestamp": 1354257929.0,
          "level": "fatal",
          "body": "State changed from starting to up",
          "attributes": {
            "heroku.logplex.drain_token": {
              "type": "string",
              "value": "d.abc123"
            },
            "heroku.logplex.frame_id": {
              "type": "string",
              "value": "frame-123"
            },
            "heroku.logplex.version": {
              "type": "string",
              "value": "Logplex/v72"
            },
            "resource.host.name": {
              "type": "string",
              "value": "host"
            },
            "resource.service.name": {
              "type": "string",
              "value": "app"
            },
            "sentry.origin": {
              "type": "string",
              "value": "auto.log_drain.heroku"
            },
            "syslog.facility": {
              "type": "integer",
              "value": 5
            },
            "syslog.procid": {
              "type": "string",
              "value": "web.3"
            },
            "syslog.version": {
              "type": "integer",
              "value": 1
            }
          }
        }
        "#);
    }

    #[test]
    fn test_logplex_message_to_sentry_log_with_logfmt() {
        let input = "100 <134>1 2016-02-13T21:20:25+00:00 host app heroku-postgres - source=DATABASE sample#db_size=1234bytes";
        let parsed = parse_logplex(input).unwrap();

        let ourlog = logplex_message_to_sentry_log(parsed, None, None, None);

        let ourlog = Annotated::new(ourlog);
        insta::assert_json_snapshot!(SerializableAnnotated(&ourlog), @r#"
        {
          "timestamp": 1455398425.0,
          "level": "info",
          "body": "source=DATABASE sample#db_size=1234bytes",
          "attributes": {
            "resource.host.name": {
              "type": "string",
              "value": "host"
            },
            "resource.service.name": {
              "type": "string",
              "value": "app"
            },
            "sentry.message.parameter.sample#db_size": {
              "type": "string",
              "value": "1234bytes"
            },
            "sentry.message.parameter.source": {
              "type": "string",
              "value": "DATABASE"
            },
            "sentry.message.template": {
              "type": "string",
              "value": "source={source} sample#db_size={sample#db_size}"
            },
            "sentry.origin": {
              "type": "string",
              "value": "auto.log_drain.heroku"
            },
            "syslog.facility": {
              "type": "integer",
              "value": 16
            },
            "syslog.procid": {
              "type": "string",
              "value": "heroku-postgres"
            },
            "syslog.version": {
              "type": "integer",
              "value": 1
            }
          }
        }
        "#);
    }
}
