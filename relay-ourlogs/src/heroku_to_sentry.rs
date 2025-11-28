//! Transforms Heroku Logplex syslog messages to Sentry Logs.

use chrono::{DateTime, FixedOffset, Utc};
use relay_conventions::ORIGIN;
use relay_event_schema::protocol::{Attributes, OurLog, OurLogLevel, Timestamp};
use relay_protocol::Annotated;
use syslog_loose::{ProcId, Protocol, SyslogFacility, SyslogSeverity, Variant};

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

/// A parsed Logplex syslog message.
///
/// This struct holds the parsed RFC5424 syslog fields from Heroku Logplex.
#[derive(Debug)]
pub struct LogplexMessage {
    /// Syslog facility.
    pub facility: Option<SyslogFacility>,
    /// Syslog severity.
    pub severity: Option<SyslogSeverity>,
    /// Syslog protocol version.
    pub protocol: Protocol,
    /// Message timestamp (required for Logplex messages).
    pub timestamp: DateTime<FixedOffset>,
    /// Hostname from the syslog message.
    pub hostname: Option<String>,
    /// Application name from the syslog message.
    pub app_name: Option<String>,
    /// Process ID from the syslog message.
    pub proc_id: Option<ProcId<String>>,
    /// Message ID from the syslog message.
    pub msg_id: Option<String>,
    /// The message body.
    pub msg: String,
}

/// Error type for Logplex parsing failures.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseError(&'static str);

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for ParseError {}

/// Strip the octet counting frame from a syslog message.
///
/// Logplex uses octet counting framing (RFC 6587 Section 3.4.1) where messages
/// are prefixed with their length: `<length> <syslog_message>`.
/// This function returns the syslog message without the length prefix.
fn strip_octet_frame(msg: &str) -> Option<&str> {
    msg.find('<').map(|i| &msg[i..])
}

/// Parse a Heroku Logplex syslog message.
///
/// Logplex uses octet counting framing (RFC 6587 Section 3.4.1) where messages
/// are prefixed with their length: `<length> <syslog_message>`.
///
/// # Message Format
///
/// Heroku Logplex always sends RFC5424 format messages. Note that Logplex omits the
/// STRUCTURED-DATA field, sending messages like:
/// `<PRI>VERSION TIMESTAMP HOSTNAME APP-NAME PROCID MSGID MSG`
///
/// The parser uses `syslog_loose` which may not fully parse all fields due to
/// the non-standard format. Missing fields will be None.
///
/// Example input: `83 <40>1 2012-11-30T06:45:29+00:00 host app web.3 - Hello`
pub fn parse_logplex(msg: &str) -> Result<LogplexMessage, ParseError> {
    // Strip octet counting frame (e.g., "83 <40>..." -> "<40>...")
    let syslog_msg =
        strip_octet_frame(msg).ok_or(ParseError("invalid syslog format: missing priority"))?;

    let parsed = syslog_loose::parse_message(syslog_msg, Variant::Either);

    // Timestamp is required - Logplex messages should always have timestamps
    let timestamp = parsed
        .timestamp
        .ok_or(ParseError("timestamp is required"))?;

    Ok(LogplexMessage {
        facility: parsed.facility,
        severity: parsed.severity,
        protocol: parsed.protocol,
        timestamp,
        hostname: parsed.hostname.map(|s| s.to_owned()),
        app_name: parsed.appname.map(|s| s.to_owned()),
        proc_id: parsed.procid.map(|p| match p {
            ProcId::PID(pid) => ProcId::PID(pid),
            ProcId::Name(name) => ProcId::Name(name.to_owned()),
        }),
        msg_id: parsed.msgid.map(|s| s.to_owned()),
        msg: parsed.msg.to_owned(),
    })
}

/// Converts a parsed Logplex message to a Sentry log.
pub fn logplex_message_to_sentry_log(
    logplex_message: LogplexMessage,
    frame_id: Option<&str>,
    drain_token: Option<&str>,
    user_agent: Option<&str>,
) -> OurLog {
    let LogplexMessage {
        facility,
        severity,
        protocol,
        timestamp,
        hostname,
        app_name,
        proc_id,
        msg_id,
        msg,
    } = logplex_message;

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

    // Convert facility enum to numeric code for syslog attribute
    let facility_code: i64 = match facility {
        Some(SyslogFacility::LOG_KERN) => 0,
        Some(SyslogFacility::LOG_USER) => 1,
        Some(SyslogFacility::LOG_MAIL) => 2,
        Some(SyslogFacility::LOG_DAEMON) => 3,
        Some(SyslogFacility::LOG_AUTH) => 4,
        Some(SyslogFacility::LOG_SYSLOG) => 5,
        Some(SyslogFacility::LOG_LPR) => 6,
        Some(SyslogFacility::LOG_NEWS) => 7,
        Some(SyslogFacility::LOG_UUCP) => 8,
        Some(SyslogFacility::LOG_CRON) => 9,
        Some(SyslogFacility::LOG_AUTHPRIV) => 10,
        Some(SyslogFacility::LOG_FTP) => 11,
        Some(SyslogFacility::LOG_NTP) => 12,
        Some(SyslogFacility::LOG_AUDIT) => 13,
        Some(SyslogFacility::LOG_ALERT) => 14,
        Some(SyslogFacility::LOG_CLOCKD) => 15,
        Some(SyslogFacility::LOG_LOCAL0) => 16,
        Some(SyslogFacility::LOG_LOCAL1) => 17,
        Some(SyslogFacility::LOG_LOCAL2) => 18,
        Some(SyslogFacility::LOG_LOCAL3) => 19,
        Some(SyslogFacility::LOG_LOCAL4) => 20,
        Some(SyslogFacility::LOG_LOCAL5) => 21,
        Some(SyslogFacility::LOG_LOCAL6) => 22,
        Some(SyslogFacility::LOG_LOCAL7) => 23,
        None => 1, // Default to LOG_USER
    };

    // Convert protocol to version number
    let version: i64 = match protocol {
        Protocol::RFC5424(v) => v.into(),
        Protocol::RFC3164 => 0,
    };

    // Convert severity enum to Sentry log level
    // Mapping follows OpenTelemetry syslog semantic conventions:
    // https://opentelemetry.io/docs/specs/otel/logs/data-model-appendix/#appendix-b-severitynumber-example-mappings
    let level = match severity {
        Some(SyslogSeverity::SEV_EMERG) => OurLogLevel::Fatal, // Emergency
        Some(SyslogSeverity::SEV_ALERT) => OurLogLevel::Fatal, // Alert
        Some(SyslogSeverity::SEV_CRIT) => OurLogLevel::Fatal,  // Critical
        Some(SyslogSeverity::SEV_ERR) => OurLogLevel::Error,   // Error
        Some(SyslogSeverity::SEV_WARNING) => OurLogLevel::Warn, // Warning
        Some(SyslogSeverity::SEV_NOTICE) => OurLogLevel::Info, // Notice
        Some(SyslogSeverity::SEV_INFO) => OurLogLevel::Info,   // Informational
        Some(SyslogSeverity::SEV_DEBUG) => OurLogLevel::Debug, // Debug
        None => OurLogLevel::Info,                             // Default to INFO
    };

    // Parse logfmt key-value pairs from message body for message parameterization
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
    let template = (!parameters.is_empty()).then_some(template_parts.join(" "));

    // Convert procid to string
    let proc_id_str: Option<String> = match proc_id {
        Some(ProcId::PID(pid)) => Some(pid.to_string()),
        Some(ProcId::Name(name)) => Some(name),
        None => None,
    };

    // Add syslog fields following OpenTelemetry syslog semantic conventions
    // See: https://opentelemetry.io/docs/specs/otel/logs/data-model-appendix/#rfc5424-syslog
    add_attribute!("syslog.facility", facility_code);
    add_attribute!("syslog.version", version);
    add_optional_attribute!("syslog.procid", proc_id_str.as_deref());
    add_optional_attribute!("syslog.msgid", msg_id.as_deref());
    add_optional_attribute!("resource.host.name", hostname.as_deref());
    add_optional_attribute!("resource.service.name", app_name.as_deref());

    // Add Heroku Logplex specific fields from log drain
    add_optional_attribute!("heroku.logplex.frame_id", frame_id);
    add_optional_attribute!("heroku.logplex.drain_token", drain_token);
    add_optional_attribute!("heroku.logplex.version", user_agent);

    // Add logfmt parameters from the message body
    add_optional_attribute!("sentry.message.template", template.as_deref());
    for (key, value) in parameters {
        let param_key = format!("sentry.message.parameter.{key}");
        add_attribute!(param_key, value);
    }

    OurLog {
        timestamp: Annotated::new(Timestamp(timestamp.with_timezone(&Utc))),
        level: Annotated::new(level),
        body: Annotated::new(msg),
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
        LogplexMessage {
            facility: Some(
                LOG_SYSLOG,
            ),
            severity: Some(
                SEV_EMERG,
            ),
            protocol: RFC5424(
                1,
            ),
            timestamp: 2012-11-30T06:45:29+00:00,
            hostname: Some(
                "host",
            ),
            app_name: Some(
                "app",
            ),
            proc_id: Some(
                Name(
                "web.3",
                ),
            ),
            msg_id: None,
                msg: "State changed from starting to up",
        }
        "#);
    }

    #[test]
    fn test_parse_logplex_process_start_with_backticks() {
        let input = "119 <40>1 2012-11-30T06:45:26+00:00 host app web.3 - Starting process with command `bundle exec rackup config.ru -p 24405`";
        let msg = parse_logplex(input).unwrap();
        insta::assert_debug_snapshot!(msg, @r#"
        LogplexMessage {
            facility: Some(
                LOG_SYSLOG,
            ),
            severity: Some(
                SEV_EMERG,
            ),
            protocol: RFC5424(
                1,
            ),
            timestamp: 2012-11-30T06:45:26+00:00,
            hostname: Some(
                "host",
            ),
            app_name: Some(
                "app",
            ),
            proc_id: Some(
                Name(
                "web.3",
                ),
            ),
            msg_id: None,
                msg: "Starting process with command `bundle exec rackup config.ru -p 24405`",
        }
        "#);
    }

    #[test]
    fn test_parse_logplex_heroku_dyno_with_drain_token() {
        let input = "156 <40>1 2012-11-30T06:45:26+00:00 heroku web.3 d.73ea7440-270a-435a-a0ea-adf50b4e5f5a - Starting process with command `bundle exec rackup config.ru -p 24405`";
        let msg = parse_logplex(input).unwrap();
        insta::assert_debug_snapshot!(msg, @r#"
        LogplexMessage {
            facility: Some(
                LOG_SYSLOG,
            ),
            severity: Some(
                SEV_EMERG,
            ),
            protocol: RFC5424(
                1,
            ),
            timestamp: 2012-11-30T06:45:26+00:00,
            hostname: Some(
                "heroku",
            ),
            app_name: Some(
                "web.3",
            ),
            proc_id: Some(
                Name(
                "d.73ea7440-270a-435a-a0ea-adf50b4e5f5a",
                ),
            ),
            msg_id: None,
                msg: "Starting process with command `bundle exec rackup config.ru -p 24405`",
        }
        "#);
    }

    #[test]
    fn test_parse_logplex_heroku_postgres_metrics() {
        let input = "530 <134>1 2016-02-13T21:20:25+00:00 host app heroku-postgres - source=DATABASE sample#current_transaction=15365 sample#db_size=4347350804bytes sample#tables=43 sample#active-connections=6 sample#waiting-connections=0 sample#index-cache-hit-rate=0.97116 sample#table-cache-hit-rate=0.73958 sample#load-avg-1m=0.05 sample#load-avg-5m=0.03 sample#load-avg-15m=0.035 sample#read-iops=0 sample#write-iops=112.73 sample#memory-total=15405636.0kB sample#memory-free=214004kB sample#memory-cached=14392920.0kB sample#memory-postgres=181644kB";
        let msg = parse_logplex(input).unwrap();
        insta::assert_debug_snapshot!(msg, @r#"
        LogplexMessage {
            facility: Some(
                LOG_LOCAL0,
            ),
            severity: Some(
                SEV_INFO,
            ),
            protocol: RFC5424(
                1,
            ),
            timestamp: 2016-02-13T21:20:25+00:00,
            hostname: Some(
                "host",
            ),
            app_name: Some(
                "app",
            ),
            proc_id: Some(
                Name(
                "heroku-postgres",
                ),
            ),
            msg_id: None,
                msg: "source=DATABASE sample#current_transaction=15365 sample#db_size=4347350804bytes sample#tables=43 sample#active-connections=6 sample#waiting-connections=0 sample#index-cache-hit-rate=0.97116 sample#table-cache-hit-rate=0.73958 sample#load-avg-1m=0.05 sample#load-avg-5m=0.03 sample#load-avg-15m=0.035 sample#read-iops=0 sample#write-iops=112.73 sample#memory-total=15405636.0kB sample#memory-free=214004kB sample#memory-cached=14392920.0kB sample#memory-postgres=181644kB",
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
