use chrono::{DateTime, Utc};
use relay_conventions::ORIGIN;
use relay_event_schema::protocol::{Attributes, OurLog, OurLogLevel, Timestamp, TraceId};
use relay_protocol::{Annotated, Meta, Remark, RemarkType};
use rsyslog::{Message, parser::Skip, parser::msg::Raw};

type LogplexMessage<'a> = Message<'a, Option<&'a str>, Skip, Raw<'a>>;

/// Parse a Logplex-framed syslog message, automatically stripping the length prefix.
///
/// Logplex frames messages with a length prefix: `<length> <syslog_message>`
/// This function detects and strips the length prefix before parsing the syslog message.
pub fn parse_logplex<'a>(msg: &'a str) -> Result<LogplexMessage<'a>, rsyslog::Error<'a>> {
    // Logplex frames messages with a length prefix: "<length> <message>"
    // Strip the length prefix before parsing
    let (_, syslog_msg) = msg.split_once(' ').ok_or_else(|| {
        rsyslog::Error::Custom(format!(
            "Invalid Logplex format: expected '<length> <message>' but got '{}'",
            msg,
        ))
    })?;

    LogplexMessage::parse(syslog_msg)
}

/// Maps syslog severity level to Sentry log level.
///
/// Syslog severity levels (RFC 5424):
/// - 0: Emergency -> Fatal
/// - 1: Alert -> Fatal
/// - 2: Critical -> Fatal
/// - 3: Error -> Error
/// - 4: Warning -> Warn
/// - 5: Notice -> Info
/// - 6: Informational -> Info
/// - 7: Debug -> Debug
fn map_syslog_severity_to_level(severity: u8) -> OurLogLevel {
    match severity {
        0..=2 => OurLogLevel::Fatal,
        3 => OurLogLevel::Error,
        4 => OurLogLevel::Warn,
        5..=6 => OurLogLevel::Info,
        7 => OurLogLevel::Debug,
        _ => OurLogLevel::Info, // Default fallback
    }
}

/// Converts a Logplex syslog message to a Sentry log.
pub fn logplex_message_to_sentry_log<'a>(message: LogplexMessage<'a>) -> OurLog {
    let Message {
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
    } = message;

    // Parse timestamp from ISO 8601 string
    let timestamp = timestamp
        .and_then(|ts| {
            // Try parsing with timezone offset first (RFC 3339)
            DateTime::parse_from_rfc3339(ts)
                .map(|dt| Timestamp(dt.with_timezone(&Utc)))
                .ok()
                .or_else(|| {
                    // Fallback: try parsing as UTC if no timezone
                    ts.parse::<DateTime<Utc>>().map(Timestamp).ok()
                })
        })
        .map(Annotated::new)
        .unwrap_or_else(|| {
            // If timestamp parsing fails, use current time with a remark
            let mut meta = Meta::default();
            meta.add_remark(Remark::new(
                RemarkType::Substituted,
                "timestamp.invalid_or_missing",
            ));
            Annotated(Some(Timestamp(Utc::now())), meta)
        });

    let mut attributes = Attributes::default();
    add_attribute!(attributes, ORIGIN, "auto.log_drain.heroku");

    add_optional_attribute!(attributes, "host.name", hostname);
    add_optional_attribute!(attributes, "syslog.app_name", app_name);
    add_optional_attribute!(attributes, "syslog.proc_id", proc_id);
    add_optional_attribute!(attributes, "syslog.msg_id", msg_id);
    add_attribute!(attributes, "syslog.severity", severity);
    add_attribute!(attributes, "syslog.facility", facility);
    add_attribute!(attributes, "syslog.version", version);

    // For Heroku logs, we don't have trace info, so we generate random trace_id
    let trace_id = {
        let mut meta = Meta::default();
        meta.add_remark(Remark::new(RemarkType::Substituted, "trace_id.missing"));
        Annotated(Some(TraceId::random()), meta)
    };

    OurLog {
        timestamp,
        trace_id,
        level: Annotated::new(map_syslog_severity_to_level(severity)),
        body: Annotated::new(msg.msg.to_owned()),
        attributes: Annotated::new(attributes),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_heroku_syslog_message() {
        // let msg = r#"<158>1 2021-03-01T19:04:19.887695+00:00 host heroku router - at=info method=POST path="/api/v1/events/smartcam" host=ratatoskr.mobility46.se request_id=5599e09a-f8e3-4ed9-8be8-6883ce842cf2 fwd="157.230.107.240" dyno=web.1 connect=0ms service=97ms status=200 bytes=140 protocol=https"#;
        // let msg = r#"83 <40>1 2012-11-30T06:45:29+00:00 host app web.3 - State changed from starting to up"#;
        // let msg = r#"119 <40>1 2012-11-30T06:45:26+00:00 host app web.3 - Starting process with command `bundle exec rackup config.ru -p 24405`"#;
        let msg = r#"530 <134>1 2016-02-13T21:20:25+00:00 host app heroku-postgres - source=DATABASE sample#current_transaction=15365 sample#db_size=4347350804bytes sample#tables=43 sample#active-connections=6 sample#waiting-connections=0 sample#index-cache-hit-rate=0.97116 sample#table-cache-hit-rate=0.73958 sample#load-avg-1m=0.05 sample#load-avg-5m=0.03 sample#load-avg-15m=0.035 sample#read-iops=0 sample#write-iops=112.73 sample#memory-total=15405636.0kB sample#memory-free=214004kB sample#memory-cached=14392920.0kB sample#memory-postgres=181644kB"#;

        // Use Skip for structured_data to skip parsing structured data
        // Use Raw for the message part to parse the rest as raw text
        let message: LogplexMessage = parse_logplex(msg).unwrap();

        // Test conversion to OurLog
        let our_log = logplex_message_to_sentry_log(message);
        assert_eq!(our_log.level.value().unwrap(), &OurLogLevel::Info);
        assert!(our_log.body.value().unwrap().contains("source=DATABASE"));

        // Check that origin attribute exists and has correct value
        let origin_value = our_log
            .attributes
            .value()
            .unwrap()
            .get_value("sentry.origin")
            .and_then(|v| v.as_str());
        assert_eq!(origin_value, Some("auto.log_drain.heroku"));

        assert!(our_log.timestamp.value().is_some());
        assert!(our_log.trace_id.value().is_some());
    }
}
