//! Conversion of Heroku Logplex messages to Sentry logs.

use relay_event_schema::protocol::OurLog;
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

/// Convert a Logplex message to a Sentry log.
///
/// This function is currently a stub and will be implemented in a future iteration.
#[allow(unused_variables)]
pub fn logplex_message_to_sentry_log(message: LogplexMessage) -> OurLog {
    // TODO: Implement conversion logic
    OurLog::default()
}
