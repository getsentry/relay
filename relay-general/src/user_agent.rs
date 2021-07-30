//! Utility functions for working with the event user-agent.
use lazy_static::lazy_static;
pub use uaparser::{Device, UserAgent, OS};
use uaparser::{Parser, UserAgentParser};

lazy_static! {
    /// The global [`UserAgentParser`] already configured with a user agent database.
    ///
    /// For usage, see [`Parser`].
    static ref UA_PARSER: UserAgentParser = {
        let ua_regexes = include_bytes!("../uap-core/regexes.yaml");
        UserAgentParser::from_bytes(ua_regexes).expect(
            "Could not create UserAgent. \
             You are probably using a bad build of 'relay-common'. ",
        )
    };
}

use crate::protocol::{Event, Headers};

fn get_user_agent_from_headers(headers: &Headers) -> Option<&str> {
    for item in headers.iter() {
        if let Some((ref o_k, ref v)) = item.value() {
            if let Some(k) = o_k.as_str() {
                if k.to_lowercase() == "user-agent" {
                    return v.as_str();
                }
            }
        }
    }

    None
}

/// Returns the user agent from an event (if available) or None (if not present).
pub fn get_user_agent(event: &Event) -> Option<&str> {
    let request = event.request.value()?;
    let headers = request.headers.value()?;
    get_user_agent_from_headers(headers)
}

pub fn parse_user_agent(user_agent: &str) -> UserAgent {
    UA_PARSER.parse_user_agent(user_agent)
}

pub fn parse_device(user_agent: &str) -> Device {
    UA_PARSER.parse_device(user_agent)
}

pub fn parse_os(user_agent: &str) -> OS {
    UA_PARSER.parse_os(user_agent)
}
