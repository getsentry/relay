//! Utility functions for working with user agents.

use lazy_static::lazy_static;
use uaparser::{Parser, UserAgentParser};

#[doc(inline)]
pub use uaparser::{Device, UserAgent, OS};

lazy_static! {
    /// The global [`UserAgentParser`] already configured with a user agent database.
    ///
    /// For usage, see [`Parser`].
    static ref UA_PARSER: UserAgentParser = {
        let ua_regexes = include_bytes!("../uap-core/regexes.yaml");
        UserAgentParser::from_bytes(ua_regexes).expect(
            "Could not create UserAgent. \
             You are probably using a bad build of 'relay-general'. ",
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

/// Initializes the user agent parser.
///
/// This loads and compiles user agent patterns, which takes a few seconds to complete. The user
/// agent parser initializes on-demand when using one of the parse methods. This function forces
/// initialization at a convenient point without introducing unwanted delays.
pub fn init_parser() {
    lazy_static::initialize(&UA_PARSER);
}

/// Returns the user agent string from an `event`.
///
/// Returns `Some` if the event's request interface contains a `user-agent` header. Returns `None`
/// otherwise.
pub fn get_user_agent(event: &Event) -> Option<&str> {
    let request = event.request.value()?;
    let headers = request.headers.value()?;
    get_user_agent_from_headers(headers)
}

/// Returns the family and version of a user agent client.
///
/// Defaults to an empty user agent.
pub fn parse_user_agent(user_agent: &str) -> UserAgent {
    UA_PARSER.parse_user_agent(user_agent)
}

/// Returns the family, brand, and model of the device of the requesting client.
///
/// Defaults to an empty device.
pub fn parse_device(user_agent: &str) -> Device {
    UA_PARSER.parse_device(user_agent)
}

/// Returns the family and version of the operating system of the requesting client.
///
/// Defaults to an empty operating system.
pub fn parse_os(user_agent: &str) -> OS {
    UA_PARSER.parse_os(user_agent)
}
