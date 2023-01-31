//! Utility functions for working with user agents.
//!
//! NOTICE:
//!
//! Adding user_agent parsing to your module will incur a latency penalty in the test suite.
//! Because of this some integration tests may fail. To fix this, you will need to add a timeout
//! to your consumer.

use once_cell::sync::Lazy;
use uaparser::{Parser, UserAgentParser};

use crate::protocol::{Headers, Request};
use crate::types::Annotated;

#[doc(inline)]
pub use uaparser::{Device, UserAgent, OS};

/// The global [`UserAgentParser`] already configured with a user agent database.
///
/// For usage, see [`Parser`].
static UA_PARSER: Lazy<UserAgentParser> = Lazy::new(|| {
    let ua_regexes = include_bytes!("../uap-core/regexes.yaml");
    UserAgentParser::from_bytes(ua_regexes).expect(
        "Could not create UserAgent. \
             You are probably using a bad build of 'relay-general'. ",
    )
});

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
    Lazy::force(&UA_PARSER);
}

/// Returns the user agent string from a `Request`.
///
/// Returns `Some` if the event's request interface contains a `user-agent` header. Returns `None`
/// otherwise.
pub fn get_user_agent(request: &Annotated<Request>) -> Option<&str> {
    let request = request.value()?;
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

/// The data container, which has both the user agent string and the client hints.
///
/// Useful for the scenarios where you will use either information from client hints if it exists,
/// and if not fall back to user agent string.
#[derive(Default, Debug)]
pub struct RawUserAgentInfo<'a> {
    /// The "old style" of a single UA string.
    pub user_agent: Option<&'a str>,
    /// User-Agent client hints.
    pub client_hints: ClientHints<'a>,
}

/// The client hint variable names mirror the name of the "SEC-CH" headers, see
/// '<https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers#user_agent_client_hints>'
#[derive(Default, Debug)]
pub struct ClientHints<'a> {
    /// The client's OS, e.g. macos, android...
    pub sec_ch_ua_platform: Option<&'a str>,
    /// The version number of the client's OS.
    pub sec_ch_ua_platform_version: Option<&'a str>,
    /// Name of the client's web browser and its version.
    pub sec_ch_ua: Option<&'a str>,
    /// Device model, e.g. samsung galaxy 3.
    pub sec_ch_ua_model: Option<&'a str>,
}

impl<'a> RawUserAgentInfo<'a> {
    pub fn new(headers: &'a Headers) -> Self {
        let mut contexts = Self::default();

        for item in headers.iter() {
            if let Some((ref o_k, ref v)) = item.value() {
                if let Some(k) = o_k.as_str() {
                    match k.to_lowercase().as_str() {
                        "user-agent" => contexts.user_agent = v.as_str(),

                        "sec-ch-ua" => contexts.client_hints.sec_ch_ua = v.as_str(),
                        "sec-ch-ua-model" => contexts.client_hints.sec_ch_ua_model = v.as_str(),
                        "sec-ch-ua-platform" => {
                            contexts.client_hints.sec_ch_ua_platform = v.as_str()
                        }
                        "sec-ch-ua-platform-version" => {
                            contexts.client_hints.sec_ch_ua_platform_version = v.as_str()
                        }
                        _ => {}
                    }
                }
            }
        }
        contexts
    }
}
