//! Utility functions for working with user agents.
//!
//! NOTICE:
//!
//! Adding user_agent parsing to your module will incur a latency penalty in the test suite.
//! Because of this some integration tests may fail. To fix this, you will need to add a timeout
//! to your consumer.

use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
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
#[derive(Default, Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct RawUserAgentInfo<S: Default + AsRef<str>> {
    /// The "old style" of a single UA string.
    pub user_agent: Option<S>,
    /// User-Agent client hints.
    pub client_hints: ClientHints<S>,
}

/// The client hint variable names mirror the name of the "SEC-CH" headers, see
/// '<https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers#user_agent_client_hints>'
#[derive(Default, Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct ClientHints<S: Default + AsRef<str>> {
    /// The client's OS, e.g. macos, android...
    pub sec_ch_ua_platform: Option<S>,
    /// The version number of the client's OS.
    pub sec_ch_ua_platform_version: Option<S>,
    /// Name of the client's web browser and its version.
    pub sec_ch_ua: Option<S>,
    /// Device model, e.g. samsung galaxy 3.
    pub sec_ch_ua_model: Option<S>,
}

/*
impl ClientHints<&str> {
    pub fn to_owned(&self) -> ClientHints<String> {
        ClientHints {
            sec_ch_ua_platform: self.sec_ch_ua_platform.map(str::to_string),
            sec_ch_ua_platform_version: self.sec_ch_ua_platform_version.map(str::to_string),
            sec_ch_ua: self.sec_ch_ua.map(str::to_string),
            sec_ch_ua_model: self.sec_ch_ua_model.map(str::to_string),
        }
    }
}
*/

impl<S: AsRef<str> + Default> ClientHints<S> {
    pub fn is_empty(&self) -> bool {
        self.sec_ch_ua_platform.is_none()
            && self.sec_ch_ua_platform_version.is_none()
            && self.sec_ch_ua.is_none()
            && self.sec_ch_ua_model.is_none()
    }
}

impl ClientHints<String> {
    pub fn as_deref(&self) -> ClientHints<&str> {
        ClientHints::<&str> {
            sec_ch_ua_platform: self.sec_ch_ua_platform.as_deref(),
            sec_ch_ua_platform_version: self.sec_ch_ua_platform_version.as_deref(),
            sec_ch_ua: self.sec_ch_ua.as_deref(),
            sec_ch_ua_model: self.sec_ch_ua_model.as_deref(),
        }
    }
}

impl RawUserAgentInfo<String> {
    pub fn as_deref(&self) -> RawUserAgentInfo<&str> {
        RawUserAgentInfo::<&str> {
            user_agent: self.user_agent.as_deref(),
            client_hints: self.client_hints.as_deref(),
        }
    }
}

impl<S: AsRef<str> + Default> RawUserAgentInfo<S> {
    pub fn extract_header(&mut self, key: &str, value: Option<S>) {
        match key.to_lowercase().as_str() {
            "user-agent" => self.user_agent = value,

            "sec-ch-ua" => self.client_hints.sec_ch_ua = value,
            "sec-ch-ua-model" => self.client_hints.sec_ch_ua_model = value,
            "sec-ch-ua-platform" => self.client_hints.sec_ch_ua_platform = value,
            "sec-ch-ua-platform-version" => {
                self.client_hints.sec_ch_ua_platform_version = value;
            }
            _ => {}
        }
    }

    pub fn is_empty(&self) -> bool {
        self.user_agent.is_none() && self.client_hints.is_empty()
    }

    pub fn from_ua(s: S) -> Self {
        Self {
            user_agent: Some(s),
            client_hints: ClientHints::default(),
        }
    }
}

impl<'a> RawUserAgentInfo<&'a str> {
    pub fn from_headers(headers: &'a Headers) -> Self {
        let mut contexts: RawUserAgentInfo<&str> = Self::default();

        for item in headers.iter() {
            if let Some((ref o_k, ref v)) = item.value() {
                if let Some(k) = o_k.as_str() {
                    contexts.extract_header(k, v.as_str());
                }
            }
        }
        contexts
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_empty() {
        assert!(RawUserAgentInfo::<&str>::default().is_empty());
    }

    /// Make sure to update all relevant code if you add a new field to RawUserAgentInfo.
    #[test]
    fn size() {
        let ua = RawUserAgentInfo::<&str>::default();
        let size = std::mem::size_of_val(&ua);
        //dbg!(size);
        assert!(size == 80);
    }
}
