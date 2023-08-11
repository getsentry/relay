//! Utility functions for working with user agents.
//!
//! NOTICE:
//!
//! Adding user_agent parsing to your module will incur a latency penalty in the test suite.
//! Because of this some integration tests may fail. To fix this, you will need to add a timeout
//! to your consumer.

use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
#[doc(inline)]
pub use uaparser::{Device, UserAgent, OS};
use uaparser::{Parser, UserAgentParser};

use crate::protocol::{HeaderName, HeaderValue, Headers, Request};
use crate::types::Annotated;

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
                if k.eq_ignore_ascii_case("user-agent") {
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

impl<S: AsRef<str> + Default> RawUserAgentInfo<S> {
    /// Checks if key matches a user agent header, in which case it sets the value accordingly.
    ///  TODO(tor): make it generic over different header types.
    pub fn set_ua_field_from_header(&mut self, key: &str, value: Option<S>) {
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

    /// Convert user-agent info to HTTP headers as stored in the `Request` interface.
    ///
    /// This function does not overwrite any pre-existing headers.
    pub fn populate_event_headers(&self, headers: &mut Headers) {
        let mut insert_header = |key: &str, val: Option<&S>| {
            if let Some(val) = val {
                if !headers.contains(key) {
                    headers.insert(HeaderName::new(key), Annotated::new(HeaderValue::new(val)));
                }
            }
        };

        insert_header(RawUserAgentInfo::USER_AGENT, self.user_agent.as_ref());
        insert_header(
            ClientHints::SEC_CH_UA_PLATFORM,
            self.client_hints.sec_ch_ua_platform.as_ref(),
        );
        insert_header(
            ClientHints::SEC_CH_UA_PLATFORM_VERSION,
            self.client_hints.sec_ch_ua_platform_version.as_ref(),
        );
        insert_header(ClientHints::SEC_CH_UA, self.client_hints.sec_ch_ua.as_ref());
        insert_header(
            ClientHints::SEC_CH_UA_MODEL,
            self.client_hints.sec_ch_ua_model.as_ref(),
        );
    }

    pub fn is_empty(&self) -> bool {
        self.user_agent.is_none() && self.client_hints.is_empty()
    }
}

impl RawUserAgentInfo<String> {
    pub const USER_AGENT: &str = "User-Agent";

    pub fn as_deref(&self) -> RawUserAgentInfo<&str> {
        RawUserAgentInfo::<&str> {
            user_agent: self.user_agent.as_deref(),
            client_hints: self.client_hints.as_deref(),
        }
    }
}

impl<'a> RawUserAgentInfo<&'a str> {
    pub fn from_headers(headers: &'a Headers) -> Self {
        let mut contexts: RawUserAgentInfo<&str> = Self::default();

        for item in headers.iter() {
            if let Some((ref o_k, ref v)) = item.value() {
                if let Some(k) = o_k.as_str() {
                    contexts.set_ua_field_from_header(k, v.as_str());
                }
            }
        }
        contexts
    }
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

impl<S: AsRef<str> + Default> ClientHints<S> {
    /// Checks every field of a passed-in ClientHints instance if it contains a value, and if it does,
    /// copy it to self.
    pub fn copy_from(&mut self, other: ClientHints<S>) {
        if other.sec_ch_ua_platform_version.is_some() {
            self.sec_ch_ua_platform_version = other.sec_ch_ua_platform_version;
        }
        if other.sec_ch_ua_platform.is_some() {
            self.sec_ch_ua_platform = other.sec_ch_ua_platform;
        }
        if other.sec_ch_ua_model.is_some() {
            self.sec_ch_ua_model = other.sec_ch_ua_model;
        }
        if other.sec_ch_ua.is_some() {
            self.sec_ch_ua = other.sec_ch_ua;
        }
    }

    /// Checks if every field is of value None.
    pub fn is_empty(&self) -> bool {
        self.sec_ch_ua_platform.is_none()
            && self.sec_ch_ua_platform_version.is_none()
            && self.sec_ch_ua.is_none()
            && self.sec_ch_ua_model.is_none()
    }
}

impl ClientHints<String> {
    pub const SEC_CH_UA_PLATFORM: &str = "SEC-CH-UA-Platform";
    pub const SEC_CH_UA_PLATFORM_VERSION: &str = "SEC-CH-UA-Platform-Version";
    pub const SEC_CH_UA: &str = "SEC-CH-UA";
    pub const SEC_CH_UA_MODEL: &str = "SEC-CH-UA-Model";

    pub fn as_deref(&self) -> ClientHints<&str> {
        ClientHints::<&str> {
            sec_ch_ua_platform: self.sec_ch_ua_platform.as_deref(),
            sec_ch_ua_platform_version: self.sec_ch_ua_platform_version.as_deref(),
            sec_ch_ua: self.sec_ch_ua.as_deref(),
            sec_ch_ua_model: self.sec_ch_ua_model.as_deref(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    impl RawUserAgentInfo<&str> {
        pub fn new_test_dummy() -> Self {
            Self {
                user_agent: Some("Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/109.0"),
                client_hints: ClientHints {
                    sec_ch_ua_platform: Some("macOS"),
                    sec_ch_ua_platform_version: Some("13.2.0"),
                    sec_ch_ua: Some(r#""Chromium";v="110", "Not A(Brand";v="24", "Google Chrome";v="110""#),
                    sec_ch_ua_model: Some("some model"),
                }

            }
        }
    }

    #[test]
    fn test_default_empty() {
        assert!(RawUserAgentInfo::<&str>::default().is_empty());
    }
}
