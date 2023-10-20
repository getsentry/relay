//! Generate context data from user agent and client hints.
//!
//! This module is responsible for taking the user agent string parsing it and filling in
//! the browser, os and device information in the event.
//!
//! ## NOTICE
//!
//! Adding user_agent parsing to your module will incur a latency penalty in the test suite.
//! Because of this some integration tests may fail. To fix this, you will need to add a timeout
//! to your consumer.

use std::borrow::Cow;
use std::fmt::Write;

use once_cell::sync::OnceCell;
use regex::Regex;
use relay_event_schema::protocol::{
    BrowserContext, Context, Contexts, DefaultContext, DeviceContext, Event, HeaderName,
    HeaderValue, Headers, OsContext,
};
use relay_protocol::Annotated;
use serde::{Deserialize, Serialize};

/// Generates context data from client hints or user agent.
pub fn normalize_user_agent(event: &mut Event) {
    let headers = match event
        .request
        .value()
        .and_then(|request| request.headers.value())
    {
        Some(headers) => headers,
        None => return,
    };

    let user_agent_info = RawUserAgentInfo::from_headers(headers);

    let contexts = event.contexts.get_or_insert_with(Contexts::new);
    normalize_user_agent_info_generic(contexts, &event.platform, &user_agent_info);
}

/// Low-level version of [`normalize_user_agent`] operating on parts.
///
/// This can be used to create contexts from client information without a full [`Event`] instance.
pub fn normalize_user_agent_info_generic(
    contexts: &mut Contexts,
    platform: &Annotated<String>,
    user_agent_info: &RawUserAgentInfo<&str>,
) {
    if !contexts.contains::<BrowserContext>() {
        if let Some(browser_context) = BrowserContext::from_hints_or_ua(user_agent_info) {
            contexts.add(browser_context);
        }
    }

    if !contexts.contains::<DeviceContext>() {
        if let Some(device_context) = DeviceContext::from_hints_or_ua(user_agent_info) {
            contexts.add(device_context);
        }
    }

    // avoid conflicts with OS-context sent by a serverside SDK by using `contexts.client_os`
    // instead of `contexts.os`. This is then preferred by the UI to show alongside device and
    // browser context.
    //
    // Why not move the existing `contexts.os` into a different key on conflicts? Because we still
    // want to index (derive tags from) the SDK-sent context.
    let os_context_key = match platform.as_str() {
        Some("javascript") => OsContext::default_key(),
        _ => "client_os",
    };

    if !contexts.contains_key(os_context_key) {
        if let Some(os_context) = OsContext::from_hints_or_ua(user_agent_info) {
            contexts.insert(os_context_key.to_owned(), Context::Os(Box::new(os_context)));
        }
    }
}

fn is_known(family: &str) -> bool {
    family != "Other"
}

fn get_version(
    major: &Option<Cow<'_, str>>,
    minor: &Option<Cow<'_, str>>,
    patch: &Option<Cow<'_, str>>,
) -> Option<String> {
    let mut version = major.as_ref()?.to_string();

    if let Some(minor) = minor {
        write!(version, ".{minor}").ok();
        if let Some(patch) = patch {
            write!(version, ".{patch}").ok();
        }
    }

    Some(version)
}

/// A container housing both the user-agent string and the client hint headers.
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

    /// Returns `true`, if neither a user agent nor client hints are available.
    pub fn is_empty(&self) -> bool {
        self.user_agent.is_none() && self.client_hints.is_empty()
    }
}

impl RawUserAgentInfo<String> {
    /// The name of the user agent HTTP header.
    pub const USER_AGENT: &'static str = "User-Agent";

    /// Converts to a borrowed `RawUserAgentInfo`.
    pub fn as_deref(&self) -> RawUserAgentInfo<&str> {
        RawUserAgentInfo::<&str> {
            user_agent: self.user_agent.as_deref(),
            client_hints: self.client_hints.as_deref(),
        }
    }
}

impl<'a> RawUserAgentInfo<&'a str> {
    /// Computes a borrowed `RawUserAgentInfo` from the given HTTP headers.
    ///
    /// This extracts both the user agent as well as client hints if available. Use
    /// [`is_empty`](Self::is_empty) to check whether information could be extracted.
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

/// The client hint variable names mirror the name of the "SEC-CH" headers.
///
/// See <https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers#user_agent_client_hints>
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ClientHints<S>
where
    S: Default + AsRef<str>,
{
    /// The client's OS, e.g. macos, android...
    pub sec_ch_ua_platform: Option<S>,
    /// The version number of the client's OS.
    pub sec_ch_ua_platform_version: Option<S>,
    /// Name of the client's web browser and its version.
    pub sec_ch_ua: Option<S>,
    /// Device model, e.g. samsung galaxy 3.
    pub sec_ch_ua_model: Option<S>,
}

impl<S> ClientHints<S>
where
    S: AsRef<str> + Default,
{
    /// Checks every field of a passed-in ClientHints instance if it contains a value, and if it
    /// does, copy it to self.
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
    /// Provides the platform or operating system on which the user agent is running.
    ///
    /// For example: `"Windows"` or `"Android"`.
    ///
    /// `Sec-CH-UA-Platform` is a low entropy hint. Unless blocked by a user agent permission
    /// policy, it is sent by default (without the server opting in by sending `Accept-CH`).
    pub const SEC_CH_UA_PLATFORM: &'static str = "SEC-CH-UA-Platform";

    /// Provides the version of the operating system on which the user agent is running.
    pub const SEC_CH_UA_PLATFORM_VERSION: &'static str = "SEC-CH-UA-Platform-Version";

    /// Provides the user agent's branding and significant version information.
    ///
    /// A brand is a commercial name for the user agent like: Chromium, Opera, Google Chrome,
    /// Microsoft Edge, Firefox, and Safari. A user agent might have several associated brands. For
    /// example, Opera, Chrome, and Edge are all based on Chromium, and will provide both brands in
    /// the `Sec-CH-UA` header.
    ///
    /// The significant version is the "marketing" version identifier that is used to distinguish
    /// between major releases of the brand. For example a Chromium build with full version number
    /// "96.0.4664.45" has a significant version number of "96".
    ///
    /// The header may include "fake" brands in any position and with any name. This is a feature
    /// designed to prevent servers from rejecting unknown user agents outright, forcing user agents
    /// to lie about their brand identity.
    ///
    /// `Sec-CH-UA` is a low entropy hint. Unless blocked by a user agent permission policy, it is
    /// sent by default (without the server opting in by sending `Accept-CH`).
    pub const SEC_CH_UA: &'static str = "SEC-CH-UA";

    /// Indicates the device model on which the browser is running.
    pub const SEC_CH_UA_MODEL: &'static str = "SEC-CH-UA-Model";

    /// Returns an instance of `ClientHints` that borrows from the original data.
    pub fn as_deref(&self) -> ClientHints<&str> {
        ClientHints::<&str> {
            sec_ch_ua_platform: self.sec_ch_ua_platform.as_deref(),
            sec_ch_ua_platform_version: self.sec_ch_ua_platform_version.as_deref(),
            sec_ch_ua: self.sec_ch_ua.as_deref(),
            sec_ch_ua_model: self.sec_ch_ua_model.as_deref(),
        }
    }
}

/// Computes a [`Context`] from either a user agent string and client hints.
trait FromUserAgentInfo: Sized {
    fn parse_client_hints(client_hints: &ClientHints<&str>) -> Option<Self>;

    fn parse_user_agent(user_agent: &str) -> Option<Self>;

    fn from_hints_or_ua(raw_info: &RawUserAgentInfo<&str>) -> Option<Self> {
        Self::parse_client_hints(&raw_info.client_hints)
            .or_else(|| raw_info.user_agent.and_then(Self::parse_user_agent))
    }
}

impl FromUserAgentInfo for DeviceContext {
    fn parse_client_hints(client_hints: &ClientHints<&str>) -> Option<Self> {
        let device = client_hints.sec_ch_ua_model?.trim().replace('\"', "");

        if device.is_empty() {
            return None;
        }

        Some(Self {
            model: Annotated::new(device),
            ..Default::default()
        })
    }

    fn parse_user_agent(user_agent: &str) -> Option<Self> {
        let device = relay_ua::parse_device(user_agent);

        if !is_known(&device.family) {
            return None;
        }

        Some(Self {
            family: Annotated::new(device.family.into_owned()),
            model: Annotated::from(device.model.map(|cow| cow.into_owned())),
            brand: Annotated::from(device.brand.map(|cow| cow.into_owned())),
            ..DeviceContext::default()
        })
    }
}

impl FromUserAgentInfo for BrowserContext {
    fn parse_client_hints(client_hints: &ClientHints<&str>) -> Option<Self> {
        let (browser, version) = browser_from_client_hints(client_hints.sec_ch_ua?)?;

        Some(Self {
            name: Annotated::new(browser),
            version: Annotated::new(version),
            ..Default::default()
        })
    }

    fn parse_user_agent(user_agent: &str) -> Option<Self> {
        let browser = relay_ua::parse_user_agent(user_agent);

        if !is_known(&browser.family) {
            return None;
        }

        Some(Self {
            name: Annotated::from(browser.family.into_owned()),
            version: Annotated::from(get_version(&browser.major, &browser.minor, &browser.patch)),
            ..BrowserContext::default()
        })
    }
}

/// The sec-ch-ua field looks something like this:
/// "Not_A Brand";v="99", "Google Chrome";v="109", "Chromium";v="109"
/// The order of the items are randomly shuffled.
///
/// It tries to detect the "not a brand" item and the browser engine, if it's neither its assumed
/// to be a browser and gets returned as such.
///
/// Returns None if no browser field detected.
fn browser_from_client_hints(s: &str) -> Option<(String, String)> {
    for item in s.split(',') {
        // if it contains one of these then we can know it isn't a browser field. atm chromium
        // browsers are the only ones supporting client hints.
        if item.contains("Brand")
            || item.contains("Chromium")
            || item.contains("Gecko") // useless until firefox and safari support client hints
            || item.contains("WebKit")
        {
            continue;
        }

        static UA_RE: OnceCell<Regex> = OnceCell::new();
        let regex = UA_RE.get_or_init(|| Regex::new(r#""([^"]*)";v="([^"]*)""#).unwrap());

        let captures = regex.captures(item)?;

        let browser = captures.get(1)?.as_str().to_owned();
        let version = captures.get(2)?.as_str().to_owned();

        if browser.trim().is_empty() || version.trim().is_empty() {
            return None;
        }

        return Some((browser, version));
    }
    None
}

impl FromUserAgentInfo for OsContext {
    fn parse_client_hints(client_hints: &ClientHints<&str>) -> Option<Self> {
        let platform = client_hints.sec_ch_ua_platform?.trim().replace('\"', "");

        // We only return early if the platform is empty, not the version number. This is because
        // an empty version number might suggest that the user need to request additional
        // client hints data.
        if platform.is_empty() {
            return None;
        }

        let version = client_hints
            .sec_ch_ua_platform_version
            .map(|version| version.trim().replace('\"', ""));

        Some(Self {
            name: Annotated::new(platform),
            version: Annotated::from(version),
            ..Default::default()
        })
    }

    fn parse_user_agent(user_agent: &str) -> Option<Self> {
        let os = relay_ua::parse_os(user_agent);
        let mut version = get_version(&os.major, &os.minor, &os.patch);

        if !is_known(&os.family) {
            return None;
        }

        let name = os.family.into_owned();

        // Since user-agent strings freeze the OS-version at windows 10 and mac os 10.15.7,
        // we will indicate that the version may in reality be higher.
        if name == "Windows" {
            if let Some(v) = version.as_mut() {
                if v == "10" {
                    v.insert_str(0, ">=");
                }
            }
        } else if name == "Mac OS X" {
            if let Some(v) = version.as_mut() {
                if v == "10.15.7" {
                    v.insert_str(0, ">=");
                }
            }
        }

        Some(Self {
            name: Annotated::new(name),
            version: Annotated::from(version),
            ..OsContext::default()
        })
    }
}

#[cfg(test)]
mod tests {
    use relay_event_schema::protocol::{Headers, PairList, Request};
    use relay_protocol::assert_annotated_snapshot;

    use super::*;

    /// Creates an Event with the specified user agent.
    fn get_event_with_user_agent(user_agent: &str) -> Event {
        let headers = vec![
            Annotated::new((
                Annotated::new("Accept".to_string().into()),
                Annotated::new("application/json".to_string().into()),
            )),
            Annotated::new((
                Annotated::new("UsEr-AgeNT".to_string().into()),
                Annotated::new(user_agent.to_string().into()),
            )),
            Annotated::new((
                Annotated::new("WWW-Authenticate".to_string().into()),
                Annotated::new("basic".to_string().into()),
            )),
        ];

        Event {
            request: Annotated::new(Request {
                headers: Annotated::new(Headers(PairList(headers))),
                ..Request::default()
            }),
            ..Event::default()
        }
    }

    const GOOD_UA: &str =
            "Mozilla/5.0 (Linux; Android 4.0.4; Galaxy Nexus Build/IMM76B) AppleWebKit/535.19 (KHTML, like Gecko) Chrome/18.0.1025.133 Mobile Safari/535.19";

    #[test]
    fn test_version_none() {
        assert_eq!(get_version(&None, &None, &None), None);
    }

    #[test]
    fn test_version_major() {
        assert_eq!(
            get_version(&Some("X".into()), &None, &None),
            Some("X".into())
        )
    }

    #[test]
    fn test_version_major_minor() {
        assert_eq!(
            get_version(&Some("X".into()), &Some("Y".into()), &None),
            Some("X.Y".into())
        )
    }

    #[test]
    fn test_version_major_minor_patch() {
        assert_eq!(
            get_version(&Some("X".into()), &Some("Y".into()), &Some("Z".into())),
            Some("X.Y.Z".into())
        )
    }

    #[test]
    fn test_verison_missing_minor() {
        assert_eq!(
            get_version(&Some("X".into()), &None, &Some("Z".into())),
            Some("X".into())
        )
    }

    #[test]
    fn test_skip_no_user_agent() {
        let mut event = Event::default();
        normalize_user_agent(&mut event);
        assert_eq!(event.contexts.value(), None);
    }

    #[test]
    fn test_skip_unrecognizable_user_agent() {
        let mut event = get_event_with_user_agent("a dont no");
        normalize_user_agent(&mut event);
        assert!(event.contexts.value().unwrap().0.is_empty());
    }

    #[test]
    fn test_browser_context() {
        let ua = "Mozilla/5.0 (-; -; -) - Chrome/18.0.1025.133 Mobile Safari/535.19";

        let mut event = get_event_with_user_agent(ua);
        normalize_user_agent(&mut event);
        assert_annotated_snapshot!(event.contexts, @r#"
        {
          "browser": {
            "name": "Chrome Mobile",
            "version": "18.0.1025",
            "type": "browser"
          }
        }
        "#);
    }

    #[test]
    fn test_os_context() {
        let ua = "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) - -";

        let mut event = get_event_with_user_agent(ua);
        normalize_user_agent(&mut event);
        assert_annotated_snapshot!(event.contexts, @r#"
        {
          "client_os": {
            "name": "Windows",
            "version": "7",
            "type": "os"
          }
        }
        "#);
    }

    #[test]
    fn test_os_context_short_version() {
        let ua = "Mozilla/5.0 (iPhone; CPU iPhone OS 12_1 like Mac OS X) - (-)";
        let mut event = get_event_with_user_agent(ua);
        normalize_user_agent(&mut event);
        assert_annotated_snapshot!(event.contexts, @r#"
        {
          "browser": {
            "name": "Mobile Safari UI/WKWebView",
            "type": "browser"
          },
          "client_os": {
            "name": "iOS",
            "version": "12.1",
            "type": "os"
          },
          "device": {
            "family": "iPhone",
            "model": "iPhone",
            "brand": "Apple",
            "type": "device"
          }
        }
        "#);
    }

    #[test]
    fn test_os_context_full_version() {
        let ua = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_4) - (-)";
        let mut event = get_event_with_user_agent(ua);
        normalize_user_agent(&mut event);
        assert_annotated_snapshot!(event.contexts, @r#"
        {
          "client_os": {
            "name": "Mac OS X",
            "version": "10.13.4",
            "type": "os"
          },
          "device": {
            "family": "Mac",
            "model": "Mac",
            "brand": "Apple",
            "type": "device"
          }
        }
        "#);
    }

    #[test]
    fn test_device_context() {
        let ua = "- (-; -; Galaxy Nexus Build/IMM76B) - (-) ";

        let mut event = get_event_with_user_agent(ua);
        normalize_user_agent(&mut event);
        assert_annotated_snapshot!(event.contexts, @r#"
        {
          "device": {
            "family": "Samsung Galaxy Nexus",
            "model": "Galaxy Nexus",
            "brand": "Samsung",
            "type": "device"
          }
        }
        "#);
    }

    #[test]
    fn test_all_contexts() {
        let mut event = get_event_with_user_agent(GOOD_UA);
        normalize_user_agent(&mut event);
        assert_annotated_snapshot!(event.contexts, @r#"
        {
          "browser": {
            "name": "Chrome Mobile",
            "version": "18.0.1025",
            "type": "browser"
          },
          "client_os": {
            "name": "Android",
            "version": "4.0.4",
            "type": "os"
          },
          "device": {
            "family": "Samsung Galaxy Nexus",
            "model": "Galaxy Nexus",
            "brand": "Samsung",
            "type": "device"
          }
        }
        "#);
    }

    #[test]
    fn test_user_agent_does_not_override_prefilled() {
        let mut event = get_event_with_user_agent(GOOD_UA);
        let mut contexts = Contexts::new();
        contexts.add(BrowserContext {
            name: Annotated::from("BR_FAMILY".to_string()),
            version: Annotated::from("BR_VERSION".to_string()),
            ..BrowserContext::default()
        });
        contexts.add(DeviceContext {
            family: Annotated::from("DEV_FAMILY".to_string()),
            model: Annotated::from("DEV_MODEL".to_string()),
            brand: Annotated::from("DEV_BRAND".to_string()),
            ..DeviceContext::default()
        });
        contexts.add(OsContext {
            name: Annotated::from("OS_FAMILY".to_string()),
            version: Annotated::from("OS_VERSION".to_string()),
            ..OsContext::default()
        });

        event.contexts = Annotated::new(contexts);

        normalize_user_agent(&mut event);
        assert_annotated_snapshot!(event.contexts, @r#"
        {
          "browser": {
            "name": "BR_FAMILY",
            "version": "BR_VERSION",
            "type": "browser"
          },
          "client_os": {
            "name": "Android",
            "version": "4.0.4",
            "type": "os"
          },
          "device": {
            "family": "DEV_FAMILY",
            "model": "DEV_MODEL",
            "brand": "DEV_BRAND",
            "type": "device"
          },
          "os": {
            "name": "OS_FAMILY",
            "version": "OS_VERSION",
            "type": "os"
          }
        }
        "#);
    }

    #[test]
    fn test_fallback_to_ua_if_no_client_hints() {
        let headers = Headers([
            Annotated::new((
                Annotated::new("user-agent".to_string().into()),
                Annotated::new(r#""Mozilla/5.0 (Linux; Android 11; foo g31(w)) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Mobile Safari/537.36""#.to_string().into()),
            )),
            Annotated::new((
                Annotated::new("invalid header".to_string().into()),
                Annotated::new("moto g31(w)".to_string().into()),
            )),
        ].into_iter().collect());

        let device = DeviceContext::from_hints_or_ua(&RawUserAgentInfo::from_headers(&headers));
        assert_eq!(device.unwrap().family.as_str().unwrap(), "foo g31(w)");
    }
    #[test]
    fn test_use_client_hints_for_device() {
        let headers = Headers([
            Annotated::new((
                Annotated::new("user-agent".to_string().into()),
                Annotated::new(r#""Mozilla/5.0 (Linux; Android 11; foo g31(w)) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Mobile Safari/537.36""#.to_string().into()),
            )),
            Annotated::new((
                Annotated::new("SEC-CH-UA-MODEL".to_string().into()),
                Annotated::new("moto g31(w)".to_string().into()),
            )),
        ].into_iter().collect());

        let device = DeviceContext::from_hints_or_ua(&RawUserAgentInfo::from_headers(&headers));
        assert_eq!(device.unwrap().model.as_str().unwrap(), "moto g31(w)");
    }

    #[test]
    fn test_strip_whitespace_and_quotes() {
        let headers = Headers(
            [Annotated::new((
                Annotated::new("SEC-CH-UA-MODEL".to_string().into()),
                Annotated::new("   \"moto g31(w)\"".to_string().into()),
            ))]
            .into_iter()
            .collect(),
        );

        let device = DeviceContext::from_hints_or_ua(&RawUserAgentInfo::from_headers(&headers));
        assert_eq!(device.unwrap().model.as_str().unwrap(), "moto g31(w)");
    }

    #[test]
    fn test_ignore_empty_device() {
        let headers = Headers(
            [Annotated::new((
                Annotated::new("SEC-CH-UA-MODEL".to_string().into()),
                Annotated::new("".to_string().into()),
            ))]
            .into_iter()
            .collect(),
        );

        let client_hints = RawUserAgentInfo::from_headers(&headers).client_hints;
        let from_hints = DeviceContext::parse_client_hints(&client_hints);
        assert!(from_hints.is_none())
    }

    #[test]
    fn test_client_hint_parser() {
        let chrome = browser_from_client_hints(
            r#"Not_A Brand";v="99", "Google Chrome";v="109", "Chromium";v="109"#,
        )
        .unwrap();
        assert_eq!(chrome.0, "Google Chrome".to_owned());
        assert_eq!(chrome.1, "109".to_owned());

        let opera = browser_from_client_hints(
            r#""Chromium";v="108", "Opera";v="94", "Not)A;Brand";v="99""#,
        )
        .unwrap();
        assert_eq!(opera.0, "Opera".to_owned());
        assert_eq!(opera.1, "94".to_owned());

        let mystery_browser = browser_from_client_hints(
            r#""Chromium";v="108", "mystery-browser";v="94", "Not)A;Brand";v="99""#,
        )
        .unwrap();

        assert_eq!(mystery_browser.0, "mystery-browser".to_owned());
        assert_eq!(mystery_browser.1, "94".to_owned());
    }

    #[test]
    fn test_client_hints_detected() {
        let headers = Headers({
            let headers = vec![
            Annotated::new((
                Annotated::new("user-agent".to_string().into()),
                Annotated::new(r#"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36"#.to_string().into()),
            )),
            Annotated::new((
                Annotated::new("SEC-CH-UA".to_string().into()),
                Annotated::new(r#"Not_A Brand";v="99", "Google Chrome";v="109", "Chromium";v="109"#.to_string().into()),
            )),
        ];
            PairList(headers)
        });

        let browser =
            BrowserContext::from_hints_or_ua(&RawUserAgentInfo::from_headers(&headers)).unwrap();

        insta::assert_debug_snapshot!(browser, @r#"
        BrowserContext {
            name: "Google Chrome",
            version: "109",
            other: {},
        }
        "#);
    }

    #[test]
    fn test_ignore_empty_browser() {
        let headers = Headers({
            let headers = vec![Annotated::new((
                Annotated::new("SEC-CH-UA".to_string().into()),
                Annotated::new(
                    // browser field missing
                    r#"Not_A Brand";v="99", " ";v="109", "Chromium";v="109"#
                        .to_string()
                        .into(),
                ),
            ))];
            PairList(headers)
        });

        let client_hints = RawUserAgentInfo::from_headers(&headers).client_hints;
        let from_hints = BrowserContext::parse_client_hints(&client_hints);
        assert!(from_hints.is_none())
    }

    #[test]
    fn test_client_hints_with_unknown_browser() {
        let headers = Headers({
            let headers = vec![
            Annotated::new((
                Annotated::new("user-agent".to_string().into()),
                Annotated::new(r#"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36"#.to_string().into()),
            )),
            Annotated::new((
                Annotated::new("SEC-CH-UA".to_string().into()),
                Annotated::new(r#"Not_A Brand";v="99", "weird browser";v="109", "Chromium";v="109"#.to_string().into()),
            )),
            Annotated::new((
                Annotated::new("SEC-CH-UA-FULL-VERSION".to_string().into()),
                Annotated::new("109.0.5414.87".to_string().into()),
            )),
        ];
            PairList(headers)
        });

        let browser =
            BrowserContext::from_hints_or_ua(&RawUserAgentInfo::from_headers(&headers)).unwrap();

        insta::assert_debug_snapshot!(browser, @r#"
        BrowserContext {
            name: "weird browser",
            version: "109",
            other: {},
        }
        "#);
    }

    #[test]
    fn fallback_on_ua_string_when_missing_browser_field() {
        let headers = Headers({
            let headers = vec![
            Annotated::new((
                Annotated::new("user-agent".to_string().into()),
                Annotated::new(r#"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36"#.to_string().into()),
            )),
            Annotated::new((
                Annotated::new("SEC-CH-UA".to_string().into()),
                Annotated::new(r#"Not_A Brand";v="99", "Chromium";v="108"#.to_string().into()), // no browser field here
            )),
        ];
            PairList(headers)
        });

        let browser =
            BrowserContext::from_hints_or_ua(&RawUserAgentInfo::from_headers(&headers)).unwrap();
        assert_eq!(
            browser.version.as_str().unwrap(),
            "109.0.0" // notice the version number is from UA string not from client hints
        );

        assert_eq!("Chrome", browser.name.as_str().unwrap());
    }

    #[test]
    fn test_strip_quotes() {
        let headers = Headers({
            let headers = vec![
                Annotated::new((
                    Annotated::new("SEC-CH-UA-PLATFORM".to_string().into()),
                    Annotated::new("\"macOS\"".to_string().into()), // no browser field here
                )),
                Annotated::new((
                    Annotated::new("SEC-CH-UA-PLATFORM-VERSION".to_string().into()),
                    Annotated::new("\"13.1.0\"".to_string().into()),
                )),
            ];
            PairList(headers)
        });
        let os = OsContext::from_hints_or_ua(&RawUserAgentInfo::from_headers(&headers));

        assert_eq!(os.clone().unwrap().name.value().unwrap(), "macOS");
        assert_eq!(os.unwrap().version.value().unwrap(), "13.1.0");
    }

    /// Verifies that client hints are chosen over ua string when available.
    #[test]
    fn test_choose_client_hints_for_os_context() {
        let headers = Headers({
            let headers = vec![
            Annotated::new((
                Annotated::new("user-agent".to_string().into()),
                Annotated::new(r#"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36"#.to_string().into()),
            )),
            Annotated::new((
                Annotated::new("SEC-CH-UA-PLATFORM".to_string().into()),
                Annotated::new(r#"macOS"#.to_string().into()), // no browser field here
            )),
            Annotated::new((
                Annotated::new("SEC-CH-UA-PLATFORM-VERSION".to_string().into()),
                Annotated::new("13.1.0".to_string().into()),
            )),
        ];
            PairList(headers)
        });

        let os = OsContext::from_hints_or_ua(&RawUserAgentInfo::from_headers(&headers)).unwrap();

        insta::assert_debug_snapshot!(os, @r#"
OsContext {
    name: "macOS",
    version: "13.1.0",
    build: ~,
    kernel_version: ~,
    rooted: ~,
    raw_description: ~,
    other: {},
}
        "#);
    }

    #[test]
    fn test_ignore_empty_os() {
        let headers = Headers({
            let headers = vec![Annotated::new((
                Annotated::new("SEC-CH-UA-PLATFORM".to_string().into()),
                Annotated::new(r#""#.to_string().into()),
            ))];
            PairList(headers)
        });

        let client_hints = RawUserAgentInfo::from_headers(&headers).client_hints;
        let from_hints = OsContext::parse_client_hints(&client_hints);
        assert!(from_hints.is_none())
    }

    #[test]
    fn test_keep_empty_os_version() {
        let headers = Headers({
            let headers = vec![
                Annotated::new((
                    Annotated::new("SEC-CH-UA-PLATFORM".to_string().into()),
                    Annotated::new(r#"macOs"#.to_string().into()),
                )),
                Annotated::new((
                    Annotated::new("SEC-CH-UA-PLATFORM-VERSION".to_string().into()),
                    Annotated::new("".to_string().into()),
                )),
            ];
            PairList(headers)
        });

        let client_hints = RawUserAgentInfo::from_headers(&headers).client_hints;
        let from_hints = OsContext::parse_client_hints(&client_hints);
        assert!(from_hints.is_some())
    }

    #[test]
    fn test_fallback_on_ua_string_for_os() {
        let headers = Headers({
            let headers = vec![
            Annotated::new((
                Annotated::new("user-agent".to_string().into()),
                Annotated::new(r#"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36"#.to_string().into()),
            )),
            Annotated::new((
                Annotated::new("invalid header".to_string().into()),
                Annotated::new(r#"macOS"#.to_string().into()),
            )),
            Annotated::new((
                Annotated::new("SEC-CH-UA-PLATFORM-VERSION".to_string().into()),
                Annotated::new("13.1.0".to_string().into()),
            )),
        ];
            PairList(headers)
        });

        let os = OsContext::from_hints_or_ua(&RawUserAgentInfo::from_headers(&headers)).unwrap();

        insta::assert_debug_snapshot!(os, @r#"
OsContext {
    name: "Mac OS X",
    version: "10.15.6",
    build: ~,
    kernel_version: ~,
    rooted: ~,
    raw_description: ~,
    other: {},
}
        "#);
    }

    #[test]
    fn test_indicate_frozen_os_windows() {
        let headers = Headers({
            let headers = vec![
            Annotated::new((
                Annotated::new("user-agent".to_string().into()),
                Annotated::new(r#"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36"#.to_string().into()),
            )),
        ];
            PairList(headers)
        });

        let os = OsContext::from_hints_or_ua(&RawUserAgentInfo::from_headers(&headers)).unwrap();

        // Checks that the '>=' prefix is added.
        assert_eq!(os.version.value().unwrap(), ">=10");
    }

    #[test]
    fn test_indicate_frozen_os_mac() {
        let headers = Headers({
            let headers = vec![
            Annotated::new((
                Annotated::new("user-agent".to_string().into()),
                Annotated::new(r#"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36"#.to_string().into()),
            )),
        ];
            PairList(headers)
        });

        let os = OsContext::from_hints_or_ua(&RawUserAgentInfo::from_headers(&headers)).unwrap();

        // Checks that the '>=' prefix is added.
        assert_eq!(os.version.value().unwrap(), ">=10.15.7");
    }

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
