//! User agent parser with built-in rules.
//!
//! # Test Performance
//!
//! Adding user agent parsing to your module will incur a latency penalty on first use. Because of
//! this, integration tests could fail. To fix this, you will need to add a timeout to your
//! consumer.

use std::borrow::Cow;
use std::sync::LazyLock;

use ua_parser::{Extractor, Regexes};

/// The family used when a user agent, device, or operating system could not be detected.
const UNKNOWN_FAMILY: &str = "Other";

/// Describes the family as well as the major, minor, and patch versions of a user agent client.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct UserAgent<'a> {
    /// The family of the user agent client, for example `Chrome`.
    pub family: Cow<'a, str>,
    /// The major version of the user agent client.
    pub major: Option<Cow<'a, str>>,
    /// The minor version of the user agent client.
    pub minor: Option<Cow<'a, str>>,
    /// The patch version of the user agent client.
    pub patch: Option<Cow<'a, str>>,
}

impl Default for UserAgent<'_> {
    fn default() -> Self {
        Self {
            family: Cow::Borrowed(UNKNOWN_FAMILY),
            major: None,
            minor: None,
            patch: None,
        }
    }
}

/// Describes the family as well as the major, minor, patch, and patch minor versions of an
/// operating system.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct OS<'a> {
    /// The family of the operating system, for example `Windows`.
    pub family: Cow<'a, str>,
    /// The major version of the operating system.
    pub major: Option<Cow<'a, str>>,
    /// The minor version of the operating system.
    pub minor: Option<Cow<'a, str>>,
    /// The patch version of the operating system.
    pub patch: Option<Cow<'a, str>>,
    /// The patch minor version of the operating system.
    pub patch_minor: Option<Cow<'a, str>>,
}

impl Default for OS<'_> {
    fn default() -> Self {
        Self {
            family: Cow::Borrowed(UNKNOWN_FAMILY),
            major: None,
            minor: None,
            patch: None,
            patch_minor: None,
        }
    }
}

/// Describes the family, brand, and model of a device.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Device<'a> {
    /// The family of the device, for example `iPhone`.
    pub family: Cow<'a, str>,
    /// The brand of the device, for example `Apple`.
    pub brand: Option<Cow<'a, str>>,
    /// The model of the device.
    pub model: Option<Cow<'a, str>>,
}

impl Default for Device<'_> {
    fn default() -> Self {
        Self {
            family: Cow::Borrowed(UNKNOWN_FAMILY),
            brand: None,
            model: None,
        }
    }
}

/// A [`Extractor`] already configured with the user agent database.
static UA_PARSER: LazyLock<Extractor<'static>> = LazyLock::new(|| {
    let ua_regexes = include_bytes!("../uap-core/regexes.yaml");
    let regexes: Regexes<'_> =
        serde_yaml::from_slice(ua_regexes).expect("user agent regexes to be valid");
    Extractor::try_from(regexes).expect("user agent regexes to be valid")
});

/// Initializes the user agent parser.
///
/// This loads and compiles user agent patterns, which takes a few seconds to complete. The user
/// agent parser initializes on-demand when using one of the parse methods. This function forces
/// initialization at a convenient point without introducing unwanted delays.
pub fn init_parser() {
    LazyLock::force(&UA_PARSER);
}

/// Returns the family and version of a user agent client.
///
/// Defaults to an empty user agent.
pub fn parse_user_agent(user_agent: &str) -> UserAgent<'_> {
    let Some(value) = UA_PARSER.ua.extract(user_agent) else {
        return UserAgent::default();
    };

    UserAgent {
        family: family_or_unknown(value.family),
        major: non_empty(value.major.map(Cow::Borrowed)),
        minor: non_empty(value.minor.map(Cow::Borrowed)),
        patch: non_empty(value.patch.map(Cow::Borrowed)),
    }
}

/// Returns the family, brand, and model of the device of the requesting client.
///
/// Defaults to an empty device.
pub fn parse_device(user_agent: &str) -> Device<'_> {
    let Some(value) = UA_PARSER.dev.extract(user_agent) else {
        return Device::default();
    };

    Device {
        family: family_or_unknown(value.device),
        brand: non_empty(value.brand),
        model: non_empty(value.model),
    }
}

/// Returns the family and version of the operating system of the requesting client.
///
/// Defaults to an empty operating system.
pub fn parse_os(user_agent: &str) -> OS<'_> {
    let Some(value) = UA_PARSER.os.extract(user_agent) else {
        return OS::default();
    };

    OS {
        family: family_or_unknown(value.os),
        major: non_empty(value.major),
        minor: non_empty(value.minor),
        patch: non_empty(value.patch),
        patch_minor: non_empty(value.patch_minor),
    }
}

fn family_or_unknown(family: Cow<'_, str>) -> Cow<'_, str> {
    match family.is_empty() {
        true => Cow::Borrowed(UNKNOWN_FAMILY),
        false => family,
    }
}

fn non_empty(value: Option<Cow<'_, str>>) -> Option<Cow<'_, str>> {
    value.filter(|value| !value.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    const FIREFOX_ON_MAC: &str =
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:160.0) Gecko/20100101 Firefox/160.0";
    const IPHONE: &str = "Mozilla/5.0 (iPhone; CPU iPhone OS 17_1_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1.2 Mobile/15E148 Safari/604.1";

    #[test]
    fn test_parse_firefox_on_mac() {
        insta::assert_debug_snapshot!(parse_user_agent(FIREFOX_ON_MAC), @r#"
        UserAgent {
            family: "Firefox",
            major: Some(
                "160",
            ),
            minor: Some(
                "0",
            ),
            patch: None,
        }
        "#);
        insta::assert_debug_snapshot!(parse_os(FIREFOX_ON_MAC), @r#"
        OS {
            family: "Mac OS X",
            major: Some(
                "10",
            ),
            minor: Some(
                "15",
            ),
            patch: None,
            patch_minor: None,
        }
        "#);
        insta::assert_debug_snapshot!(parse_device(FIREFOX_ON_MAC), @r#"
        Device {
            family: "Mac",
            brand: Some(
                "Apple",
            ),
            model: Some(
                "Mac",
            ),
        }
        "#);
    }

    #[test]
    fn test_parse_iphone() {
        insta::assert_debug_snapshot!(parse_user_agent(IPHONE), @r#"
        UserAgent {
            family: "Mobile Safari",
            major: Some(
                "17",
            ),
            minor: Some(
                "1",
            ),
            patch: Some(
                "2",
            ),
        }
        "#);
        insta::assert_debug_snapshot!(parse_os(IPHONE), @r#"
        OS {
            family: "iOS",
            major: Some(
                "17",
            ),
            minor: Some(
                "1",
            ),
            patch: Some(
                "2",
            ),
            patch_minor: None,
        }
        "#);
        insta::assert_debug_snapshot!(parse_device(IPHONE), @r#"
        Device {
            family: "iPhone",
            brand: Some(
                "Apple",
            ),
            model: Some(
                "iPhone",
            ),
        }
        "#);
    }

    #[test]
    fn test_parse_unknown_user_agent() {
        let user_agent = "some user agent";

        assert_eq!(parse_user_agent(user_agent), Default::default());
        assert_eq!(parse_os(user_agent), Default::default());
        assert_eq!(parse_device(user_agent), Default::default());

        insta::assert_debug_snapshot!(parse_user_agent(user_agent), @r#"
        UserAgent {
            family: "Other",
            major: None,
            minor: None,
            patch: None,
        }
        "#);
        insta::assert_debug_snapshot!(parse_os(user_agent), @r#"
        OS {
            family: "Other",
            major: None,
            minor: None,
            patch: None,
            patch_minor: None,
        }
        "#);
        insta::assert_debug_snapshot!(parse_device(user_agent), @r#"
        Device {
            family: "Other",
            brand: None,
            model: None,
        }
        "#);
    }
}
