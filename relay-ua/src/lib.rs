//! User agent parser with built-in rules.
//!
//! # Test Performance
//!
//! Adding user agent parsing to your module will incur a latency penalty on first use. Because of
//! this, integration tests could fail. To fix this, you will need to add a timeout to your
//! consumer.

use std::borrow::Cow;
use std::cell::RefCell;
use std::sync::LazyLock;

use uaparser::{Parser, UserAgentParser};

#[doc(inline)]
pub use uaparser::{Device, OS, UserAgent};

/// The global [`UserAgentParser`] already configured with a user agent database.
///
/// For usage, see [`Parser`].
static UA_PARSER: LazyLock<UserAgentParser> = LazyLock::new(|| {
    let ua_regexes = include_bytes!("../uap-core/regexes.yaml");
    UserAgentParser::builder()
        .with_unicode_support(false)
        .build_from_bytes(ua_regexes)
        .expect("Could not create UserAgent. You are probably using a bad build of relay.")
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
    // The same user agent is often parsed many times in a row: once for every log, span or trace
    // metric in a container, and once more by inbound filters after normalization. A parse runs
    // hundreds of regexes, so each thread remembers its last result.
    thread_local! {
        static LAST: RefCell<Option<(String, UserAgent<'static>)>> = const { RefCell::new(None) };
    }

    LAST.with_borrow_mut(|last| {
        if let Some((raw, parsed)) = last
            && raw == user_agent
        {
            return parsed.clone();
        }

        let parsed = UA_PARSER.parse_user_agent(user_agent);
        let owned = UserAgent {
            family: Cow::Owned(parsed.family.into_owned()),
            major: parsed.major.map(|v| Cow::Owned(v.into_owned())),
            minor: parsed.minor.map(|v| Cow::Owned(v.into_owned())),
            patch: parsed.patch.map(|v| Cow::Owned(v.into_owned())),
        };
        *last = Some((user_agent.to_owned(), owned.clone()));
        owned
    })
}

/// Returns the family, brand, and model of the device of the requesting client.
///
/// Defaults to an empty device.
pub fn parse_device(user_agent: &str) -> Device<'_> {
    UA_PARSER.parse_device(user_agent)
}

/// Returns the family and version of the operating system of the requesting client.
///
/// Defaults to an empty operating system.
pub fn parse_os(user_agent: &str) -> OS<'_> {
    UA_PARSER.parse_os(user_agent)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_user_agent_repeated() {
        let chrome = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36";
        let firefox = "Mozilla/5.0 (X11; Linux x86_64; rv:131.0) Gecko/20100101 Firefox/131.0";

        for ua in [chrome, chrome, firefox, chrome, "", "", firefox] {
            assert_eq!(
                parse_user_agent(ua),
                UA_PARSER.parse_user_agent(ua),
                "input: {ua:?}"
            );
        }
    }
}
