//! User agent parser with built-in rules.
//!
//! # Test Performance
//!
//! Adding user agent parsing to your module will incur a latency penalty on first use. Because of
//! this, integration tests could fail. To fix this, you will need to add a timeout to your
//! consumer.

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
    UA_PARSER.parse_user_agent(user_agent)
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
