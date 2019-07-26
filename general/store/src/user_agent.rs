use lazy_static::lazy_static;
use uaparser;
use uaparser::UserAgentParser;

/// Builds a [`UserAgentParser`]: https://docs.rs/uap-rs/0.2.2/uap_rs/struct.UserAgentParser.html
/// already configured with a user agent database.
///
/// For usage see [`Parser`]: https://docs.rs/uap-rs/0.2.2/uap_rs/trait.Parser.html
///

lazy_static! {
    pub static ref UA_PARSER: UserAgentParser = {
        let ua_regexes = include_bytes!("../uap-core/regexes.yaml");
        UserAgentParser::from_bytes(ua_regexes).expect(
            "Could not create UserAgent. \
             You are probably using a bad build of 'semaphore-common'. ",
        )
    };
}
