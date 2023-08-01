use once_cell::sync::OnceCell;
use regex::Regex;

use crate::protocol::FromUserAgentInfo;
use crate::types::{Annotated, Object, Value};
use crate::{store, user_agent};

/// Web browser information.
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct BrowserContext {
    /// Display name of the browser application.
    pub name: Annotated<String>,

    /// Version string of the browser.
    pub version: Annotated<String>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties, retain = "true", pii = "maybe")]
    pub other: Object<Value>,
}

impl super::DefaultContext for BrowserContext {
    fn default_key() -> &'static str {
        "browser"
    }

    fn from_context(context: super::Context) -> Option<Self> {
        match context {
            super::Context::Browser(c) => Some(*c),
            _ => None,
        }
    }

    fn cast(context: &super::Context) -> Option<&Self> {
        match context {
            super::Context::Browser(c) => Some(c),
            _ => None,
        }
    }

    fn cast_mut(context: &mut super::Context) -> Option<&mut Self> {
        match context {
            super::Context::Browser(c) => Some(c),
            _ => None,
        }
    }

    fn into_context(self) -> super::Context {
        super::Context::Browser(Box::new(self))
    }
}

impl FromUserAgentInfo for BrowserContext {
    fn parse_client_hints(client_hints: &user_agent::ClientHints<&str>) -> Option<Self> {
        let (browser, version) = browser_from_client_hints(client_hints.sec_ch_ua?)?;

        Some(Self {
            name: Annotated::new(browser),
            version: Annotated::new(version),
            ..Default::default()
        })
    }

    fn parse_user_agent(user_agent: &str) -> Option<Self> {
        let browser = user_agent::parse_user_agent(user_agent);

        if !store::user_agent::is_known(&browser.family) {
            return None;
        }

        Some(Self {
            name: Annotated::from(browser.family.into_owned()),
            version: Annotated::from(store::user_agent::get_version(
                &browser.major,
                &browser.minor,
                &browser.patch,
            )),
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

#[cfg(test)]
mod tests {
    use insta::assert_debug_snapshot;

    use super::*;
    use crate::protocol::{Headers, PairList};
    use crate::user_agent::RawUserAgentInfo;

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

        assert_debug_snapshot!(browser, @r#"
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

        assert_debug_snapshot!(browser, @r#"
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
}
