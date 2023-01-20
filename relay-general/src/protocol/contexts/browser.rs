use crate::protocol::ContextFromUserAgentInfo;
use crate::store::user_agent::{get_version, is_known};
use crate::types::{Annotated, Object, Value};
use crate::user_agent::{parse_user_agent, RawUserAgentInfo};

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

impl BrowserContext {
    /// The key under which a browser context is generally stored (in `Contexts`)
    pub fn default_key() -> &'static str {
        "browser"
    }
}

impl ContextFromUserAgentInfo for BrowserContext {
    fn from_client_hints(raw_contexts: &RawUserAgentInfo) -> Option<Self> {
        let browser = parse_client_hint_browser(raw_contexts.sec_ch_ua?)?;
        let version = raw_contexts.sec_ch_ua_full_version?.to_owned();

        Some(Self {
            name: Annotated::new(browser.to_string()),
            version: Annotated::new(version),
            ..Default::default()
        })
    }

    fn from_user_agent(user_agent: &str) -> Option<Self> {
        let browser = parse_user_agent(user_agent);

        if !is_known(browser.family.as_str()) {
            return None;
        }

        Some(Self {
            name: Annotated::from(browser.family),
            version: Annotated::from(get_version(&browser.major, &browser.minor, &browser.patch)),
            ..BrowserContext::default()
        })
    }
}

#[derive(Debug, PartialEq)]
enum Browser {
    Chrome,
    Edge,
    Safari,
    Brave,
    Opera,
    Firefox,
    Other(String),
}

impl Browser {
    fn as_str(&self) -> &str {
        match self {
            Self::Chrome => "chrome",
            Self::Edge => "edge",
            Self::Firefox => "firefox",
            Self::Safari => "safari",
            Self::Brave => "brave",
            Self::Opera => "opera",
            Self::Other(s) => s,
        }
    }
}

impl std::string::ToString for Browser {
    fn to_string(&self) -> String {
        self.as_str().to_owned()
    }
}

/// the sec-ch-ua field looks something like this:
/// "Not_A Brand";v="99", "Google Chrome";v="109", "Chromium";v="109"
///
/// this function attempts to detect the field with the browser, parse it as a browser enum and
/// return that. if it fails to parse it as a browser, it uses the "other" variant of browser enum
///
/// returns None if no browser field detected
fn parse_client_hint_browser<S: Into<String>>(s: S) -> Option<Browser> {
    let s = s.into();
    use Browser::*;

    let items: Vec<&str> = s.split(',').collect();
    let mut browser = String::new();
    let mut browser_detected = false;

    for item in items {
        let item = item.to_lowercase();

        // if it contains one of these then we can know it isn't a browser field. atm chromium
        // browsers are the only ones supporting client hints.
        if item.contains("brand")
            || item.contains("chromium")
            || item.contains("gecko") // useless until firefox and safari support client hints
            || item.contains("webkit")
        {
            continue;
        }

        if item.contains(Chrome.as_str()) {
            return Some(Browser::Chrome);
        } else if item.contains(Opera.as_str()) {
            return Some(Browser::Opera);
        } else if item.contains(Edge.as_str()) {
            return Some(Browser::Edge);
        } else if item.contains(Firefox.as_str()) {
            return Some(Browser::Firefox);
        } else if item.contains(Safari.as_str()) {
            return Some(Browser::Safari);
        } else if item.contains(Brave.as_str()) {
            return Some(Browser::Brave);
        }
        browser = item;
        browser_detected = true;
        break;
    }

    if !browser_detected {
        return None;
    }

    // "\"foo-browser\";v=\"109\"" -> "foo-browser"
    let cleaned: String = browser
        .split(';')
        .take(1)
        .map(|s| s.trim().to_owned().replace('\"', ""))
        .collect();

    Some(Browser::Other(cleaned))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{Headers, PairList};

    #[test]
    fn test_client_hint_parser() {
        let chrome = parse_client_hint_browser(
            r#"Not_A Brand";v="99", "Google Chrome";v="109", "Chromium";v="109"#,
        );
        assert_eq!(chrome.unwrap(), Browser::Chrome);

        let opera = parse_client_hint_browser(
            r#""Chromium";v="108", "Opera";v="94", "Not)A;Brand";v="99""#,
        );
        assert_eq!(opera.unwrap(), Browser::Opera);

        let mystery_browser = parse_client_hint_browser(
            r#""Chromium";v="108", "mystery-browser";v="94", "Not)A;Brand";v="99""#,
        );

        assert_eq!(
            mystery_browser.unwrap(),
            Browser::Other("mystery-browser".to_owned())
        );

        let missing_browser =
            parse_client_hint_browser(r#""Chromium";v="108", "Not)A;Brand";v="99""#);
        assert!(missing_browser.is_none());
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
            Annotated::new((
                Annotated::new("SEC-CH-UA-FULL-VERSION".to_string().into()),
                Annotated::new("109.0.5414.87".to_string().into()),
            )),
        ];
            PairList(headers)
        });

        let browser = BrowserContext::from_hints_or_ua(&RawUserAgentInfo::new(&headers));
        assert_eq!(
            browser.clone().unwrap().version.as_str().unwrap(),
            "109.0.5414.87"
        );

        assert_eq!(
            Browser::Chrome.as_str(),
            browser.unwrap().name.as_str().unwrap()
        );
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

        let browser = BrowserContext::from_hints_or_ua(&RawUserAgentInfo::new(&headers));
        assert_eq!(
            browser.clone().unwrap().version.as_str().unwrap(),
            "109.0.5414.87"
        );

        assert_eq!("weird browser", browser.unwrap().name.as_str().unwrap());
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
                Annotated::new(r#"Not_A Brand";v="99", "Chromium";v="109"#.to_string().into()), // no browser field here
            )),
            Annotated::new((
                Annotated::new("SEC-CH-UA-FULL-VERSION".to_string().into()),
                Annotated::new("109.0.5414.87".to_string().into()),
            )),
        ];
            PairList(headers)
        });

        let browser = BrowserContext::from_hints_or_ua(&RawUserAgentInfo::new(&headers));
        assert_eq!(
            browser.clone().unwrap().version.as_str().unwrap(),
            "109.0.0" // notice the version number is from UA string not from client hints
        );

        assert_eq!("Chrome", browser.unwrap().name.as_str().unwrap());
    }
}
