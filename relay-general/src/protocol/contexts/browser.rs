use crate::protocol::FromUserAgentInfo;
use crate::store;
use crate::types::{Annotated, Object, Value};
use crate::user_agent;

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

impl FromUserAgentInfo for BrowserContext {
    fn from_client_hints(client_hints: &user_agent::ClientHints) -> Option<Self> {
        let browser = Browser::from_ua_client_hint(client_hints.sec_ch_ua?)?;
        let version = client_hints.sec_ch_ua_full_version?.to_owned();

        Some(Self {
            name: Annotated::new(browser.to_string()),
            version: Annotated::new(version),
            ..Default::default()
        })
    }

    fn from_user_agent(user_agent: &str) -> Option<Self> {
        let browser = user_agent::parse_user_agent(user_agent);

        if !store::user_agent::is_known(&browser.family) {
            return None;
        }

        Some(Self {
            name: Annotated::from(browser.family),
            version: Annotated::from(store::user_agent::get_version(
                &browser.major,
                &browser.minor,
                &browser.patch,
            )),
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

impl std::convert::From<String> for Browser {
    fn from(s: String) -> Self {
        match s.as_str() {
            "Google Chrome" => Browser::Chrome,
            "Microsoft Edge" => Browser::Edge,
            "Firefox" => Browser::Firefox,
            "Safari" => Browser::Safari,
            "Brave" => Browser::Brave,
            "Opera" => Browser::Opera,
            _ => Browser::Other(s.to_string()),
        }
    }
}

impl std::string::ToString for Browser {
    fn to_string(&self) -> String {
        match self {
            Browser::Chrome => "Google Chrome".to_string(),
            Browser::Edge => "Microsoft Edge".to_string(),
            Browser::Firefox => "Firefox".to_string(),
            Browser::Safari => "Safari".to_string(),
            Browser::Brave => "Brave".to_string(),
            Browser::Opera => "Opera".to_string(),
            Browser::Other(s) => s.clone(),
        }
    }
}

impl Browser {
    /// the sec-ch-ua field looks something like this:
    /// "Not_A Brand";v="99", "Google Chrome";v="109", "Chromium";v="109"
    ///
    /// this function attempts to detect the field with the browser, parse it as a browser enum and
    /// return that. if it fails to parse it as a browser, it uses the "other" variant of browser enum
    ///
    /// returns None if no browser field detected
    fn from_ua_client_hint(s: &str) -> Option<Self> {
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

            // "\"foo-browser\";v=\"109\"" -> "foo-browser"
            let browser: String = item
                .split(';')
                .take(1)
                .map(|s| s.trim().to_owned().replace('\"', ""))
                .collect();

            return Some(Self::from(browser));
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{Headers, PairList};
    use crate::user_agent::RawUserAgentInfo;

    #[test]
    fn test_client_hint_parser() {
        let chrome = Browser::from_ua_client_hint(
            r#"Not_A Brand";v="99", "Google Chrome";v="109", "Chromium";v="109"#,
        );
        assert_eq!(chrome.unwrap(), Browser::Chrome);

        let opera = Browser::from_ua_client_hint(
            r#""Chromium";v="108", "Opera";v="94", "Not)A;Brand";v="99""#,
        );
        assert_eq!(opera.unwrap(), Browser::Opera);

        let mystery_browser = Browser::from_ua_client_hint(
            r#""Chromium";v="108", "mystery-browser";v="94", "Not)A;Brand";v="99""#,
        );

        assert_eq!(
            mystery_browser.unwrap(),
            Browser::Other("mystery-browser".to_owned())
        );

        let missing_browser =
            Browser::from_ua_client_hint(r#""Chromium";v="108", "Not)A;Brand";v="99""#);
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
            Browser::Chrome.to_string(),
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
