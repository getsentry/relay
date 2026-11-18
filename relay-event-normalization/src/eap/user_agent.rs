use std::cell::OnceCell;

use relay_conventions::attributes::{BROWSER__NAME, BROWSER__VERSION, USER_AGENT__ORIGINAL};
use relay_event_schema::protocol::{Attributes, BrowserContext};
use relay_protocol::{Annotated, Value};

use crate::{ClientHints, FromUserAgentInfo as _};

// The vendored uap-core fixtures top out at 50 bytes for names and 27 for versions.
// Leave room for longer values while bounding what enrichment copies into each item.
const MAX_BROWSER_FIELD_BYTES: usize = 128;

/// Lazy browser enrichment shared by the items in one container.
///
/// Transport parsing, including unsuccessful parses, happens at most once.
/// Oversized derived browser fields are skipped before being copied into items.
pub struct UserAgentNormalizer {
    transport_user_agent: Option<String>,
    sec_ch_ua: Option<String>,
    hints: OnceCell<Option<Browser>>,
    user_agent: OnceCell<Option<Browser>>,
}

impl UserAgentNormalizer {
    /// Creates a cache for one container from its transport headers.
    /// Omit the headers when transport inference is disabled.
    pub fn new(user_agent: Option<&str>, sec_ch_ua: Option<&str>) -> Self {
        Self {
            transport_user_agent: user_agent.map(str::to_owned),
            sec_ch_ua: sec_ch_ua.map(str::to_owned),
            hints: OnceCell::new(),
            user_agent: OnceCell::new(),
        }
    }

    /// Normalizes the user agent/client information into [`Attributes`].
    ///
    /// Does not modify the attributes if there is already browser information present,
    /// to preserve original values. An explicit user agent takes precedence over both transport
    /// headers.
    pub fn normalize(&self, attributes: &mut Annotated<Attributes>) {
        let attributes = attributes.get_or_insert_with(Default::default);
        if attributes.contains_key(BROWSER__NAME) || attributes.contains_key(BROWSER__VERSION) {
            return;
        }

        let browser = if attributes.contains_key(USER_AGENT__ORIGINAL) {
            // Even an invalid explicit attribute prevents transport inference.
            attributes
                .get_value(USER_AGENT__ORIGINAL)
                .and_then(Value::as_str)
                .and_then(Browser::from_user_agent)
        } else {
            if let Some(original) = &self.transport_user_agent {
                attributes.insert(USER_AGENT__ORIGINAL, original.clone());
            }
            self.browser_from_hints()
                .or_else(|| self.browser_from_transport())
                .cloned()
        };

        if let Some(browser) = browser {
            attributes.insert(BROWSER__NAME, browser.name);
            attributes.insert(BROWSER__VERSION, browser.version);
        }
    }

    fn browser_from_hints(&self) -> Option<&Browser> {
        self.hints
            .get_or_init(|| {
                Browser::from_hints(ClientHints {
                    sec_ch_ua: self.sec_ch_ua.as_deref(),
                    ..Default::default()
                })
            })
            .as_ref()
    }

    fn browser_from_transport(&self) -> Option<&Browser> {
        self.user_agent
            .get_or_init(|| Browser::from_user_agent(self.transport_user_agent.as_deref()?))
            .as_ref()
    }
}

#[derive(Clone)]
struct Browser {
    name: Annotated<String>,
    version: Annotated<String>,
}

impl Browser {
    fn from_hints(hints: ClientHints<&str>) -> Option<Self> {
        let (name, version) = hints.browser()?;
        if !browser_fields_fit(Some(name), Some(version)) {
            return None;
        }
        Some(Self {
            name: Annotated::new(name.to_owned()),
            version: Annotated::new(version.to_owned()),
        })
    }

    fn from_user_agent(user_agent: &str) -> Option<Self> {
        let browser = BrowserContext::parse_user_agent(user_agent)?;
        if !browser_fields_fit(
            browser.name.value().map(String::as_str),
            browser.version.value().map(String::as_str),
        ) {
            return None;
        }
        Some(Self {
            name: browser.name,
            version: browser.version,
        })
    }
}

fn browser_fields_fit(name: Option<&str>, version: Option<&str>) -> bool {
    [name, version]
        .into_iter()
        .flatten()
        .all(|value| value.len() <= MAX_BROWSER_FIELD_BYTES)
}

#[cfg(test)]
mod tests {
    use relay_protocol::assert_annotated_snapshot;

    use super::*;

    const CHROME_USER_AGENT: &str = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36";

    #[test]
    fn test_browser_from_hints_rejects_oversized_name() {
        let name = "é".repeat(65);
        let header = format!(r#""{name}";v="123""#);
        assert!(
            Browser::from_hints(ClientHints {
                sec_ch_ua: Some(&header),
                ..Default::default()
            })
            .is_none()
        );
    }

    #[test]
    fn test_browser_from_hints_rejects_oversized_version() {
        let version = "1".repeat(129);
        let header = format!(r#""TestBrowser";v="{version}""#);
        assert!(
            Browser::from_hints(ClientHints {
                sec_ch_ua: Some(&header),
                ..Default::default()
            })
            .is_none()
        );
    }

    #[test]
    fn test_browser_from_hints_preserves_values_at_limit() {
        let name = "é".repeat(64);
        let version = "1".repeat(128);
        let header = format!(r#""{name}";v="{version}""#);
        let browser = Browser::from_hints(ClientHints {
            sec_ch_ua: Some(&header),
            ..Default::default()
        })
        .unwrap();

        assert_eq!(browser.name.value(), Some(&name));
        assert_eq!(browser.version.value(), Some(&version));
    }

    #[test]
    fn test_normalize_user_agent_falls_back_after_oversized_hints() {
        let hints = format!(r#""{}";v="123""#, "x".repeat(129));
        let normalizer = UserAgentNormalizer::new(Some(CHROME_USER_AGENT), Some(&hints));
        let mut attributes = Annotated::default();
        normalizer.normalize(&mut attributes);
        let attributes = attributes.value().unwrap();

        assert_eq!(
            attributes.get_value(BROWSER__NAME).and_then(Value::as_str),
            Some("Chrome")
        );
        assert_eq!(
            attributes
                .get_value(BROWSER__VERSION)
                .and_then(Value::as_str),
            Some("131.0.0")
        );
    }

    #[test]
    fn test_normalize_user_agent_prefers_item_ua_over_transport() {
        let firefox = "Mozilla/5.0 (X11; Linux x86_64; rv:128.0) Gecko/20100101 Firefox/128.0";
        let normalizer =
            UserAgentNormalizer::new(Some(CHROME_USER_AGENT), Some(r#""Edge";v="131""#));
        let mut attributes = Attributes::default();
        attributes.insert(USER_AGENT__ORIGINAL, firefox);
        let mut attributes = Annotated::new(attributes);
        normalizer.normalize(&mut attributes);
        let attributes = attributes.value().unwrap();

        assert_eq!(
            attributes
                .get_value(USER_AGENT__ORIGINAL)
                .and_then(Value::as_str),
            Some(firefox)
        );
        assert_eq!(
            attributes.get_value(BROWSER__NAME).and_then(Value::as_str),
            Some("Firefox")
        );
        assert_eq!(
            attributes
                .get_value(BROWSER__VERSION)
                .and_then(Value::as_str),
            Some("128.0")
        );

        let mut next = Annotated::default();
        normalizer.normalize(&mut next);
        assert_eq!(
            next.value()
                .unwrap()
                .get_value(BROWSER__NAME)
                .and_then(Value::as_str),
            Some("Edge")
        );
    }

    #[test]
    fn test_normalize_user_agent_none() {
        let normalizer = UserAgentNormalizer::new(Some(CHROME_USER_AGENT), None);
        let mut attributes = Default::default();
        normalizer.normalize(&mut attributes);

        assert_annotated_snapshot!(attributes, @r###"
        {
          "browser.name": {
            "type": "string",
            "value": "Chrome"
          },
          "browser.version": {
            "type": "string",
            "value": "131.0.0"
          },
          "user_agent.original": {
            "type": "string",
            "value": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
          }
        }
        "###);
    }

    #[test]
    fn test_normalize_user_agent_existing() {
        let normalizer = UserAgentNormalizer::new(Some(CHROME_USER_AGENT), None);
        let mut attributes = Annotated::from_json(
            r#"{
          "browser.name": {
            "type": "string",
            "value": "Very Special"
          },
          "browser.version": {
            "type": "string",
            "value": "13.3.7"
          }
        }"#,
        )
        .unwrap();

        normalizer.normalize(&mut attributes);

        assert_annotated_snapshot!(attributes, @r#"
        {
          "browser.name": {
            "type": "string",
            "value": "Very Special"
          },
          "browser.version": {
            "type": "string",
            "value": "13.3.7"
          }
        }
        "#
        );

        // Either prefilled browser field prevents enrichment on its own.
        for (key, other, expected) in [
            (BROWSER__NAME, BROWSER__VERSION, "Very Special"),
            (BROWSER__VERSION, BROWSER__NAME, "13.3.7"),
        ] {
            let mut partial = attributes.clone();
            partial.value_mut().as_mut().unwrap().remove(other);
            normalizer.normalize(&mut partial);
            let partial = partial.value().unwrap();
            assert_eq!(
                partial.get_value(key).and_then(Value::as_str),
                Some(expected)
            );
            assert!(!partial.contains_key(other));
            assert!(!partial.contains_key(USER_AGENT__ORIGINAL));
        }
        assert!(normalizer.hints.get().is_none());
        assert!(normalizer.user_agent.get().is_none());

        let mut missing = Annotated::default();
        normalizer.normalize(&mut missing);
        assert_eq!(
            missing
                .value()
                .unwrap()
                .get_value(BROWSER__NAME)
                .and_then(Value::as_str),
            Some("Chrome")
        );
    }
}
