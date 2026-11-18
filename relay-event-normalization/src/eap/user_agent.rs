use std::cell::OnceCell;

use relay_conventions::attributes::{BROWSER__NAME, BROWSER__VERSION, USER_AGENT__ORIGINAL};
use relay_event_schema::protocol::{Attributes, BrowserContext};
use relay_protocol::Annotated;

use crate::{ClientHints, FromUserAgentInfo as _};

/// Lazy browser enrichment shared by the items in one container.
///
/// Transport parsing, including unsuccessful parses, happens at most once. Strings
/// are bounded before being copied into items; explicit item user agents remain local.
pub struct UserAgentNormalizer {
    transport_user_agent: Option<String>,
    sec_ch_ua: Option<String>,
    max_chars: usize,
    hints: OnceCell<Option<Browser>>,
    user_agent: OnceCell<Option<Browser>>,
    original: OnceCell<String>,
}

impl UserAgentNormalizer {
    /// Creates a cache for one container from its transport headers.
    /// Omit the headers when transport inference is disabled.
    pub fn new(
        user_agent: Option<&str>,
        sec_ch_ua: Option<&str>,
        max_tag_value_length: usize,
    ) -> Self {
        Self {
            transport_user_agent: user_agent.map(str::to_owned),
            sec_ch_ua: sec_ch_ua.map(str::to_owned),
            max_chars: max_tag_value_length,
            hints: OnceCell::new(),
            user_agent: OnceCell::new(),
            original: OnceCell::new(),
        }
    }

    /// Normalizes the user agent/client information into [`Attributes`].
    ///
    /// Does not modify the attributes if there is already browser information present,
    /// to preserve original values. Explicit user agents take precedence over the transport user agent.
    pub fn normalize(&self, attributes: &mut Annotated<Attributes>) {
        let attributes = attributes.get_or_insert_with(Default::default);
        if attributes.contains_key(BROWSER__NAME) || attributes.contains_key(BROWSER__VERSION) {
            return;
        }

        if let Some(browser) = self.browser_for_item(attributes) {
            attributes.insert(BROWSER__NAME, browser.name);
            attributes.insert(BROWSER__VERSION, browser.version);
        }
        if !attributes.contains_key(USER_AGENT__ORIGINAL)
            && let Some(original) = self.transport_original()
        {
            attributes.insert(USER_AGENT__ORIGINAL, original.to_owned());
        }
    }

    fn browser_for_item(&self, attributes: &Attributes) -> Option<Browser> {
        if let Some(browser) = self.browser_from_hints() {
            return Some(browser.clone());
        }

        // Even an invalid explicit attribute prevents the transport fallback.
        if attributes.contains_key(USER_AGENT__ORIGINAL) {
            let ua = attributes.get_value(USER_AGENT__ORIGINAL)?.as_str()?;
            return Browser::from_user_agent(ua, self.max_chars);
        }

        self.browser_from_transport().cloned()
    }

    fn browser_from_hints(&self) -> Option<&Browser> {
        self.hints
            .get_or_init(|| {
                Browser::from_hints(
                    ClientHints {
                        sec_ch_ua: self.sec_ch_ua.as_deref(),
                        ..Default::default()
                    },
                    self.max_chars,
                )
            })
            .as_ref()
    }

    fn browser_from_transport(&self) -> Option<&Browser> {
        self.user_agent
            .get_or_init(|| {
                Browser::from_user_agent(self.transport_user_agent.as_deref()?, self.max_chars)
            })
            .as_ref()
    }

    fn transport_original(&self) -> Option<&str> {
        let user_agent = self.transport_user_agent.as_deref()?;
        Some(
            self.original
                .get_or_init(|| char_prefix(user_agent, self.max_chars).to_owned()),
        )
    }
}

#[derive(Clone)]
struct Browser {
    name: Annotated<String>,
    version: Annotated<String>,
}

impl Browser {
    fn from_hints(hints: ClientHints<&str>, max_chars: usize) -> Option<Self> {
        let (name, version) = hints.browser()?;
        Some(Self {
            name: Annotated::new(char_prefix(name, max_chars).to_owned()),
            version: Annotated::new(char_prefix(version, max_chars).to_owned()),
        })
    }

    fn from_user_agent(user_agent: &str, max_chars: usize) -> Option<Self> {
        // Parse the full input: truncating it first could change browser detection.
        let mut browser = BrowserContext::parse_user_agent(user_agent)?;
        for field in [&mut browser.name, &mut browser.version] {
            if let Some(value) = field.value_mut() {
                value.truncate(char_prefix(value, max_chars).len());
            }
        }
        Some(Self {
            name: browser.name,
            version: browser.version,
        })
    }
}

fn char_prefix(value: &str, max_chars: usize) -> &str {
    // A string that fits in bytes also fits in characters.
    if value.len() <= max_chars {
        return value;
    }
    let end = value
        .char_indices()
        .nth(max_chars)
        .map_or(value.len(), |(i, _)| i);
    &value[..end]
}

#[cfg(test)]
mod tests {
    use relay_protocol::{Value, assert_annotated_snapshot};

    use super::*;

    const CHROME_USER_AGENT: &str = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36";

    #[test]
    fn test_normalize_user_agent_bounds() {
        for transport in [None, Some("Mozilla/5.0 ".repeat(1000))] {
            for maximum in [0, 1, 3, 256] {
                let name = "🦀browser".repeat(1024);
                let version = "é123".repeat(1024);
                let hints = format!(r#""{name}";v="{version}""#);
                let normalizer =
                    UserAgentNormalizer::new(transport.as_deref(), Some(&hints), maximum);
                assert!(normalizer.hints.get().is_none());
                for _ in 0..32 {
                    let mut attributes = Annotated::default();
                    normalizer.normalize(&mut attributes);
                    let attributes = attributes.value().unwrap();
                    for (key, source) in [
                        (BROWSER__NAME, Some(name.as_str())),
                        (BROWSER__VERSION, Some(version.as_str())),
                        (USER_AGENT__ORIGINAL, transport.as_deref()),
                    ] {
                        let expected = source.map(|s| s.chars().take(maximum).collect::<String>());
                        // Empty attribute strings are omitted, including when the limit is zero.
                        assert_eq!(
                            attributes.get_value(key).and_then(Value::as_str),
                            expected.as_deref().filter(|s| !s.is_empty()),
                            "{key}, limit={maximum}"
                        );
                    }
                }
                assert!(normalizer.hints.get().unwrap().is_some());
                assert!(normalizer.user_agent.get().is_none());
            }
        }
    }

    #[test]
    fn test_normalize_user_agent_precedence() {
        const FIREFOX: &str =
            "Mozilla/5.0 (X11; Linux x86_64; rv:128.0) Gecko/20100101 Firefox/128.0";
        for transport in [None, Some(CHROME_USER_AGENT)] {
            let fallback_version = transport.map(|_| "131");
            for (hints, name, version, fallback_version) in [
                (None, "Fir", "128", fallback_version),
                (Some("invalid hints"), "Fir", "128", fallback_version),
                (
                    Some(r#""Google Chrome";v="123456""#),
                    "Chr",
                    "123",
                    Some("123"),
                ),
            ] {
                let normalizer = UserAgentNormalizer::new(transport, hints, 3);
                let mut attributes = Attributes::default();
                attributes.insert(USER_AGENT__ORIGINAL, FIREFOX.to_owned());
                let mut attributes = Annotated::new(attributes);
                normalizer.normalize(&mut attributes);
                let attributes = attributes.value().unwrap();
                // Hints win over the item UA, which wins over the transport UA.
                // Google Chrome is normalized before bounding, yielding Chr rather than Goo.
                for (key, expected) in [
                    (BROWSER__NAME, name),
                    (BROWSER__VERSION, version),
                    (USER_AGENT__ORIGINAL, FIREFOX),
                ] {
                    assert_eq!(
                        attributes.get_value(key).and_then(Value::as_str),
                        Some(expected)
                    );
                }
                assert!(normalizer.user_agent.get().is_none());
                assert!(normalizer.original.get().is_none());

                // The explicit item UA must not populate the transport cache for later items.
                for _ in 0..2 {
                    let mut attributes = Annotated::default();
                    normalizer.normalize(&mut attributes);
                    let attributes = attributes.value().unwrap();
                    for (key, expected) in [
                        (BROWSER__NAME, fallback_version.map(|_| "Chr")),
                        (BROWSER__VERSION, fallback_version),
                        (USER_AGENT__ORIGINAL, transport.map(|_| "Moz")),
                    ] {
                        assert_eq!(attributes.get_value(key).and_then(Value::as_str), expected);
                    }
                }
            }
        }
    }

    #[test]
    fn test_normalize_user_agent_none() {
        let normalizer = UserAgentNormalizer::new(Some(CHROME_USER_AGENT), None, 256);
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

        // A short stored prefix must not change detection from the full transport UA.
        let normalizer = UserAgentNormalizer::new(Some(CHROME_USER_AGENT), None, 4);
        for _ in 0..3 {
            let mut attributes = Annotated::default();
            normalizer.normalize(&mut attributes);
            let attributes = attributes.value().unwrap();
            for (key, expected) in [
                (BROWSER__NAME, "Chro"),
                (BROWSER__VERSION, "131."),
                (USER_AGENT__ORIGINAL, "Mozi"),
            ] {
                assert_eq!(
                    attributes.get_value(key).and_then(Value::as_str),
                    Some(expected)
                );
            }
        }
        assert!(normalizer.hints.get().unwrap().is_none());
        assert!(normalizer.user_agent.get().unwrap().is_some());
        assert_eq!(normalizer.original.get().map(String::as_str), Some("Mozi"));
    }

    #[test]
    fn test_normalize_user_agent_existing() {
        let normalizer = UserAgentNormalizer::new(Some(CHROME_USER_AGENT), None, 4);
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
        assert!(normalizer.original.get().is_none());

        let mut missing = Annotated::default();
        normalizer.normalize(&mut missing);
        assert_eq!(
            missing
                .value()
                .unwrap()
                .get_value(BROWSER__NAME)
                .and_then(Value::as_str),
            Some("Chro")
        );
    }
}
