use std::cell::OnceCell;

use relay_conventions::attributes::{BROWSER__NAME, BROWSER__VERSION, USER_AGENT__ORIGINAL};
use relay_event_schema::protocol::{Attributes, BrowserContext};
use relay_protocol::{Annotated, Value};

use crate::{ClientHints, FromUserAgentInfo as _};

/// Client user agent information.
///
/// This is information which is extracted from the client's request.
#[derive(Debug, Copy, Clone, Default)]
pub struct ClientUserAgentInfo<'a> {
    /// The user agent extracted from the client request.
    pub user_agent: Option<&'a str>,
    /// Client hints extracted from the client request.
    pub hints: ClientHints<&'a str>,
}

/// Lazy browser enrichment shared by the items in one container.
///
/// Transport parsing, including unsuccessful parses, happens at most once. Strings
/// are bounded before being copied into items; explicit item user agents remain local.
pub struct UserAgentNormalizer {
    max_chars: usize,
    hints: OnceCell<Option<BrowserContext>>,
    user_agent: OnceCell<Option<BrowserContext>>,
    original: OnceCell<String>,
}

impl UserAgentNormalizer {
    /// Creates an empty cache. Use one normalizer per container with the same transport headers.
    pub fn new(max_tag_value_length: usize) -> Self {
        Self {
            max_chars: max_tag_value_length,
            hints: OnceCell::new(),
            user_agent: OnceCell::new(),
            original: OnceCell::new(),
        }
    }

    fn parse_hints(&self, hints: ClientHints<&str>) -> Option<BrowserContext> {
        let (name, version) = crate::browser_from_client_hints(hints.sec_ch_ua?)?;
        let name = if name == "Google Chrome" {
            "Chrome"
        } else {
            name
        };
        Some(BrowserContext {
            name: Annotated::new(char_prefix(name, self.max_chars).to_owned()),
            version: Annotated::new(char_prefix(version, self.max_chars).to_owned()),
            ..Default::default()
        })
    }

    fn parse_user_agent(&self, user_agent: &str) -> Option<BrowserContext> {
        // Parse the full input: truncating it first could change browser detection.
        let mut browser = BrowserContext::parse_user_agent(user_agent)?;
        for field in [&mut browser.name, &mut browser.version] {
            if let Some(value) = field.value_mut() {
                value.truncate(char_prefix(value, self.max_chars).len());
            }
        }
        Some(browser)
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

/// Normalizes the user agent/client information into [`Attributes`].
///
/// Does not modify the attributes if there is already browser information present,
/// to preserve original values. Explicit user agents take precedence over the transport user agent.
///
/// The `client_info` should be omitted for cases where the info is unreliable or incorrect, for
/// example when the client is a backend SDK or another Relay.
///
/// Share the normalizer across a container to avoid reparsing transport headers for every item.
pub fn normalize_user_agent(
    attributes: &mut Annotated<Attributes>,
    client_info: Option<ClientUserAgentInfo<'_>>,
    normalizer: &UserAgentNormalizer,
) {
    let attributes = attributes.get_or_insert_with(Default::default);
    if attributes.contains_key(BROWSER__NAME) || attributes.contains_key(BROWSER__VERSION) {
        return;
    }

    let client_info = client_info.unwrap_or_default();
    let hints = normalizer
        .hints
        .get_or_init(|| normalizer.parse_hints(client_info.hints));
    let browser = if let Some(browser) = hints {
        Some((browser.name.clone(), browser.version.clone()))
    } else if attributes.contains_key(USER_AGENT__ORIGINAL) {
        // Even an invalid explicit attribute prevents the transport fallback.
        attributes
            .get_value(USER_AGENT__ORIGINAL)
            .and_then(Value::as_str)
            .and_then(|ua| normalizer.parse_user_agent(ua))
            .map(|browser| (browser.name, browser.version))
    } else {
        normalizer
            .user_agent
            .get_or_init(|| {
                client_info
                    .user_agent
                    .and_then(|ua| normalizer.parse_user_agent(ua))
            })
            .as_ref()
            .map(|browser| (browser.name.clone(), browser.version.clone()))
    };

    if let Some(ua) = client_info.user_agent {
        attributes.insert_if_missing(USER_AGENT__ORIGINAL, || {
            normalizer
                .original
                .get_or_init(|| char_prefix(ua, normalizer.max_chars).to_owned())
                .clone()
        });
    }
    if let Some((name, version)) = browser {
        attributes.insert_if_missing(BROWSER__NAME, || name);
        attributes.insert_if_missing(BROWSER__VERSION, || version);
    }
}

#[cfg(test)]
mod tests {
    use relay_protocol::assert_annotated_snapshot;

    use super::*;

    const CHROME_USER_AGENT: &str = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36";

    #[test]
    fn test_normalize_user_agent_bounds() {
        for transport in [None, Some("Mozilla/5.0 ".repeat(1000))] {
            for maximum in [0, 1, 3, 256] {
                let name = "🦀browser".repeat(1024);
                let version = "é123".repeat(1024);
                let hints = format!(r#""{name}";v="{version}""#);
                let client_info = Some(ClientUserAgentInfo {
                    user_agent: transport.as_deref(),
                    hints: ClientHints {
                        sec_ch_ua: Some(&hints),
                        ..Default::default()
                    },
                });
                let normalizer = UserAgentNormalizer::new(maximum);
                assert!(normalizer.hints.get().is_none());
                for _ in 0..32 {
                    let mut attributes = Annotated::default();
                    normalize_user_agent(&mut attributes, client_info, &normalizer);
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
                let client_info = Some(ClientUserAgentInfo {
                    user_agent: transport,
                    hints: ClientHints {
                        sec_ch_ua: hints,
                        ..Default::default()
                    },
                });
                let normalizer = UserAgentNormalizer::new(3);
                let mut attributes = Attributes::default();
                attributes.insert(USER_AGENT__ORIGINAL, FIREFOX.to_owned());
                let mut attributes = Annotated::new(attributes);
                normalize_user_agent(&mut attributes, client_info, &normalizer);
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
                    normalize_user_agent(&mut attributes, client_info, &normalizer);
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
        let normalizer = UserAgentNormalizer::new(256);
        let mut attributes = Default::default();
        normalize_user_agent(
            &mut attributes,
            Some(ClientUserAgentInfo {
                user_agent: Some(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
                ),
                ..Default::default()
            }),
            &normalizer,
        );

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
        let normalizer = UserAgentNormalizer::new(4);
        for _ in 0..3 {
            let mut attributes = Annotated::default();
            normalize_user_agent(
                &mut attributes,
                Some(ClientUserAgentInfo {
                    user_agent: Some(CHROME_USER_AGENT),
                    ..Default::default()
                }),
                &normalizer,
            );
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
        let normalizer = UserAgentNormalizer::new(4);
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

        normalize_user_agent(
            &mut attributes,
            Some(ClientUserAgentInfo {
                user_agent: Some(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
                ),
                ..Default::default()
            }),
            &normalizer,
        );

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
            normalize_user_agent(
                &mut partial,
                Some(ClientUserAgentInfo {
                    user_agent: Some(CHROME_USER_AGENT),
                    ..Default::default()
                }),
                &normalizer,
            );
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
        normalize_user_agent(
            &mut missing,
            Some(ClientUserAgentInfo {
                user_agent: Some(CHROME_USER_AGENT),
                ..Default::default()
            }),
            &normalizer,
        );
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
