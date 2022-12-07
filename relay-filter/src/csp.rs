//! Implements event filtering for events originating from CSP endpoints
//!
//! Events originating from a CSP message can be filtered based on the source URL

use relay_general::protocol::{Event, EventType};

use crate::{CspFilterConfig, FilterStatKey};

/// Checks if the event is a CSP Event from one of the disallowed sources.
pub fn matches<It, S>(event: &Event, disallowed_sources: It) -> bool
where
    It: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    if event.ty.value() != Some(&EventType::Csp) {
        return false;
    }

    // parse the sources for easy processing
    let disallowed_sources: Vec<SchemeDomainPort> = disallowed_sources
        .into_iter()
        .map(|origin| -> SchemeDomainPort { origin.as_ref().into() })
        .collect();

    if let Some(csp) = event.csp.value() {
        if matches_any_origin(csp.blocked_uri.as_str(), &disallowed_sources) {
            return true;
        }
        if matches_any_origin(csp.source_file.as_str(), &disallowed_sources) {
            return true;
        }
    }
    false
}

/// Filters CSP events based on disallowed sources.
pub fn should_filter(event: &Event, config: &CspFilterConfig) -> Result<(), FilterStatKey> {
    if matches(event, &config.disallowed_sources) {
        Err(FilterStatKey::InvalidCsp)
    } else {
        Ok(())
    }
}

/// A pattern used to match allowed paths.
///
/// Scheme, domain and port are extracted from an url,
/// they may be either a string (to be matched exactly, case insensitive)
/// or None (matches anything in the respective position).
#[derive(Hash, PartialEq, Eq)]
pub struct SchemeDomainPort {
    /// The scheme of the url.
    pub scheme: Option<String>,
    /// The domain of the url.
    pub domain: Option<String>,
    /// The port of the url.
    pub port: Option<String>,
}

impl From<&str> for SchemeDomainPort {
    /// parse a string into a SchemaDomainPort pattern
    fn from(url: &str) -> SchemeDomainPort {
        /// converts string into patterns for SchemeDomainPort
        /// the convention is that a "*" matches everything which
        /// we encode as a None (same as the absence of the pattern)
        fn normalize(pattern: &str) -> Option<String> {
            if pattern == "*" {
                None
            } else {
                Some(pattern.to_lowercase())
            }
        }

        //split the scheme from the url
        let scheme_idx = url.find("://");
        let (scheme, rest) = if let Some(idx) = scheme_idx {
            (normalize(&url[..idx]), &url[idx + 3..]) // chop after the scheme + the "://" delimiter
        } else {
            (None, url) // no scheme, chop nothing form original string
        };

        //extract domain:port from the rest of the url
        let end_domain_idx = rest.find('/');
        let domain_port = if let Some(end_domain_idx) = end_domain_idx {
            &rest[..end_domain_idx] // remove the path from rest
        } else {
            rest // no path, use everything
        };

        //split the domain and the port
        let port_separator_idx = domain_port.find(':');
        let (domain, port) = if let Some(port_separator_idx) = port_separator_idx {
            //we have a port separator, split the string into domain and port
            (
                normalize(&domain_port[..port_separator_idx]),
                normalize(&domain_port[port_separator_idx + 1..]),
            )
        } else {
            (normalize(domain_port), None) // no port, whole string represents the domain
        };

        SchemeDomainPort {
            scheme,
            domain,
            port,
        }
    }
}

/// Checks if a url satisfies one of the specified origins.
///
/// An origin specification may be in any of the following formats:
///
///  - `http://domain.com[:port]`
///  - an exact match is required
///  - `*`: anything goes
///  - `*.domain.com`: matches domain.com and any subdomains
///  - `*:port`: matches any hostname as long as the port matches
pub fn matches_any_origin(url: Option<&str>, origins: &[SchemeDomainPort]) -> bool {
    // if we have a "*" (Any) option, anything matches so don't bother going forward
    if origins
        .iter()
        .any(|o| o.scheme.is_none() && o.port.is_none() && o.domain.is_none())
    {
        return true;
    }

    if let Some(url) = url {
        let url = SchemeDomainPort::from(url);

        for origin in origins {
            if origin.scheme.is_some() && url.scheme != origin.scheme {
                continue; // scheme not matched
            }
            if origin.port.is_some() && url.port != origin.port {
                continue; // port not matched
            }
            if origin.domain.is_some() && url.domain != origin.domain {
                // no direct match for domain, look for  partial patterns (e.g. "*.domain.com")
                if let (Some(origin_domain), Some(domain)) = (&origin.domain, &url.domain) {
                    if origin_domain.starts_with('*')
                        && ((*domain).ends_with(origin_domain.get(1..).unwrap_or(""))
                            || domain.as_str() == origin_domain.get(2..).unwrap_or(""))
                    {
                        return true; // partial domain pattern match
                    }
                }
                continue; // domain not matched
            }
            // if we are here all patterns have matched so we are done
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    use relay_general::protocol::Csp;
    use relay_general::types::Annotated;

    fn get_csp_event(blocked_uri: Option<&str>, source_file: Option<&str>) -> Event {
        fn annotated_string_or_none(val: Option<&str>) -> Annotated<String> {
            match val {
                None => Annotated::empty(),
                Some(val) => Annotated::from(val.to_string()),
            }
        }
        Event {
            ty: Annotated::from(EventType::Csp),
            csp: Annotated::from(Csp {
                blocked_uri: annotated_string_or_none(blocked_uri),
                source_file: annotated_string_or_none(source_file),
                ..Csp::default()
            }),
            ..Event::default()
        }
    }

    #[test]
    fn test_scheme_domain_port() {
        let examples = &[
            ("*", None, None, None),
            ("*://*", None, None, None),
            ("*://*:*", None, None, None),
            ("https://*", Some("https"), None, None),
            ("https://*.abc.net", Some("https"), Some("*.abc.net"), None),
            ("https://*:*", Some("https"), None, None),
            ("x.y.z", None, Some("x.y.z"), None),
            ("x.y.z:*", None, Some("x.y.z"), None),
            ("*://x.y.z:*", None, Some("x.y.z"), None),
            ("*://*.x.y.z:*", None, Some("*.x.y.z"), None),
            ("*:8000", None, None, Some("8000")),
            ("*://*:8000", None, None, Some("8000")),
            ("http://x.y.z", Some("http"), Some("x.y.z"), None),
            ("http://*:8000", Some("http"), None, Some("8000")),
            ("abc:8000", None, Some("abc"), Some("8000")),
            ("*.abc.com:8000", None, Some("*.abc.com"), Some("8000")),
            ("*.com:86", None, Some("*.com"), Some("86")),
            (
                "http://abc.com:86",
                Some("http"),
                Some("abc.com"),
                Some("86"),
            ),
            (
                "http://x.y.z:4000",
                Some("http"),
                Some("x.y.z"),
                Some("4000"),
            ),
            ("http://", Some("http"), Some(""), None),
        ];

        for (url, scheme, domain, port) in examples {
            let actual: SchemeDomainPort = (*url).into();
            assert_eq!(
                (actual.scheme, actual.domain, actual.port),
                (
                    scheme.map(|x| x.to_string()),
                    domain.map(|x| x.to_string()),
                    port.map(|x| x.to_string())
                )
            );
        }
    }

    #[test]
    fn test_matches_any_origin() {
        let examples = &[
            //MATCH
            //generic matches
            ("http://abc1.com", vec!["*://*:*", "bbc.com"], true),
            ("http://abc2.com", vec!["*:*", "bbc.com"], true),
            ("http://abc3.com", vec!["*", "bbc.com"], true),
            ("http://abc4.com", vec!["http://*", "bbc.com"], true),
            (
                "http://abc5.com",
                vec!["http://abc5.com:*", "bbc.com"],
                true,
            ),
            ("http://abc.com:80", vec!["*://*:*", "bbc.com"], true),
            ("http://abc.com:81", vec!["*:*", "bbc.com"], true),
            ("http://abc.com:82", vec!["*:82", "bbc.com"], true),
            ("http://abc.com:83", vec!["http://*:83", "bbc.com"], true),
            ("http://abc.com:84", vec!["abc.com:*", "bbc.com"], true),
            //partial domain matches
            ("http://abc.com:85", vec!["*.abc.com:85", "bbc.com"], true),
            ("http://abc.com:86", vec!["*.com:86"], true),
            ("http://abc.com:86", vec!["*.com:86", "bbc.com"], true),
            ("http://abc.def.ghc.com:87", vec!["*.com:87"], true),
            ("http://abc.def.ghc.com:88", vec!["*.ghc.com:88"], true),
            ("http://abc.def.ghc.com:89", vec!["*.def.ghc.com:89"], true),
            //exact matches
            ("http://abc.com:90", vec!["abc.com", "bbc.com"], true),
            ("http://abc.com:91", vec!["abc.com:91", "bbc.com"], true),
            ("http://abc.com:92", vec!["http://abc.com:92"], true),
            ("http://abc.com:93", vec!["http://abc.com", "bbc.com"], true),
            //matches in various positions
            ("http://abc6.com", vec!["abc6.com", "bbc.com"], true),
            ("http://abc7.com", vec!["bbc.com", "abc7.com"], true),
            ("http://abc8.com", vec!["bbc.com", "abc8.com", "def"], true),
            //NON MATCH
            //different domain
            (
                "http://abc9.com",
                vec!["http://other.com", "bbc.com"],
                false,
            ),
            ("http://abc10.com", vec!["http://*.other.com", "bbc"], false),
            ("abc11.com", vec!["*.other.com", "bbc"], false),
            //different scheme
            (
                "https://abc12.com",
                vec!["http://abc12.com", "bbc.com"],
                false,
            ),
            //different port
            (
                "http://abc13.com:80",
                vec!["http://abc13.com:8080", "bbc.com"],
                false,
            ),
            // this used to crash
            ("http://y:80", vec!["http://x"], false),
        ];

        for (url, origins, expected) in examples {
            let origins: Vec<_> = origins
                .iter()
                .map(|url| SchemeDomainPort::from(*url))
                .collect();
            let actual = matches_any_origin(Some(*url), &origins[..]);
            assert_eq!(*expected, actual, "Could not match {}.", url);
        }
    }

    #[test]
    fn test_filters_known_blocked_source_files() {
        let event = get_csp_event(None, Some("http://known.bad.com"));
        let config = CspFilterConfig {
            disallowed_sources: vec!["http://known.bad.com".to_string()],
        };

        let actual = should_filter(&event, &config);
        assert_ne!(
            actual,
            Ok(()),
            "CSP filter should have filtered known bad source file"
        );
    }

    #[test]
    fn test_does_not_filter_benign_source_files() {
        let event = get_csp_event(None, Some("http://good.file.com"));
        let config = CspFilterConfig {
            disallowed_sources: vec!["http://known.bad.com".to_string()],
        };

        let actual = should_filter(&event, &config);
        assert_eq!(
            actual,
            Ok(()),
            "CSP filter should have NOT filtered good source file"
        );
    }

    #[test]
    fn test_filters_known_blocked_uris() {
        let event = get_csp_event(Some("http://known.bad.com"), None);
        let config = CspFilterConfig {
            disallowed_sources: vec!["http://known.bad.com".to_string()],
        };

        let actual = should_filter(&event, &config);
        assert_ne!(
            actual,
            Ok(()),
            "CSP filter should have filtered known blocked uri"
        );
    }

    #[test]
    fn test_does_not_filter_benign_uris() {
        let event = get_csp_event(Some("http://good.file.com"), None);
        let config = CspFilterConfig {
            disallowed_sources: vec!["http://known.bad.com".to_string()],
        };

        let actual = should_filter(&event, &config);
        assert_eq!(
            actual,
            Ok(()),
            "CSP filter should have NOT filtered unknown blocked uri"
        );
    }

    #[test]
    fn test_does_not_filter_non_csp_messages() {
        let mut event = get_csp_event(Some("http://known.bad.com"), None);
        event.ty = Annotated::from(EventType::Transaction);
        let config = CspFilterConfig {
            disallowed_sources: vec!["http://known.bad.com".to_string()],
        };

        let actual = should_filter(&event, &config);
        assert_eq!(
            actual,
            Ok(()),
            "CSP filter should have NOT filtered non CSP event"
        );
    }

    fn get_disallowed_sources() -> Vec<String> {
        vec![
            "about".to_string(),
            "ms-browser-extension".to_string(),
            "*.superfish.com".to_string(),
            "chrome://*".to_string(),
            "chrome-extension://*".to_string(),
            "chromeinvokeimmediate://*".to_string(),
            "chromenull://*".to_string(),
            "localhost".to_string(),
        ]
    }

    /// Copy the test cases from Sentry
    #[test]
    fn test_sentry_csp_filter_compatibility_bad_reports() {
        let examples = &[
            (Some("about"), None),
            (Some("ms-browser-extension"), None),
            (Some("http://foo.superfish.com"), None),
            (None, Some("chrome-extension://fdsa")),
            (None, Some("http://localhost:8000")),
            (None, Some("http://localhost")),
            (None, Some("http://foo.superfish.com")),
        ];

        for (blocked_uri, source_file) in examples {
            let event = get_csp_event(*blocked_uri, *source_file);
            let config = CspFilterConfig {
                disallowed_sources: get_disallowed_sources(),
            };

            let actual = should_filter(&event, &config);
            assert_ne!(
                actual,
                Ok(()),
                "CSP filter should have filtered  bad request {:?} {:?}",
                blocked_uri,
                source_file
            );
        }
    }

    #[test]
    fn test_sentry_csp_filter_compatibility_good_reports() {
        let examples = &[
            (Some("http://example.com"), None),
            (None, Some("http://example.com")),
            (None, None),
        ];

        for (blocked_uri, source_file) in examples {
            let event = get_csp_event(*blocked_uri, *source_file);
            let config = CspFilterConfig {
                disallowed_sources: get_disallowed_sources(),
            };

            let actual = should_filter(&event, &config);
            assert_eq!(
                actual,
                Ok(()),
                "CSP filter should have  NOT filtered  request {:?} {:?}",
                blocked_uri,
                source_file
            );
        }
    }
}
