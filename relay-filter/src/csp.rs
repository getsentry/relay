//! Implements event filtering for events originating from CSP endpoints
//!
//! Events originating from a CSP message can be filtered based on the source URL

use relay_event_schema::protocol::Csp;

use crate::{CspFilterConfig, FilterStatKey, Filterable};

/// Checks if the event is a CSP Event from one of the disallowed sources.
fn matches<It, S>(csp: Option<&Csp>, disallowed_sources: It) -> bool
where
    It: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    // parse the sources for easy processing
    let disallowed_sources: Vec<SchemeDomainPort> = disallowed_sources
        .into_iter()
        .map(|origin| -> SchemeDomainPort { origin.as_ref().into() })
        .collect();

    if let Some(csp) = csp {
        if matches_any_origin(csp.blocked_uri.as_str(), &disallowed_sources) {
            return true;
        }
        if matches_any_origin(csp.source_file.as_str(), &disallowed_sources) {
            return true;
        }
        if matches_any_origin(csp.document_uri.as_str(), &disallowed_sources) {
            return true;
        }
    }
    false
}

/// Filters CSP events based on disallowed sources.
pub fn should_filter<F>(item: &F, config: &CspFilterConfig) -> Result<(), FilterStatKey>
where
    F: Filterable,
{
    if matches(item.csp(), &config.disallowed_sources) {
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

        // extract domain:port from the rest of the url
        let end_domain_idx = rest.find('/');
        let domain_port = if let Some(end_domain_idx) = end_domain_idx {
            &rest[..end_domain_idx] // remove the path from rest
        } else {
            rest // no path, use everything
        };

        // split the domain and the port
        let ipv6_end_bracket_idx = domain_port.rfind(']');
        let port_separator_idx = if let Some(end_bracket_idx) = ipv6_end_bracket_idx {
            // we have an ipv6 address, find the port separator after the closing bracket
            domain_port[end_bracket_idx..]
                .rfind(':')
                .map(|x| x + end_bracket_idx)
        } else {
            // no ipv6 address, find the port separator in the whole string
            domain_port.rfind(':')
        };
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
    use relay_event_schema::protocol::{Csp, Event, EventType};
    use relay_protocol::Annotated;

    use super::*;

    fn get_csp_event(
        blocked_uri: Option<&str>,
        source_file: Option<&str>,
        document_uri: Option<&str>,
    ) -> Event {
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
                document_uri: annotated_string_or_none(document_uri),
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
            ("abc.com/[something]", None, Some("abc.com"), None),
            ("abc.com/something]:", None, Some("abc.com"), None),
            ("abc.co]m/[something:", None, Some("abc.co]m"), None),
            ("]abc.com:9000", None, Some("]abc.com"), Some("9000")),
            (
                "https://api.example.com/foo/00000000-0000-0000-0000-000000000000?includes[]=user&includes[]=image&includes[]=author&includes[]=tag",
                Some("https"),
                Some("api.example.com"),
                None,
            )
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
    fn test_scheme_domain_port_with_ip() {
        let examples = [
            (
                "http://192.168.1.1:3000",
                Some("http"),
                Some("192.168.1.1"),
                Some("3000"),
            ),
            ("192.168.1.1", None, Some("192.168.1.1"), None),
            ("[fd45:7aa3:7ae4::]", None, Some("[fd45:7aa3:7ae4::]"), None),
            ("http://172.16.*.*", Some("http"), Some("172.16.*.*"), None),
            (
                "http://[1fff:0:a88:85a3::ac1f]:8001",
                Some("http"),
                Some("[1fff:0:a88:85a3::ac1f]"),
                Some("8001"),
            ),
            // invalid IPv6 for localhost since it's not inside brackets
            ("::1", None, Some(":"), Some("1")),
            ("[::1]", None, Some("[::1]"), None),
            (
                "http://[fe80::862a:fdff:fe78:a2bf%13]",
                Some("http"),
                Some("[fe80::862a:fdff:fe78:a2bf%13]"),
                None,
            ),
            // invalid addresses. although these results don't represent correct results,
            // they are here to make sure the application won't crash.
            ("192.168.1.1.1", None, Some("192.168.1.1.1"), None),
            ("192.168.1.300", None, Some("192.168.1.300"), None),
            (
                "[2001:0db8:85a3:::8a2e:0370:7334]",
                None,
                Some("[2001:0db8:85a3:::8a2e:0370:7334]"),
                None,
            ),
            ("[fe80::1::]", None, Some("[fe80::1::]"), None),
            ("fe80::1::", None, Some("fe80::1:"), Some("")),
            (
                "[2001:0db8:85a3:xyz::8a2e:0370:7334]",
                None,
                Some("[2001:0db8:85a3:xyz::8a2e:0370:7334]"),
                None,
            ),
            (
                "2001:0db8:85a3:xyz::8a2e:0370:7334",
                None,
                Some("2001:0db8:85a3:xyz::8a2e:0370"),
                Some("7334"),
            ),
            ("192.168.0.1/24", None, Some("192.168.0.1"), None),
        ];

        for (url, scheme, domain, port) in examples {
            let actual = SchemeDomainPort::from(url);
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
            // starts on both ends
            (
                "https://abc.software.example.com",
                vec!["*abc.software.example.com*"],
                false,
            ),
        ];

        for (url, origins, expected) in examples {
            let origins: Vec<_> = origins
                .iter()
                .map(|url| SchemeDomainPort::from(*url))
                .collect();
            let actual = matches_any_origin(Some(*url), &origins[..]);
            assert_eq!(*expected, actual, "Could not match {url}.");
        }
    }

    #[test]
    fn test_filters_known_blocked_source_files() {
        let event = get_csp_event(None, Some("http://known.bad.com"), None);
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
        let event = get_csp_event(None, Some("http://good.file.com"), None);
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
    fn test_filters_known_document_uris() {
        let event = get_csp_event(None, None, Some("http://known.bad.com"));
        let config = CspFilterConfig {
            disallowed_sources: vec!["http://known.bad.com".to_string()],
        };

        let actual = should_filter(&event, &config);
        assert_ne!(
            actual,
            Ok(()),
            "CSP filter should have filtered known document uri"
        );
    }

    #[test]
    fn test_filters_known_blocked_uris() {
        let event = get_csp_event(Some("http://known.bad.com"), None, None);
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
        let event = get_csp_event(Some("http://good.file.com"), None, None);
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
        let mut event = get_csp_event(Some("http://known.bad.com"), None, None);
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
            let event = get_csp_event(*blocked_uri, *source_file, None);
            let config = CspFilterConfig {
                disallowed_sources: get_disallowed_sources(),
            };

            let actual = should_filter(&event, &config);
            assert_ne!(
                actual,
                Ok(()),
                "CSP filter should have filtered  bad request {blocked_uri:?} {source_file:?}"
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
            let event = get_csp_event(*blocked_uri, *source_file, None);
            let config = CspFilterConfig {
                disallowed_sources: get_disallowed_sources(),
            };

            let actual = should_filter(&event, &config);
            assert_eq!(
                actual,
                Ok(()),
                "CSP filter should have  NOT filtered  request {blocked_uri:?} {source_file:?}"
            );
        }
    }
}
