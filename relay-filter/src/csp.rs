//! Implements event filtering for events originating from CSP endpoints
//!
//! Events originating from a CSP message can be filtered based on the source URL

use relay_event_schema::protocol::Csp;

use crate::origin::{SchemeDomainPort, matches_any_origin};
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
                Some(val) => Annotated::from(val.to_owned()),
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
                    scheme.map(|x| x.to_owned()),
                    domain.map(|x| x.to_owned()),
                    port.map(|x| x.to_owned())
                )
            );
        }
    }

    #[test]
    fn test_filters_known_blocked_source_files() {
        let event = get_csp_event(None, Some("http://known.bad.com"), None);
        let config = CspFilterConfig {
            disallowed_sources: vec!["http://known.bad.com".to_owned()],
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
            disallowed_sources: vec!["http://known.bad.com".to_owned()],
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
            disallowed_sources: vec!["http://known.bad.com".to_owned()],
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
            disallowed_sources: vec!["http://known.bad.com".to_owned()],
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
            disallowed_sources: vec!["http://known.bad.com".to_owned()],
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
            disallowed_sources: vec!["http://known.bad.com".to_owned()],
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
            "about".to_owned(),
            "ms-browser-extension".to_owned(),
            "*.superfish.com".to_owned(),
            "chrome://*".to_owned(),
            "chrome-extension://*".to_owned(),
            "chromeinvokeimmediate://*".to_owned(),
            "chromenull://*".to_owned(),
            "localhost".to_owned(),
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
