//! Implements event filtering for events originating from CSP endpoints
//!
//! Events originating from a CSP message can be filtered based on the source URL
//!
use url::Url;

use crate::protocol::{Event, EventType};
use std::collections::HashSet;

/// Should filter event
pub fn should_filter(event: &Event, csp_disallowed_sources: &[String]) -> Result<(), String> {
    // only filter CSP events with non empty disallowed source list
    if csp_disallowed_sources.is_empty() || event.ty.value() != Some(&EventType::Csp) {
        return Ok(());
    }

    // parse the sources for easy processing
    let csp_disallowed_sources: HashSet<SchemeDomainPort> = csp_disallowed_sources
        .iter()
        .map(|origin| -> SchemeDomainPort { origin.to_lowercase().as_str().into() })
        .collect();

    if let Some(csp) = event.csp.value() {
        if matches_any_origin(csp.blocked_uri.value(), &csp_disallowed_sources) {
            return Err("CSP filter failed, blocked_uri".to_string());
        }
        if matches_any_origin(csp.source_file.value(), &csp_disallowed_sources) {
            return Err("CSP filter failed, source_file".to_string());
        }
    }
    Ok(())
}

#[derive(PartialEq, Eq)]
struct Bubu {
    pub a: i32,
    pub b: i32,
}

/// A pattern used to match allowed paths
///
/// scheme, domain and port are extracted from an url
/// they may be either a string (to be matched exactly, case insensitive)
/// or None (matches anything in the respective position)
#[derive(Hash, PartialEq, Eq)]
struct SchemeDomainPort {
    pub scheme: Option<String>,
    pub domain: Option<String>,
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
        let end_domain_idx = rest.find("/");
        let domain_port = if let Some(end_domain_idx) = end_domain_idx {
            &rest[..end_domain_idx] // remove the path from rest
        } else {
            rest // no path, use everything
        };

        //split the domain and the port
        let port_separator_idx = domain_port.find(":");
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

/// Checks if a url satisfies one of the specified origins
///
/// An origin specification may be in any of the following formats:
///  - http://domain.com[:port]  - an exact match is required
///  - * : anything goes
///  - *.domain.com : matches domain.com and any subdomains
///  - *.port : matches any hostname as long as the port matches
fn matches_any_origin(url: Option<&String>, origins: &HashSet<SchemeDomainPort>) -> bool {
    // if we have a "*" (Any) option, anything matches so don't bother going forward
    if origins
        .iter()
        .any(|o| o.scheme == None && o.port == None && o.domain == None)
    {
        return true;
    }

    if url == None {
        return false;
    }
    let url = Url::parse(url.unwrap());

    if let Ok(url) = url {
        let scheme: Option<String> = Some(url.scheme().to_string());
        let domain = url.host_str().map(|s| s.to_string());
        let port = url.port().map(|p| p.to_string());

        let mut matches: HashSet<SchemeDomainPort> = HashSet::new();

        // create a cartesian product with all patterns that could match (duplicates will be eliminated by the set)
        for s in &[&None, &scheme] {
            for d in &[&None, &domain] {
                for p in &[&None, &port] {
                    matches.insert(SchemeDomainPort {
                        //TODO why do I need to deref ?
                        scheme: (*s).clone(),
                        domain: (*d).clone(),
                        port: (*p).clone(),
                    });
                }
            }
        }

        return matches
            .intersection(origins)
            //TODO is there a better way to check if the intersection is empty ?
            .collect::<Vec<&SchemeDomainPort>>()
            .is_empty();
    }

    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scheme_domain_port() {
        let examples = &[
            ("*", None, None, None),
            ("*://*", None, None, None),
            ("*://*:*", None, None, None),
            ("https://*", Some("https"), None, None),
            ("https://*:*", Some("https"), None, None),
            ("x.y.z", None, Some("x.y.z"), None),
            ("x.y.z:*", None, Some("x.y.z"), None),
            ("*://x.y.z:*", None, Some("x.y.z"), None),
            ("*:8000", None, None, Some("8000")),
            ("*://*:8000", None, None, Some("8000")),
            ("http://x.y.z", Some("http"), Some("x.y.z"), None),
            ("http://*:8000", Some("http"), None, Some("8000")),
            ("abc:8000", None, Some("abc"), Some("8000")),
            (
                "http://x.y.z:4000",
                Some("http"),
                Some("x.y.z"),
                Some("4000"),
            ),
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
            (
                "http://abc.com/some/path&param=2",
                vec!["http://*", "http://other.com"],
                true,
            ),
            (
                "http://abc.com/some/path&param=2",
                vec!["http://*", "http://other.com", "fasdfas"],
                true,
            ),
        ];
    }

    #[test]
    fn test_url_parsing() {
        let urls = [
            "http://a.b.c/f.html",
            "abc:4000",
            "abc:4000/f/d/h.html",
            "http://abc:4000/f/d/h.html",
        ];

        let parsed = urls.iter().map(|u| {
            Url::parse(u)
                .map(|u| {
                    vec![
                        format!("scheme->{}", u.scheme()),
                        format!("domain->{}", u.domain().unwrap_or("No Doamin")),
                        format!("port->{}", u.port().map(|x| x as i16).unwrap_or(-1)),
                        format!("path->{}", u.path()),
                    ]
                })
                .unwrap_or(vec![])
        });

        let display: Vec<_> = urls.iter().zip(parsed).collect();
        insta::assert_debug_snapshot_matches!(display, @r###"
       ⋮[
       ⋮    (
       ⋮        "http://a.b.c/f.html",
       ⋮        [
       ⋮            "scheme->http",
       ⋮            "domain->a.b.c",
       ⋮            "port->-1",
       ⋮            "path->/f.html",
       ⋮        ],
       ⋮    ),
       ⋮    (
       ⋮        "abc:4000",
       ⋮        [
       ⋮            "scheme->abc",
       ⋮            "domain->No Doamin",
       ⋮            "port->-1",
       ⋮            "path->4000",
       ⋮        ],
       ⋮    ),
       ⋮    (
       ⋮        "abc:4000/f/d/h.html",
       ⋮        [
       ⋮            "scheme->abc",
       ⋮            "domain->No Doamin",
       ⋮            "port->-1",
       ⋮            "path->4000/f/d/h.html",
       ⋮        ],
       ⋮    ),
       ⋮    (
       ⋮        "http://abc:4000/f/d/h.html",
       ⋮        [
       ⋮            "scheme->http",
       ⋮            "domain->abc",
       ⋮            "port->4000",
       ⋮            "path->/f/d/h.html",
       ⋮        ],
       ⋮    ),
       ⋮]
        "###);
    }
}
