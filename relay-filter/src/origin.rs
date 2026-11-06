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
                if let (Some(origin_domain), Some(domain)) = (&origin.domain, &url.domain)
                    && let Some(suffix) = origin_domain.strip_prefix('*')
                    && host_matches_or_is_subdomain_of(
                        domain,
                        suffix.strip_prefix('.').unwrap_or(suffix),
                    )
                {
                    return true; // partial domain pattern match
                }
                continue; // domain not matched
            }
            // if we are here all patterns have matched so we are done
            return true;
        }
    }
    false
}

/// Checks if `host` is `domain` itself or one of its subdomains.
///
/// The match only happens on a label boundary, so `notabc.com` and `bc.com` are not subdomains of
/// `abc.com`. An empty `domain` never matches.
pub fn host_matches_or_is_subdomain_of(host: &str, domain: &str) -> bool {
    if domain.is_empty() {
        return false;
    }
    host.strip_suffix(domain)
        .is_some_and(|s| s.is_empty() || s.ends_with('.'))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_host_matches_or_is_subdomain_of() {
        for (host, domain, expected) in [
            ("abc.com", "abc.com", true),
            ("www.abc.com", "abc.com", true),
            ("a.b.abc.com", "abc.com", true),
            ("notabc.com", "abc.com", false),
            ("bc.com", "abc.com", false),
            ("abc.com.evil.org", "abc.com", false),
            ("", "abc.com", false),
            // an empty domain never matches
            ("", "", false),
            ("abc.com", "", false),
            ("abc.", "", false),
        ] {
            assert_eq!(
                host_matches_or_is_subdomain_of(host, domain),
                expected,
                "host `{host}` against domain `{domain}`"
            );
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
            ),
        ];

        for (url, scheme, domain, port) in examples {
            let actual: SchemeDomainPort = (*url).into();
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
            // wildcard without a dot only matches on a label boundary
            ("http://notabc.com", vec!["*abc.com"], false),
            ("http://bc.com", vec!["*abc.com"], false),
            ("http://abc.com", vec!["*abc.com"], true),
            ("http://www.abc.com", vec!["*abc.com"], true),
            ("http://bc.com", vec!["*.abc.com"], false),
            ("http://notabc.com", vec!["*.abc.com"], false),
            ("http://www.abc.com", vec!["*.abc.com"], true),
            ("http://abc.com", vec!["*."], false),
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
}
