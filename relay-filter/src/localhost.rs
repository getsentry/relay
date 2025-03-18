//! Implements filtering for events originating from the localhost

use crate::{FilterConfig, FilterStatKey, Filterable};

const LOCAL_IPS: &[&str] = &["127.0.0.1", "::1"];
const LOCAL_DOMAINS: &[&str] = &["127.0.0.1", "localhost"];

const FORWARDED_HOST_HEADER: &str = "X-Forwarded-Host";

const HOST_HEADER: &str = "Host";

/// Check if the event originates from the local host.
fn matches<F: Filterable>(item: &F) -> bool {
    if let Some(ip_addr) = item.ip_addr() {
        if LOCAL_IPS.iter().any(|ip| *ip == ip_addr) {
            return true;
        }
    }

    if let Some(url) = item.url() {
        if url.scheme() == "file" {
            return true;
        }

        if let Some(host) = url.host_str() {
            if LOCAL_DOMAINS
                .iter()
                .any(|d| host_matches_or_is_subdomain_of(host, d))
            {
                return true;
            }
        }
    }

    for header_name in [HOST_HEADER, FORWARDED_HOST_HEADER] {
        if let Some(header) = item.header(header_name) {
            if let Some(domain_part) = header.split(":").next() {
                // header values here will usually look like "localhost:3000" or "127.0.0.1:8080"
                if LOCAL_DOMAINS.iter().any(|domain| domain_part == *domain) {
                    return true;
                }
            }
        }
    }

    false
}

fn host_matches_or_is_subdomain_of(host: &str, domain: &str) -> bool {
    host.strip_suffix(domain)
        .is_some_and(|s| s.is_empty() || s.ends_with('.'))
}

/// Filters events originating from the local host.
pub fn should_filter<F: Filterable>(item: &F, config: &FilterConfig) -> Result<(), FilterStatKey> {
    if !config.is_enabled {
        return Ok(());
    }
    if matches(item) {
        return Err(FilterStatKey::Localhost);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use relay_event_schema::protocol::{
        Event, HeaderName, HeaderValue, Headers, IpAddr, PairList, Request, User,
    };
    use relay_protocol::Annotated;

    use super::*;

    fn get_event_with_ip_addr(val: &str) -> Event {
        Event {
            user: Annotated::from(User {
                ip_address: Annotated::from(IpAddr(val.to_string())),
                ..User::default()
            }),
            ..Event::default()
        }
    }

    fn get_event_with_domain(val: &str) -> Event {
        Event {
            request: Annotated::from(Request {
                url: Annotated::from(format!("http://{val}:8080/")),
                ..Request::default()
            }),
            ..Event::default()
        }
    }

    fn get_event_with_url(val: &str) -> Event {
        Event {
            request: Annotated::from(Request {
                url: Annotated::from(val.to_string()),
                ..Request::default()
            }),
            ..Event::default()
        }
    }

    fn get_event_with_header(key: &str, value: &str) -> Event {
        Event {
            request: Annotated::from(Request {
                headers: Annotated::from(Headers(PairList(vec![Annotated::new((
                    Annotated::new(HeaderName::new(key)),
                    Annotated::new(HeaderValue::new(value)),
                ))]))),
                ..Default::default()
            }),
            ..Event::default()
        }
    }

    #[test]
    fn test_dont_filter_when_disabled() {
        for event in &[
            get_event_with_ip_addr("127.0.0.1"),
            get_event_with_domain("localhost"),
        ] {
            let filter_result = should_filter(event, &FilterConfig { is_enabled: false });
            assert_eq!(
                filter_result,
                Ok(()),
                "Event filtered although filter should have been disabled."
            );
        }
    }

    #[test]
    fn test_filter_local_ip() {
        for ip_addr in &["127.0.0.1", "::1"] {
            let event = get_event_with_ip_addr(ip_addr);
            let filter_result = should_filter(&event, &FilterConfig { is_enabled: true });
            assert_ne!(
                filter_result,
                Ok(()),
                "Failed to filter address '{ip_addr}'"
            );
        }
    }

    #[test]
    fn test_dont_filter_non_local_ip() {
        for ip_addr in &["133.12.12.1", "2001:db8:0:0:0:ff00:42:8329"] {
            let event = get_event_with_ip_addr(ip_addr);
            let filter_result = should_filter(&event, &FilterConfig { is_enabled: true });
            assert_eq!(
                filter_result,
                Ok(()),
                "Filtered valid ip address '{ip_addr}'"
            );
        }
    }

    #[test]
    fn test_dont_filter_missing_ip_or_domains() {
        let event = Event::default();
        let filter_result = should_filter(&event, &FilterConfig { is_enabled: true });
        assert_eq!(
            filter_result,
            Ok(()),
            "Filtered event with no ip address and no domain."
        );
    }

    #[test]
    fn test_filter_local_domains() {
        for domain in &[
            "127.0.0.1",
            "localhost",
            "foo.localhost",
            "foo.bar.baz.localhost",
        ] {
            let event = get_event_with_domain(domain);
            let filter_result = should_filter(&event, &FilterConfig { is_enabled: true });
            assert_ne!(filter_result, Ok(()), "Failed to filter domain '{domain}'");
        }
    }

    #[test]
    fn test_dont_filter_non_local_domains() {
        for domain in &[
            "my.dom.com",
            "123.123.123.44",
            "localhost.com",
            "foolocalhost",
            "localhostbar",
            "alocalhostgoesintoabar",
        ] {
            let event = get_event_with_domain(domain);
            let filter_result = should_filter(&event, &FilterConfig { is_enabled: true });
            assert_eq!(
                filter_result,
                Ok(()),
                "Filtered perfectly valid domain '{domain}'"
            );
        }
    }

    #[test]
    fn test_filter_file_urls() {
        let url = "file:///Users/Maisey/work/squirrelchasers/src/leaderboard.html";
        let event = get_event_with_url(url);
        let filter_result = should_filter(&event, &FilterConfig { is_enabled: true });
        assert_ne!(
            filter_result,
            Ok(()),
            "Failed to filter event with url '{url}'"
        );
    }

    #[test]
    fn test_dont_filter_non_file_urls() {
        let url = "http://www.squirrelchasers.com/leaderboard";
        let event = get_event_with_url(url);
        let filter_result = should_filter(&event, &FilterConfig { is_enabled: true });
        assert_eq!(filter_result, Ok(()), "Filtered valid url '{url}'");
    }

    #[test]
    fn test_filter_forwarded_host_header() {
        for host_value in ["localhost:3000", "127.0.0.1:3000", "localhost"] {
            let event = get_event_with_header(FORWARDED_HOST_HEADER, host_value);
            let filter_result = should_filter(&event, &FilterConfig { is_enabled: true });
            assert_eq!(filter_result, Err(FilterStatKey::Localhost))
        }
    }

    #[test]
    fn test_filter_request_host_header() {
        for host_value in ["localhost:3000", "127.0.0.1:3000", "localhost"] {
            let event = get_event_with_header(HOST_HEADER, host_value);
            let filter_result = should_filter(&event, &FilterConfig { is_enabled: true });
            assert_eq!(filter_result, Err(FilterStatKey::Localhost))
        }
    }

    #[test]
    fn test_filter_request_subdomain_host_header() {
        for domain in ["localhost.sentry.io", "localhost.sentry.io:3000"] {
            let event = get_event_with_header(HOST_HEADER, domain);
            let filter_result = should_filter(&event, &FilterConfig { is_enabled: true });
            assert_eq!(filter_result, Ok(()))
        }
    }

    #[test]
    fn test_filter_request_subdomain_forwarded_host_header() {
        for domain in ["localhost.sentry.io", "localhost.sentry.io:3000"] {
            let event = get_event_with_header(FORWARDED_HOST_HEADER, domain);
            let filter_result = should_filter(&event, &FilterConfig { is_enabled: true });
            assert_eq!(filter_result, Ok(()))
        }
    }
}
