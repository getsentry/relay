//! Implements filtering for events originating from the localhost
use relay_event_schema::protocol::Event;
use url::Url;

use crate::{FilterConfig, FilterStatKey};

const LOCAL_IPS: &[&str] = &["127.0.0.1", "::1"];
const LOCAL_DOMAINS: &[&str] = &["127.0.0.1", "localhost"];

/// Check if the event originates from the local host.
pub fn matches(event: &Event) -> bool {
    if let Some(ip_addr) = get_ip_addr(event) {
        for &local_ip in LOCAL_IPS {
            if local_ip == ip_addr {
                return true;
            }
        }
    }

    if let Some(url) = get_url(event) {
        if let Some(host) = url.host_str() {
            for &local_domain in LOCAL_DOMAINS {
                if host == local_domain {
                    return true;
                }
            }
        }
        if url.scheme() == "file" {
            return true;
        }
    }

    false
}

/// Filters events originating from the local host.
pub fn should_filter(event: &Event, config: &FilterConfig) -> Result<(), FilterStatKey> {
    if !config.is_enabled {
        return Ok(());
    }
    if matches(event) {
        return Err(FilterStatKey::Localhost);
    }
    Ok(())
}

fn get_ip_addr(event: &Event) -> Option<&str> {
    let user = event.user.value()?;
    Some(user.ip_address.value()?.as_ref())
}

fn get_url(event: &Event) -> Option<Url> {
    let url_str = event.request.value()?.url.value()?;
    Url::parse(url_str).ok()
}

#[cfg(test)]
mod tests {
    use relay_event_schema::protocol::{IpAddr, Request, User};
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
        for domain in &["127.0.0.1", "localhost"] {
            let event = get_event_with_domain(domain);
            let filter_result = should_filter(&event, &FilterConfig { is_enabled: true });
            assert_ne!(filter_result, Ok(()), "Failed to filter domain '{domain}'");
        }
    }

    #[test]
    fn test_dont_filter_non_local_domains() {
        for domain in &["my.dom.com", "123.123.123.44"] {
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
}
