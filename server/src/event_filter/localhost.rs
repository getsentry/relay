//! Implements filtering for events originating from the localhost

use crate::actors::project::FilterConfig;
use semaphore_general::protocol::Event;

const LOCAL_IPS: &'static [&str] = &["127.0.0.1", "::1"];
const LOCAL_DOMAINS: &'static [&str] = &["127.0.0.1", "localhost"];

/// Filters events originating from the local host
pub fn should_filter(event: &Event, config: &FilterConfig) -> Result<(), String> {
    if !config.is_enabled {
        return Ok(());
    }

    if let Some(ip_addr) = get_ip_addr(event) {
        for &local_ip in LOCAL_IPS {
            if local_ip == ip_addr {
                return Err("local ip".to_string());
            }
        }
    }
    if let Some(domain) = get_domain(event) {
        for &local_domain in LOCAL_DOMAINS {
            if local_domain == domain {
                return Err("local domain".to_string());
            }
        }
    }

    return Ok(());
}

fn get_ip_addr(event: &Event) -> Option<&str> {
    event
        .user
        .value()
        .and_then(|user| user.ip_address.value())
        .map(|ip_address| ip_address.as_ref())
}

fn get_domain(event: &Event) -> Option<&str> {
    event
        .request
        .value()
        .and_then(|req| req.url.value())
        .map(|the_string| the_string.as_str())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event_filter::util::test_utils::*;
    use semaphore_general::protocol::{IpAddr, Request, User};
    use semaphore_general::types::Annotated;

    fn get_event_with_ip_addr(val: &str) -> Event {
        Event {
            user: Annotated::from(User {
                ip_address: Annotated::from(IpAddr(val.to_string())),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn get_event_with_domain(val: &str) -> Event {
        Event {
            request: Annotated::from(Request {
                url: Annotated::from(val.to_string()),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn it_should_not_filter_events_if_filter_is_disabled() {
        for event in [
            get_event_with_ip_addr("127.0.0.1"),
            get_event_with_domain("localhost"),
        ]
        .iter()
        {
            let filter_result = should_filter(&event, &get_f_config(false));
            assert_eq!(
                filter_result,
                Ok(()),
                "Event filtered although filter should have been disabled."
            );
        }
    }

    #[test]
    fn it_should_filter_events_with_local_ip() {
        for ip_addr in ["127.0.0.1", "::1"].iter() {
            let event = get_event_with_ip_addr(ip_addr);
            let filter_result = should_filter(&event, &get_f_config(true));
            assert_ne!(
                filter_result,
                Ok(()),
                "Failed to filter address '{}'",
                ip_addr
            );
        }
    }

    #[test]
    fn it_should_not_filter_events_with_non_local_ip() {
        for ip_addr in ["133.12.12.1", "2001:db8:0:0:0:ff00:42:8329"].iter() {
            let event = get_event_with_ip_addr(ip_addr);
            let filter_result = should_filter(&event, &get_f_config(true));
            assert_eq!(
                filter_result,
                Ok(()),
                "Filtered valid ip address '{}'",
                ip_addr
            );
        }
    }

    #[test]
    fn it_should_not_filter_events_with_missing_ip_or_domains() {
        let event = Event {
            ..Default::default()
        };
        let filter_result = should_filter(&event, &get_f_config(true));
        assert_eq!(
            filter_result,
            Ok(()),
            "Filtered event with no ip address and no domain."
        );
    }

    #[test]
    fn it_should_filter_events_with_local_domains() {
        for domain in ["127.0.0.1", "localhost"].iter() {
            let event = get_event_with_domain(domain);
            let filter_result = should_filter(&event, &get_f_config(true));
            assert_ne!(
                filter_result,
                Ok(()),
                "Failed to filter domain '{}'",
                domain
            );
        }
    }

    #[test]
    fn it_should_not_filter_events_with_non_local_domains() {
        for domain in ["my.dom.com", "123.123.123.44"].iter() {
            let event = get_event_with_domain(domain);
            let filter_result = should_filter(&event, &get_f_config(true));
            assert_eq!(
                filter_result,
                Ok(()),
                "Filtered perfectly valid domain '{}'",
                domain
            );
        }
    }
}
