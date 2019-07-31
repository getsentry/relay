//! Contains the user agent normalization code
//!
//! This module is responsible for taking the user agent string parsing it and filling in
//! the browser, os and device information in the event.
//!

use uaparser::{UserAgent, OS};

use crate::protocol::{BrowserContext, Context, Contexts, DeviceContext, Event, OsContext};
use crate::types::Annotated;
use crate::user_agent::{get_user_agent, parse_device, parse_os, parse_user_agent};

pub fn normalize_user_agent(event: &mut Event) {
    let user_agent = match get_user_agent(event) {
        Some(ua) => ua,
        None => return,
    };

    let device = parse_device(user_agent);
    let os = parse_os(user_agent);
    let ua = parse_user_agent(user_agent);

    if !is_known(ua.family.as_str())
        && !is_known(device.family.as_str())
        && !is_known(os.family.as_str())
    {
        return; // no useful information in the user agent
    }

    let contexts = event.contexts.get_or_insert_with(|| Contexts::new());

    if is_known(ua.family.as_str()) && !contexts.contains_key(BrowserContext::key()) {
        let version = get_browser_version(&ua);
        contexts.add(Context::Browser(Box::new(BrowserContext {
            name: Annotated::from(ua.family),
            version: Annotated::from(version),
            ..BrowserContext::default()
        })));
    }

    if is_known(device.family.as_str()) && !contexts.contains_key(DeviceContext::key()) {
        contexts.add(Context::Device(Box::new(DeviceContext {
            family: Annotated::from(device.family),
            model: Annotated::from(device.model),
            brand: Annotated::from(device.brand),
            ..DeviceContext::default()
        })));
    }

    if is_known(os.family.as_str()) && !contexts.contains_key(OsContext::key()) {
        let version = get_os_version(&os);
        contexts.add(Context::Os(Box::new(OsContext {
            name: Annotated::from(os.family),
            version: Annotated::from(version),
            ..OsContext::default()
        })));
    }
}

fn is_known(family: &str) -> bool {
    return family != "Other";
}
fn _v(value: &Option<String>) -> &str {
    match value {
        None => "",
        Some(val) => val.as_str(),
    }
}
fn get_os_version(os: &OS) -> String {
    format!("{}.{}.{}", _v(&os.major), _v(&os.minor), _v(&os.patch))
}
fn get_browser_version(ua: &UserAgent) -> String {
    format!("{}.{}.{}", _v(&ua.major), _v(&ua.minor), _v(&ua.patch))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutils;

    const GOOD_UA: &str =
            "Mozilla/5.0 (Linux; Android 4.0.4; Galaxy Nexus Build/IMM76B) AppleWebKit/535.19 (KHTML, like Gecko) Chrome/18.0.1025.133 Mobile Safari/535.19";

    fn get_context<'a>(event: &'a Event, idx: &str) -> Option<&'a Context> {
        Some(&event.contexts.value()?.get(idx)?.value()?.0)
    }

    #[test]
    fn test_events_without_user_agents_should_not_add_contexts() {
        let mut event = Event::default();
        normalize_user_agent(&mut event);

        assert_eq!(event.contexts.value(), None);
    }

    #[test]
    fn test_events_with_unrecognizable_user_agents_should_not_add_contexts() {
        let mut event = testutils::get_event_with_user_agent("a dont no");
        normalize_user_agent(&mut event);

        assert_eq!(event.contexts.value(), None);
    }

    #[test]
    fn test_a_user_agent_with_browser_information_fills_the_browser_context_in() {
        let ua = "Mozilla/5.0 (-; -; -) - Chrome/18.0.1025.133 Mobile Safari/535.19";

        let mut event = testutils::get_event_with_user_agent(ua);

        normalize_user_agent(&mut event);

        let browser_context = get_context(&event, BrowserContext::key());
        assert_ne!(browser_context, None);
        if let Some(Context::Browser(bc)) = browser_context {
            assert_eq!(bc.name.value(), Some(&"Chrome Mobile".to_string()));
            assert_eq!(bc.version.value(), Some(&"18.0.1025".to_string()));
        } else {
            panic!("Could not find the browser context");
        }
        let device_context = get_context(&event, DeviceContext::key());
        assert_eq!(device_context, None);
        let os_context = get_context(&event, OsContext::key());
        assert_eq!(os_context, None);
    }

    #[test]
    fn test_a_user_agent_with_os_information_fills_the_os_context_in() {
        let ua = "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) - -";

        let mut event = testutils::get_event_with_user_agent(ua);

        normalize_user_agent(&mut event);

        let browser_context = get_context(&event, BrowserContext::key());
        assert_eq!(browser_context, None);
        let device_context = get_context(&event, DeviceContext::key());
        assert_eq!(device_context, None);
        let os_context = get_context(&event, OsContext::key());
        assert_ne!(os_context, None);
        if let Some(Context::Os(os)) = os_context {
            assert_eq!(os.name.value(), Some(&"Windows 7".to_string()));
            assert_eq!(os.version.value(), Some(&"..".to_string()));
        } else {
            panic!("Could not find the os context");
        }
    }

    #[test]
    fn test_a_user_agent_with_device_information_fills_the_device_context_in() {
        let ua = "- (-; -; Galaxy Nexus Build/IMM76B) - (-) ";

        let mut event = testutils::get_event_with_user_agent(ua);

        normalize_user_agent(&mut event);

        let browser_context = get_context(&event, BrowserContext::key());
        assert_eq!(browser_context, None);
        let device_context = get_context(&event, DeviceContext::key());
        assert_ne!(device_context, None);
        if let Some(Context::Device(dc)) = device_context {
            assert_eq!(dc.family.value(), Some(&"Samsung Galaxy Nexus".to_string()));
            assert_eq!(dc.model.value(), Some(&"Galaxy Nexus".to_string()));
            assert_eq!(dc.brand.value(), Some(&"Samsung".to_string()));
        } else {
            panic!("Could not find the device context");
        }
        let os_context = get_context(&event, OsContext::key());
        assert_eq!(os_context, None);
    }

    #[test]
    fn test_a_user_agent_with_all_info_fills_everything_in() {
        let mut event = testutils::get_event_with_user_agent(GOOD_UA);

        normalize_user_agent(&mut event);

        let browser_context = get_context(&event, BrowserContext::key());
        assert_ne!(browser_context, None);
        if let Some(Context::Browser(bc)) = browser_context {
            assert_eq!(bc.name.value(), Some(&"Chrome Mobile".to_string()));
            assert_eq!(bc.version.value(), Some(&"18.0.1025".to_string()));
        } else {
            panic!("Could not find the browser context");
        }
        let device_context = get_context(&event, DeviceContext::key());
        assert_ne!(device_context, None);
        if let Some(Context::Device(dc)) = device_context {
            assert_eq!(dc.family.value(), Some(&"Samsung Galaxy Nexus".to_string()));
            assert_eq!(dc.model.value(), Some(&"Galaxy Nexus".to_string()));
            assert_eq!(dc.brand.value(), Some(&"Samsung".to_string()));
        } else {
            panic!("Could not find the device context");
        }
        let os_context = get_context(&event, OsContext::key());
        assert_ne!(os_context, None);
        if let Some(Context::Os(os)) = os_context {
            assert_eq!(os.name.value(), Some(&"Android".to_string()));
            assert_eq!(os.version.value(), Some(&"4.0.4".to_string()));
        } else {
            panic!("Could not find the os context");
        }
    }

    #[test]
    fn test_user_agent_does_not_override_already_pre_filled_event_info() {
        let mut event = testutils::get_event_with_user_agent(GOOD_UA);
        let mut contexts = Contexts::new();
        contexts.add(Context::Browser(Box::new(BrowserContext {
            name: Annotated::from("BR_FAMILY".to_string()),
            version: Annotated::from("BR_VERSION".to_string()),
            ..BrowserContext::default()
        })));
        contexts.add(Context::Device(Box::new(DeviceContext {
            family: Annotated::from("DEV_FAMILY".to_string()),
            model: Annotated::from("DEV_MODEL".to_string()),
            brand: Annotated::from("DEV_BRAND".to_string()),
            ..DeviceContext::default()
        })));
        contexts.add(Context::Os(Box::new(OsContext {
            name: Annotated::from("OS_FAMILY".to_string()),
            version: Annotated::from("OS_VERSION".to_string()),
            ..OsContext::default()
        })));

        event.contexts = Annotated::new(contexts);

        normalize_user_agent(&mut event);

        let browser_context = get_context(&event, BrowserContext::key());
        assert_ne!(browser_context, None);
        if let Some(Context::Browser(bc)) = browser_context {
            assert_eq!(bc.name.value(), Some(&"BR_FAMILY".to_string()));
            assert_eq!(bc.version.value(), Some(&"BR_VERSION".to_string()));
        } else {
            panic!("Could not find the browser context");
        }
        let device_context = get_context(&event, DeviceContext::key());
        assert_ne!(device_context, None);
        if let Some(Context::Device(dc)) = device_context {
            assert_eq!(dc.family.value(), Some(&"DEV_FAMILY".to_string()));
            assert_eq!(dc.model.value(), Some(&"DEV_MODEL".to_string()));
            assert_eq!(dc.brand.value(), Some(&"DEV_BRAND".to_string()));
        } else {
            panic!("Could not find the device context");
        }
        let os_context = get_context(&event, OsContext::key());
        assert_ne!(os_context, None);
        if let Some(Context::Os(os)) = os_context {
            assert_eq!(os.name.value(), Some(&"OS_FAMILY".to_string()));
            assert_eq!(os.version.value(), Some(&"OS_VERSION".to_string()));
        } else {
            panic!("Could not find the os context");
        }
    }
}
