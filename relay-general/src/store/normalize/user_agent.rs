//! Contains the user agent normalization code
//!
//! This module is responsible for taking the user agent string parsing it and filling in
//! the browser, os and device information in the event.
//!

use std::fmt::Write;

use crate::protocol::{BrowserContext, Context, Contexts, DeviceContext, Event, OsContext};
use crate::types::Annotated;
use crate::user_agent::{get_user_agent_generic, parse_device, parse_os, parse_user_agent};

pub fn normalize_user_agent(event: &mut Event) {
    let user_agent = match get_user_agent_generic(&event.request) {
        Some(ua) => ua,
        None => return,
    };

    if let Some(contexts) = event.contexts.value_mut() {
        // If a contexts object exists we modify in place.
        normalize_user_agent_generic(contexts, &event.platform, user_agent);
    } else {
        // If a contexts object does not exist we create a new one and attempt to populate
        // it.  If we didn't write any data to our new contexts instance we can throw it out
        // and leave the existing contexts value as "None".
        let mut contexts = Contexts::new();
        normalize_user_agent_generic(&mut contexts, &event.platform, user_agent);

        if !contexts.is_empty() {
            event.contexts.set_value(Some(contexts));
        }
    }
}

pub fn normalize_user_agent_generic(
    contexts: &mut Contexts,
    platform: &Annotated<String>,
    user_agent: &str,
) {
    if !contexts.contains_key(BrowserContext::default_key()) {
        if let Some(browser_context) = user_agent_as_browser_context(user_agent) {
            contexts.add(Context::Browser(Box::new(browser_context)));
        }
    }

    if !contexts.contains_key(DeviceContext::default_key()) {
        if let Some(device_context) = user_agent_as_device_context(user_agent) {
            contexts.add(Context::Device(Box::new(device_context)));
        }
    }

    let os_context_key = match platform.as_str() {
        Some("javascript") => OsContext::default_key(),
        _ => "client_os",
    };
    if !contexts.contains_key(os_context_key) {
        if let Some(os_context) = user_agent_as_os_context(user_agent) {
            contexts.insert(
                os_context_key.to_owned(),
                Annotated::new(Context::Os(Box::new(os_context)).into()),
            );
        }
    }
}

fn user_agent_as_browser_context(user_agent: &str) -> Option<BrowserContext> {
    let browser = parse_user_agent(user_agent);

    if !is_known(browser.family.as_str()) {
        return None;
    }

    Some(BrowserContext {
        name: Annotated::from(browser.family),
        version: Annotated::from(get_version(&browser.major, &browser.minor, &browser.patch)),
        ..BrowserContext::default()
    })
}

fn user_agent_as_device_context(user_agent: &str) -> Option<DeviceContext> {
    let device = parse_device(user_agent);

    if !is_known(device.family.as_str()) {
        return None;
    }

    Some(DeviceContext {
        family: Annotated::from(device.family),
        model: Annotated::from(device.model),
        brand: Annotated::from(device.brand),
        ..DeviceContext::default()
    })
}

fn user_agent_as_os_context(user_agent: &str) -> Option<OsContext> {
    let os = parse_os(user_agent);

    if !is_known(os.family.as_str()) {
        return None;
    }

    Some(OsContext {
        name: Annotated::from(os.family),
        version: Annotated::from(get_version(&os.major, &os.minor, &os.patch)),
        ..OsContext::default()
    })
}

fn is_known(family: &str) -> bool {
    family != "Other"
}

fn get_version(
    major: &Option<String>,
    minor: &Option<String>,
    patch: &Option<String>,
) -> Option<String> {
    let mut version = major.clone()?;

    if let Some(minor) = minor {
        write!(version, ".{}", minor).ok();
        if let Some(patch) = patch {
            write!(version, ".{}", patch).ok();
        }
    }

    Some(version)
}

#[cfg(test)]
mod tests {
    use crate::testutils::{self, assert_annotated_snapshot};

    use super::*;

    const GOOD_UA: &str =
            "Mozilla/5.0 (Linux; Android 4.0.4; Galaxy Nexus Build/IMM76B) AppleWebKit/535.19 (KHTML, like Gecko) Chrome/18.0.1025.133 Mobile Safari/535.19";

    #[test]
    fn test_version_none() {
        assert_eq!(get_version(&None, &None, &None), None);
    }

    #[test]
    fn test_version_major() {
        assert_eq!(
            get_version(&Some("X".into()), &None, &None),
            Some("X".into())
        )
    }

    #[test]
    fn test_version_major_minor() {
        assert_eq!(
            get_version(&Some("X".into()), &Some("Y".into()), &None),
            Some("X.Y".into())
        )
    }

    #[test]
    fn test_version_major_minor_patch() {
        assert_eq!(
            get_version(&Some("X".into()), &Some("Y".into()), &Some("Z".into())),
            Some("X.Y.Z".into())
        )
    }

    #[test]
    fn test_verison_missing_minor() {
        assert_eq!(
            get_version(&Some("X".into()), &None, &Some("Z".into())),
            Some("X".into())
        )
    }

    #[test]
    fn test_skip_no_user_agent() {
        let mut event = Event::default();
        normalize_user_agent(&mut event);
        assert_eq!(event.contexts.value(), None);
    }

    #[test]
    fn test_skip_unrecognizable_user_agent() {
        let mut event = testutils::get_event_with_user_agent("a dont no");
        normalize_user_agent(&mut event);
        assert_eq!(event.contexts.value(), None);
    }

    #[test]
    fn test_browser_context() {
        let ua = "Mozilla/5.0 (-; -; -) - Chrome/18.0.1025.133 Mobile Safari/535.19";

        let mut event = testutils::get_event_with_user_agent(ua);
        normalize_user_agent(&mut event);
        assert_annotated_snapshot!(event.contexts, @r###"
        {
          "browser": {
            "name": "Chrome Mobile",
            "version": "18.0.1025",
            "type": "browser"
          }
        }
        "###);
    }

    #[test]
    fn test_os_context() {
        let ua = "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) - -";

        let mut event = testutils::get_event_with_user_agent(ua);
        normalize_user_agent(&mut event);
        assert_annotated_snapshot!(event.contexts, @r###"
        {
          "client_os": {
            "name": "Windows",
            "version": "7",
            "type": "os"
          }
        }
        "###);
    }

    #[test]
    fn test_os_context_short_version() {
        let ua = "Mozilla/5.0 (iPhone; CPU iPhone OS 12_1 like Mac OS X) - (-)";
        let mut event = testutils::get_event_with_user_agent(ua);
        normalize_user_agent(&mut event);
        assert_annotated_snapshot!(event.contexts, @r###"
        {
          "browser": {
            "name": "Mobile Safari UI/WKWebView",
            "type": "browser"
          },
          "client_os": {
            "name": "iOS",
            "version": "12.1",
            "type": "os"
          },
          "device": {
            "family": "iPhone",
            "model": "iPhone",
            "brand": "Apple",
            "type": "device"
          }
        }
        "###);
    }

    #[test]
    fn test_os_context_full_version() {
        let ua = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_4) - (-)";
        let mut event = testutils::get_event_with_user_agent(ua);
        normalize_user_agent(&mut event);
        assert_annotated_snapshot!(event.contexts, @r###"
        {
          "client_os": {
            "name": "Mac OS X",
            "version": "10.13.4",
            "type": "os"
          },
          "device": {
            "family": "Mac",
            "model": "Mac",
            "brand": "Apple",
            "type": "device"
          }
        }
        "###);
    }

    #[test]
    fn test_device_context() {
        let ua = "- (-; -; Galaxy Nexus Build/IMM76B) - (-) ";

        let mut event = testutils::get_event_with_user_agent(ua);
        normalize_user_agent(&mut event);
        assert_annotated_snapshot!(event.contexts, @r###"
        {
          "device": {
            "family": "Samsung Galaxy Nexus",
            "model": "Galaxy Nexus",
            "brand": "Samsung",
            "type": "device"
          }
        }
        "###);
    }

    #[test]
    fn test_all_contexts() {
        let mut event = testutils::get_event_with_user_agent(GOOD_UA);
        normalize_user_agent(&mut event);
        assert_annotated_snapshot!(event.contexts, @r###"
        {
          "browser": {
            "name": "Chrome Mobile",
            "version": "18.0.1025",
            "type": "browser"
          },
          "client_os": {
            "name": "Android",
            "version": "4.0.4",
            "type": "os"
          },
          "device": {
            "family": "Samsung Galaxy Nexus",
            "model": "Galaxy Nexus",
            "brand": "Samsung",
            "type": "device"
          }
        }
        "###);
    }

    #[test]
    fn test_user_agent_does_not_override_prefilled() {
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
        assert_annotated_snapshot!(event.contexts, @r###"
        {
          "browser": {
            "name": "BR_FAMILY",
            "version": "BR_VERSION",
            "type": "browser"
          },
          "client_os": {
            "name": "Android",
            "version": "4.0.4",
            "type": "os"
          },
          "device": {
            "family": "DEV_FAMILY",
            "model": "DEV_MODEL",
            "brand": "DEV_BRAND",
            "type": "device"
          },
          "os": {
            "name": "OS_FAMILY",
            "version": "OS_VERSION",
            "type": "os"
          }
        }
        "###);
    }
}
