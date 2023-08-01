//! Contains the user agent normalization code
//!
//! This module is responsible for taking the user agent string parsing it and filling in
//! the browser, os and device information in the event.
//!

use std::borrow::Cow;
use std::fmt::Write;

use crate::protocol::{
    BrowserContext, Context, Contexts, DefaultContext, DeviceContext, Event, FromUserAgentInfo,
    OsContext,
};
use crate::types::Annotated;
use crate::user_agent::RawUserAgentInfo;

pub fn normalize_user_agent(event: &mut Event) {
    let headers = match event
        .request
        .value()
        .and_then(|request| request.headers.value())
    {
        Some(headers) => headers,
        None => return,
    };

    let user_agent_info = RawUserAgentInfo::from_headers(headers);

    let contexts = event.contexts.get_or_insert_with(|| Contexts::new());
    normalize_user_agent_info_generic(contexts, &event.platform, &user_agent_info);
}

pub fn normalize_user_agent_info_generic(
    contexts: &mut Contexts,
    platform: &Annotated<String>,
    user_agent_info: &RawUserAgentInfo<&str>,
) {
    if !contexts.contains::<BrowserContext>() {
        if let Some(browser_context) = BrowserContext::from_hints_or_ua(user_agent_info) {
            contexts.add(browser_context);
        }
    }

    if !contexts.contains::<DeviceContext>() {
        if let Some(device_context) = DeviceContext::from_hints_or_ua(user_agent_info) {
            contexts.add(device_context);
        }
    }

    // avoid conflicts with OS-context sent by a serverside SDK by using `contexts.client_os`
    // instead of `contexts.os`. This is then preferred by the UI to show alongside device and
    // browser context.
    //
    // Why not move the existing `contexts.os` into a different key on conflicts? Because we still
    // want to index (derive tags from) the SDK-sent context.
    let os_context_key = match platform.as_str() {
        Some("javascript") => OsContext::default_key(),
        _ => "client_os",
    };
    if !contexts.contains_key(os_context_key) {
        if let Some(os_context) = OsContext::from_hints_or_ua(user_agent_info) {
            contexts.insert(os_context_key.to_owned(), Context::Os(Box::new(os_context)));
        }
    }
}

pub fn is_known(family: &str) -> bool {
    family != "Other"
}

pub fn get_version(
    major: &Option<Cow<'_, str>>,
    minor: &Option<Cow<'_, str>>,
    patch: &Option<Cow<'_, str>>,
) -> Option<String> {
    let mut version = major.as_ref()?.to_string();

    if let Some(minor) = minor {
        write!(version, ".{minor}").ok();
        if let Some(patch) = patch {
            write!(version, ".{patch}").ok();
        }
    }

    Some(version)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutils::{self, assert_annotated_snapshot};

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
        assert!(event.contexts.value().unwrap().0.is_empty());
    }

    #[test]
    fn test_browser_context() {
        let ua = "Mozilla/5.0 (-; -; -) - Chrome/18.0.1025.133 Mobile Safari/535.19";

        let mut event = testutils::get_event_with_user_agent(ua);
        normalize_user_agent(&mut event);
        assert_annotated_snapshot!(event.contexts, @r#"
        {
          "browser": {
            "name": "Chrome Mobile",
            "version": "18.0.1025",
            "type": "browser"
          }
        }
        "#);
    }

    #[test]
    fn test_os_context() {
        let ua = "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) - -";

        let mut event = testutils::get_event_with_user_agent(ua);
        normalize_user_agent(&mut event);
        assert_annotated_snapshot!(event.contexts, @r#"
        {
          "client_os": {
            "name": "Windows",
            "version": "7",
            "type": "os"
          }
        }
        "#);
    }

    #[test]
    fn test_os_context_short_version() {
        let ua = "Mozilla/5.0 (iPhone; CPU iPhone OS 12_1 like Mac OS X) - (-)";
        let mut event = testutils::get_event_with_user_agent(ua);
        normalize_user_agent(&mut event);
        assert_annotated_snapshot!(event.contexts, @r#"
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
        "#);
    }

    #[test]
    fn test_os_context_full_version() {
        let ua = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_4) - (-)";
        let mut event = testutils::get_event_with_user_agent(ua);
        normalize_user_agent(&mut event);
        assert_annotated_snapshot!(event.contexts, @r#"
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
        "#);
    }

    #[test]
    fn test_device_context() {
        let ua = "- (-; -; Galaxy Nexus Build/IMM76B) - (-) ";

        let mut event = testutils::get_event_with_user_agent(ua);
        normalize_user_agent(&mut event);
        assert_annotated_snapshot!(event.contexts, @r#"
        {
          "device": {
            "family": "Samsung Galaxy Nexus",
            "model": "Galaxy Nexus",
            "brand": "Samsung",
            "type": "device"
          }
        }
        "#);
    }

    #[test]
    fn test_all_contexts() {
        let mut event = testutils::get_event_with_user_agent(GOOD_UA);
        normalize_user_agent(&mut event);
        assert_annotated_snapshot!(event.contexts, @r#"
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
        "#);
    }

    #[test]
    fn test_user_agent_does_not_override_prefilled() {
        let mut event = testutils::get_event_with_user_agent(GOOD_UA);
        let mut contexts = Contexts::new();
        contexts.add(BrowserContext {
            name: Annotated::from("BR_FAMILY".to_string()),
            version: Annotated::from("BR_VERSION".to_string()),
            ..BrowserContext::default()
        });
        contexts.add(DeviceContext {
            family: Annotated::from("DEV_FAMILY".to_string()),
            model: Annotated::from("DEV_MODEL".to_string()),
            brand: Annotated::from("DEV_BRAND".to_string()),
            ..DeviceContext::default()
        });
        contexts.add(OsContext {
            name: Annotated::from("OS_FAMILY".to_string()),
            version: Annotated::from("OS_VERSION".to_string()),
            ..OsContext::default()
        });

        event.contexts = Annotated::new(contexts);

        normalize_user_agent(&mut event);
        assert_annotated_snapshot!(event.contexts, @r#"
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
        "#);
    }
}
