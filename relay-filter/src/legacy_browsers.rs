//! Implements filtering for events originating from legacy browsers.

use std::collections::BTreeSet;

use relay_event_schema::protocol::Event;
use relay_ua::UserAgent;

use crate::{FilterStatKey, LegacyBrowser, LegacyBrowsersFilterConfig};

/// Checks if the event originates from legacy browsers.
pub fn matches(event: &Event, browsers: &BTreeSet<LegacyBrowser>) -> bool {
    if let Some(user_agent_string) = event.user_agent() {
        let user_agent = relay_ua::parse_user_agent(user_agent_string);

        // remap IE Mobile to IE (sentry python, filter compatibility)
        let family = match user_agent.family.as_ref() {
            "IE Mobile" => "IE",
            other => other,
        };

        if browsers.contains(&LegacyBrowser::Default) {
            return default_filter(family, &user_agent);
        }

        for browser_type in browsers {
            let should_filter = match browser_type {
                LegacyBrowser::IePre9 => filter_browser(family, &user_agent, "IE", |x| x <= 8),
                LegacyBrowser::Ie9 => filter_browser(family, &user_agent, "IE", |x| x == 9),
                LegacyBrowser::Ie10 => filter_browser(family, &user_agent, "IE", |x| x == 10),
                LegacyBrowser::Ie11 => filter_browser(family, &user_agent, "IE", |x| x == 11),
                LegacyBrowser::OperaMiniPre8 => {
                    filter_browser(family, &user_agent, "Opera Mini", |x| x < 8)
                }
                LegacyBrowser::OperaPre15 => {
                    filter_browser(family, &user_agent, "Opera", |x| x < 15)
                }
                LegacyBrowser::AndroidPre4 => {
                    filter_browser(family, &user_agent, "Android", |x| x < 4)
                }
                LegacyBrowser::SafariPre6 => {
                    filter_browser(family, &user_agent, "Safari", |x| x < 6)
                }
                LegacyBrowser::EdgePre79 => filter_browser(family, &user_agent, "Edge", |x| x < 79),
                LegacyBrowser::Unknown(_) => {
                    // unknown browsers should not be filtered
                    false
                }
                LegacyBrowser::Default => unreachable!(),
            };
            if should_filter {
                return true;
            }
        }
    }
    false
}

/// Filters events originating from legacy browsers.
pub fn should_filter(
    event: &Event,
    config: &LegacyBrowsersFilterConfig,
) -> Result<(), FilterStatKey> {
    if !config.is_enabled || config.browsers.is_empty() {
        return Ok(()); // globally disabled or no individual browser enabled
    }

    let browsers = &config.browsers;
    if matches(event, browsers) {
        Err(FilterStatKey::LegacyBrowsers)
    } else {
        Ok(())
    }
}

fn get_browser_major_version(user_agent: &UserAgent) -> Option<i32> {
    if let Some(browser_major_version_str) = &user_agent.major {
        if let Ok(browser_major_version) = browser_major_version_str.parse::<i32>() {
            return Some(browser_major_version);
        }
    }

    None
}

fn min_version(family: &str) -> Option<i32> {
    match family {
        "Chrome" => Some(0),
        "IE" => Some(10),
        "Firefox" => Some(0),
        "Safari" => Some(6),
        "Edge" => Some(0),
        "Opera" => Some(15),
        "Android" => Some(4),
        "Opera Mini" => Some(8),
        _ => None,
    }
}

fn default_filter(mapped_family: &str, user_agent: &UserAgent) -> bool {
    if let Some(browser_major_version) = get_browser_major_version(user_agent) {
        if let Some(min_version) = min_version(mapped_family) {
            if min_version > browser_major_version {
                return true;
            }
        }
    }
    false
}

fn filter_browser<F>(
    mapped_family: &str,
    user_agent: &UserAgent,
    family: &str,
    should_filter: F,
) -> bool
where
    F: FnOnce(i32) -> bool,
{
    if mapped_family == family {
        if let Some(browser_major_version) = get_browser_major_version(user_agent) {
            if should_filter(browser_major_version) {
                return true;
            }
        }
    }
    false
}

#[cfg(test)]
mod tests {
    const IE8_UA: &str = "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0)";
    const IE_MOBILE9_UA: &str =
        "Mozilla/5.0 (compatible; MSIE 9.0; Windows Phone OS 7.5; Trident/5.0; IEMobile/9.0; NOKIA; Lumia 710)";
    const IE9_UA: &str = "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 7.1; Trident/5.0)";
    const IE10_UA: &str = "Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; Trident/6.0)";
    const IE11_UA: &str = "Mozilla/5.0 (Windows NT 6.3; Trident/7.0; rv:11.0) like Gecko";
    const OPERA_MINI_PRE8_UA: &str =
        "Opera/9.80 (J2ME/MIDP; Opera Mini/7.0.32796/59.323; U; fr) Presto/2.12.423 Version/12.16";
    const OPERA_MINI_8_UA: &str =
        "Opera/9.80 (J2ME/MIDP; Opera Mini/8.0.35158/36.2534; U; en) Presto/2.12.423 Version/12.16";
    const OPERA_PRE15_UA: &str =
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.12 Safari/537.36 OPR/14.0.1116.4";
    const OPERA_15_UA: &str =
        "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/28.0.1500.45 Safari/537.36 OPR/15.0.1147.61 (Edition Next)";
    const ANDROID_PRE4_UA: &str =
        "Mozilla/5.0 (Linux; U; Android 3.2; nl-nl; GT-P6800 Build/HTJ85B) AppleWebKit/534.13 (KHTML, like Gecko) Version/4.0 Safari/534.13";
    const ANDROID_4_UA: &str =
        "Mozilla/5.0 (Linux; U; Android 4.1.1; en-gb; Build/KLP) AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 Safari/534.30";
    const SAFARI_PRE6_UA: &str =
        "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 1063; tr-DE) AppleWebKit/533.16 (KHTML like Gecko) Version/5.0 Safari/533.16";
    const SAFARI_6_UA: &str =
        "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10.17.4; en-GB) AppleWebKit/605.1.5 (KHTML, like Gecko) Version/6.0 Safari/605.1.5";
    const EDGE_ANDROID_118_UA: &str =
        "Mozilla/5.0 (Linux; Android 10; Pixel 3 XL) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.5993.80 Mobile Safari/537.36 EdgA/118.0.2088.58";
    const EDGE_79_UA: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3919.0 Safari/537.36 Edg/79.0.294.1";
    const EDGE_18_UA: &str =
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.102 Safari/537.36 Edge/18.19582";
    const EDGE_12_UA: &str =
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36 Edge/12.246";

    use std::collections::BTreeSet;

    use super::*;
    use crate::testutils;

    fn get_legacy_browsers_config(
        is_enabled: bool,
        legacy_browsers: &[LegacyBrowser],
    ) -> LegacyBrowsersFilterConfig {
        LegacyBrowsersFilterConfig {
            is_enabled,
            browsers: {
                let mut browsers = BTreeSet::<LegacyBrowser>::new();
                for elm in legacy_browsers {
                    browsers.insert(elm.clone());
                }
                browsers
            },
        }
    }

    #[test]
    fn test_dont_filter_when_disabled() {
        let evt = testutils::get_event_with_user_agent(IE8_UA);
        let filter_result = should_filter(
            &evt,
            &get_legacy_browsers_config(false, &[LegacyBrowser::Default]),
        );
        assert_eq!(
            filter_result,
            Ok(()),
            "Event filtered although filter should have been disabled"
        )
    }

    #[test]
    fn test_filter_default_browsers() {
        for old_user_agent in &[
            IE9_UA,
            IE_MOBILE9_UA,
            SAFARI_PRE6_UA,
            OPERA_PRE15_UA,
            ANDROID_PRE4_UA,
            OPERA_MINI_PRE8_UA,
        ] {
            let evt = testutils::get_event_with_user_agent(old_user_agent);
            let filter_result = should_filter(
                &evt,
                &get_legacy_browsers_config(true, &[LegacyBrowser::Default]),
            );
            assert_ne!(
                filter_result,
                Ok(()),
                "Default filter should have filtered User Agent\n{old_user_agent}"
            )
        }
    }

    #[test]
    fn test_dont_filter_default_above_minimum_versions() {
        for old_user_agent in &[
            IE10_UA,
            SAFARI_6_UA,
            OPERA_15_UA,
            ANDROID_4_UA,
            OPERA_MINI_8_UA,
        ] {
            let evt = testutils::get_event_with_user_agent(old_user_agent);
            let filter_result = should_filter(
                &evt,
                &get_legacy_browsers_config(true, &[LegacyBrowser::Default]),
            );
            assert_eq!(
                filter_result,
                Ok(()),
                "Default filter shouldn't have filtered User Agent\n{old_user_agent}"
            )
        }
    }

    #[test]
    fn test_filter_configured_browsers() {
        let test_configs = [
            (
                IE10_UA,
                &[LegacyBrowser::AndroidPre4, LegacyBrowser::Ie10][..],
            ),
            (IE11_UA, &[LegacyBrowser::Ie11][..]),
            (IE10_UA, &[LegacyBrowser::Ie10][..]),
            (IE9_UA, &[LegacyBrowser::Ie9][..]),
            (IE_MOBILE9_UA, &[LegacyBrowser::Ie9][..]),
            (
                IE9_UA,
                &[LegacyBrowser::AndroidPre4, LegacyBrowser::Ie9][..],
            ),
            (IE8_UA, &[LegacyBrowser::IePre9][..]),
            (
                IE8_UA,
                &[LegacyBrowser::OperaPre15, LegacyBrowser::IePre9][..],
            ),
            (OPERA_PRE15_UA, &[LegacyBrowser::OperaPre15][..]),
            (
                OPERA_PRE15_UA,
                &[LegacyBrowser::Ie10, LegacyBrowser::OperaPre15][..],
            ),
            (OPERA_MINI_PRE8_UA, &[LegacyBrowser::OperaMiniPre8][..]),
            (
                OPERA_MINI_PRE8_UA,
                &[LegacyBrowser::Ie10, LegacyBrowser::OperaMiniPre8][..],
            ),
            (ANDROID_PRE4_UA, &[LegacyBrowser::AndroidPre4][..]),
            (
                ANDROID_PRE4_UA,
                &[LegacyBrowser::Ie10, LegacyBrowser::AndroidPre4][..],
            ),
            (SAFARI_PRE6_UA, &[LegacyBrowser::SafariPre6][..]),
            (
                SAFARI_PRE6_UA,
                &[LegacyBrowser::OperaPre15, LegacyBrowser::SafariPre6][..],
            ),
            (EDGE_12_UA, &[LegacyBrowser::EdgePre79][..]),
            (
                EDGE_18_UA,
                &[LegacyBrowser::OperaPre15, LegacyBrowser::EdgePre79][..],
            ),
        ];

        for (ref user_agent, ref active_filters) in &test_configs {
            let evt = testutils::get_event_with_user_agent(user_agent);
            let filter_result =
                should_filter(&evt, &get_legacy_browsers_config(true, active_filters));
            assert_ne!(
                filter_result,
                Ok(()),
                "Filters {active_filters:?} should have filtered User Agent\n{user_agent} for "
            )
        }
    }

    #[test]
    fn test_dont_filter_unconfigured_browsers() {
        let test_configs = [
            (IE11_UA, LegacyBrowser::Ie10),
            (IE10_UA, LegacyBrowser::Ie9),
            (IE10_UA, LegacyBrowser::Ie11),
            (IE9_UA, LegacyBrowser::IePre9),
            (OPERA_15_UA, LegacyBrowser::OperaPre15),
            (OPERA_MINI_8_UA, LegacyBrowser::OperaMiniPre8),
            (ANDROID_4_UA, LegacyBrowser::AndroidPre4),
            (SAFARI_6_UA, LegacyBrowser::SafariPre6),
            (EDGE_12_UA, LegacyBrowser::Ie10),
            (EDGE_18_UA, LegacyBrowser::Ie10),
            (EDGE_79_UA, LegacyBrowser::EdgePre79),
            (EDGE_ANDROID_118_UA, LegacyBrowser::EdgePre79),
        ];

        for (user_agent, active_filter) in &test_configs {
            let evt = testutils::get_event_with_user_agent(user_agent);
            let filter_result = should_filter(
                &evt,
                &get_legacy_browsers_config(true, &[active_filter.clone()]),
            );
            assert_eq!(
                filter_result,
                Ok(()),
                "Filter {active_filter:?} shouldn't have filtered User Agent\n{user_agent} for "
            )
        }
    }

    /// Test to ensure Sentry filter compatibility.
    ///
    /// To be remove if/when Sentry backward compatibility is no longer required.
    mod sentry_compatibility {
        use super::*;

        // User agents used by Sentry tests
        const ANDROID2_S_UA: &str =
            "Mozilla/5.0 (Linux; U; Android 2.3.5; en-us; HTC Vision Build/GRI40) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile Safari/533.1";
        const ANDROID4_S_UA: &str =
            "Mozilla/5.0 (Linux; Android 4.0.4; Galaxy Nexus Build/IMM76B) AppleWebKit/535.19 (KHTML, like Gecko) Chrome/18.0.1025.133 Mobile Safari/535.19";
        const IE5_S_UA: &str =
            "Mozilla/4.0 (compatible; MSIE 5.50; Windows NT; SiteKiosk 4.9; SiteCoach 1.0)";
        const IE8_S_UA: &str =
            "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; Win64; x64; Trident/4.0; .NET CLR 2.0.50727; SLCC2; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; MDDC; Tablet PC 2.0)";
        const IE9_S_UA: &str = "Mozilla/5.0 (Windows; U; MSIE 9.0; WIndows NT 9.0; en-US))";
        const IE_MOBILE9_S_UA: &str =
            "Mozilla/5.0 (compatible; MSIE 9.0; Windows Phone OS 7.5; Trident/5.0; IEMobile/9.0; NOKIA; Lumia 710)";
        const IE10_S_UA: &str =
            "Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 7.0; InfoPath.3; .NET CLR 3.1.40767; Trident/6.0; en-IN)";
        const IE_MOBILE10_S_UA: &str =
            "Mozilla/5.0 (compatible; MSIE 10.0; Windows Phone 8.0; Trident/6.0; IEMobile/10.0; ARM; Touch; NOKIA; Lumia 520)";
        const OPERA11_S_UA: &str = "Opera/9.80 (Windows NT 5.1; U; it) Presto/2.7.62 Version/11.00";
        const OPERA_12_S_UA: &str =
            "Opera/9.80 (X11; Linux i686; Ubuntu/14.10) Presto/2.12.388 Version/12.16";
        const OPERA_15_S_UA: &str =
            "Mozilla/5.0 (X11; Linux x86_64; Debian) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/28.0.1500.52 Safari/537.36 OPR/15.0.1147.100";
        const CHROME_S_UA: &str =
            "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36";
        const EDGE_S_UA: &str =
            "Mozilla/5.0 (Windows NT 10.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36 Edge/12.10136";
        const SAFARI5_S_UA: &str =
            "Mozilla/5.0 (Windows; U; Windows NT 6.1; zh-HK) AppleWebKit/533.18.1 (KHTML, like Gecko) Version/5.0.2 Safari/533.18.5";
        const SAFARI7_S_UA: &str =
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.75.14 (KHTML, like Gecko) Version/7.0.3 Safari/7046A194A";
        const OPERA_MINI8_S_UA: &str =
            "Opera/9.80 (J2ME/MIDP; Opera Mini/8.0.35158/36.2534; U; en) Presto/2.12.423 Version/12.16";
        const OPERA_MINI7_S_UA: &str =
            "Opera/9.80 (J2ME/MIDP; Opera Mini/7.0.32796/59.323; U; fr) Presto/2.12.423 Version/12.16";

        #[test]
        fn test_filter_sentry_user_agents() {
            let test_configs = [
                (ANDROID2_S_UA, LegacyBrowser::Default),
                (IE9_S_UA, LegacyBrowser::Default),
                (IE_MOBILE9_S_UA, LegacyBrowser::Default),
                (IE5_S_UA, LegacyBrowser::Default),
                (OPERA11_S_UA, LegacyBrowser::Default),
                (OPERA_12_S_UA, LegacyBrowser::Default),
                (OPERA_MINI7_S_UA, LegacyBrowser::Default),
                (OPERA_12_S_UA, LegacyBrowser::OperaPre15),
                (OPERA_MINI7_S_UA, LegacyBrowser::OperaMiniPre8),
                (OPERA_MINI7_S_UA, LegacyBrowser::Default),
                (OPERA_MINI7_S_UA, LegacyBrowser::Default),
                (IE8_S_UA, LegacyBrowser::IePre9),
                (IE8_S_UA, LegacyBrowser::Default),
                (IE9_S_UA, LegacyBrowser::Ie9),
                (IE_MOBILE9_S_UA, LegacyBrowser::Ie9),
                (IE10_S_UA, LegacyBrowser::Ie10),
                (IE_MOBILE10_S_UA, LegacyBrowser::Ie10),
                (SAFARI5_S_UA, LegacyBrowser::SafariPre6),
                (SAFARI5_S_UA, LegacyBrowser::Default),
                (ANDROID2_S_UA, LegacyBrowser::AndroidPre4),
            ];

            for (ref user_agent, ref active_filter) in &test_configs {
                let evt = testutils::get_event_with_user_agent(user_agent);
                let filter_result = should_filter(
                    &evt,
                    &get_legacy_browsers_config(true, &[active_filter.clone()]),
                );
                assert_ne!(
                    filter_result,
                    Ok(()),
                    "Filter <{active_filter:?}> should have filtered User Agent\n{user_agent} for "
                )
            }
        }

        #[test]
        fn test_dont_filter_sentry_allowed_user_agents() {
            let test_configs = [
                (ANDROID4_S_UA, LegacyBrowser::Default),
                (IE10_S_UA, LegacyBrowser::Default),
                (IE_MOBILE10_S_UA, LegacyBrowser::Default),
                (CHROME_S_UA, LegacyBrowser::Default),
                (EDGE_S_UA, LegacyBrowser::Default),
                (OPERA_15_S_UA, LegacyBrowser::Default),
                (OPERA_MINI8_S_UA, LegacyBrowser::Default),
                (IE10_S_UA, LegacyBrowser::Default),
                (IE_MOBILE10_S_UA, LegacyBrowser::Default),
                (SAFARI7_S_UA, LegacyBrowser::Default),
            ];

            for (ref user_agent, ref active_filter) in &test_configs {
                let evt = testutils::get_event_with_user_agent(user_agent);
                let filter_result = should_filter(
                    &evt,
                    &get_legacy_browsers_config(true, &[active_filter.clone()]),
                );
                assert_eq!(
                    filter_result,
                    Ok(()),
                    "Filter {active_filter:?} shouldn't have filtered User Agent\n{user_agent} for "
                )
            }
        }
    }
}
