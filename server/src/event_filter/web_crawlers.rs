//! Filters events coming from user agents known to be web crawlers.

use crate::actors::project::FilterConfig;
use regex::Regex;
use semaphore_general::protocol::Event;

use lazy_static::lazy_static;

use crate::event_filter::util;

/// Filters events originating from a known web crawler
pub fn should_filter(event: &Event, config: &FilterConfig) -> Result<(), String> {
    if !config.is_enabled {
        return Ok(());
    }

    if let Some(user_agent) = util::get_user_agent(event) {
        if WEB_CRAWLERS.is_match(user_agent) {
            return Err("User agent is web crawler".to_string());
        }
    }
    return Ok(());
}

lazy_static! {
    static ref WEB_CRAWLERS: Regex = Regex::new(
        r#"(?ix)
        Mediapartners-Google|
        AdsBot-Google|
        Googlebot|
        FeedFetcher-Google|
        BingBot|                    # Bing search
        BingPreview|
        Baiduspider|                # Baidu search
        Slurp|                      # Yahoo
        Sogou|                      # Sogou
        facebook|                   # facebook
        ia_archiver|                # Alexa
        bots?[/\s\);]|              # Generic bot
        spider[/\s\);]|             # Generic spider
        Slack|                      # Slack - see https://api.slack.com/robots
        Calypso\sAppCrawler|        # Google indexing bot
        pingdom|                    # Pingdom
        lyticsbot                   # Lytics
    "#
    )
    .expect("Invalid web crawlers filter Regex");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event_filter::util::test_utils::*;

    #[test]
    fn it_should_not_filter_events_when_disabled() {
        let evt = get_event_with_user_agent("Googlebot");
        let filter_result = should_filter(&evt, &get_f_config(false));
        assert_eq!(
            filter_result,
            Ok(()),
            "Event filtered although filter should have been disabled"
        )
    }

    #[test]
    fn it_should_filter_events_from_banned_user_agents() {
        for banned_user_agent in &[
            "Mediapartners-Google",
            "AdsBot-Google",
            "Googlebot",
            "FeedFetcher-Google",
            "BingBot",
            "BingPreview",
            "Baiduspider",
            "Slurp",
            "Sogou",
            "facebook",
            "ia_archiver",
            "bots ",
            "bots;",
            "bots)",
            "spider ",
            "spider;",
            "spider)",
            "Slack",
            "Calypso AppCrawler",
            "pingdom",
            "lyticsbot",
        ] {
            let event = get_event_with_user_agent(banned_user_agent);
            let filter_result = should_filter(&event, &get_f_config(true));
            assert_ne!(
                filter_result,
                Ok(()),
                "Failed to filter web crawler with user agent '{}'",
                banned_user_agent
            );
        }
    }

    #[test]
    fn it_should_not_filter_events_from_normal_user_agents() {
        for user_agent in &["some user agent", "IE", "ie", "opera", "safari"] {
            let event = get_event_with_user_agent(user_agent);
            let filter_result = should_filter(&event, &get_f_config(true));
            assert_eq!(
                filter_result,
                Ok(()),
                "Failed benign user agent '{}'",
                user_agent
            );
        }
    }
}
