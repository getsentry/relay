//! Filters events coming from user agents known to be web crawlers.

use lazy_static::lazy_static;
use regex::Regex;

use relay_general::protocol::Event;
use relay_general::user_agent;

use crate::{FilterConfig, FilterStatKey};

/// Filters events originating from a known web crawler.
pub fn should_filter(event: &Event, config: &FilterConfig) -> Result<(), FilterStatKey> {
    if !config.is_enabled {
        return Ok(());
    }

    if let Some(user_agent) = user_agent::get_user_agent(event) {
        if WEB_CRAWLERS.is_match(user_agent) {
            return Err(FilterStatKey::WebCrawlers);
        }
    }

    Ok(())
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
        lyticsbot|                  # Lytics
        AWS\sSecurity\sScanner      # AWS Security Scanner causing DisallowedHost errors in Django, see
                                    # https://forums.aws.amazon.com/thread.jspa?messageID=932404
                                    # and https://github.com/getsentry/sentry-python/issues/641
    "#
    )
    .expect("Invalid web crawlers filter Regex");
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::testutils;

    #[test]
    fn test_filter_when_disabled() {
        let evt = testutils::get_event_with_user_agent("Googlebot");
        let filter_result = should_filter(&evt, &FilterConfig { is_enabled: false });
        assert_eq!(
            filter_result,
            Ok(()),
            "Event filtered although filter should have been disabled"
        )
    }

    #[test]
    fn test_filter_banned_user_agents() {
        let user_agents = [
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
            "AWS Security Scanner",
        ];

        for banned_user_agent in &user_agents {
            let event = testutils::get_event_with_user_agent(banned_user_agent);
            let filter_result = should_filter(&event, &FilterConfig { is_enabled: true });
            assert_ne!(
                filter_result,
                Ok(()),
                "Failed to filter web crawler with user agent '{}'",
                banned_user_agent
            );
        }
    }

    #[test]
    fn test_dont_filter_normal_user_agents() {
        for user_agent in &["some user agent", "IE", "ie", "opera", "safari"] {
            let event = testutils::get_event_with_user_agent(user_agent);
            let filter_result = should_filter(&event, &FilterConfig { is_enabled: true });
            assert_eq!(
                filter_result,
                Ok(()),
                "Failed benign user agent '{}'",
                user_agent
            );
        }
    }
}
