//! Filters events coming from user agents known to be web crawlers.

use lazy_static::lazy_static;
use regex::Regex;

use relay_general::protocol::Event;
use relay_general::user_agent;

use crate::{FilterConfig, FilterStatKey};

/// Checks if the event originates from a known web crawler.
pub fn matches(event: &Event) -> bool {
    if let Some(user_agent) = user_agent::get_user_agent(event) {
        WEB_CRAWLERS.is_match(user_agent)
    } else {
        false
    }
}

/// Filters events originating from a known web crawler.
pub fn should_filter(event: &Event, config: &FilterConfig) -> Result<(), FilterStatKey> {
    if !config.is_enabled {
        return Ok(());
    }

    if matches(event) {
        return Err(FilterStatKey::WebCrawlers);
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
        AWS\sSecurity\sScanner|     # AWS Security Scanner causing DisallowedHost errors in Django, see
                                    # https://forums.aws.amazon.com/thread.jspa?messageID=932404
                                    # and https://github.com/getsentry/sentry-python/issues/641
        HubSpot\sCrawler            # HubSpot web crawler (web-crawlers@hubspot.com)
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
            "Mozilla/5.0 (Linux; Android 6.0.1; Calypso AppCrawler Build/MMB30Y; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/53.0.2785.124 Mobile Safari/537.36",
            "Slackbot-LinkExpanding 1.0 (+https://api.slack.com/robots)",
            "Slack-ImgProxy 0.19 (+https://api.slack.com/robots)",
            "Slackbot 1.0(+https://api.slack.com/robots)",
            "Twitterbot/1.0",
            "FeedFetcher-Google; (+http://www.google.com/feedfetcher.html)",
            "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
            "AdsBot-Google (+http://www.google.com/adsbot.html)",
            "Mozilla/5.0 (compatible; HubSpot Crawler; web-crawlers@hubspot.com)",
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
        let normal_user_agents = [
            "some user agent",
            "IE",
            "ie",
            "opera",
            "safari",
            "APIs-Google (+https://developers.google.com/webmasters/APIs-Google.html)",
            "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36",
        ];
        for user_agent in &normal_user_agents {
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
