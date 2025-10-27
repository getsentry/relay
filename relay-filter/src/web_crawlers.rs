//! Filters events coming from user agents known to be web crawlers.

use std::sync::LazyLock;

use regex::Regex;

use crate::{FilterConfig, FilterStatKey, Filterable};

static WEB_CRAWLERS: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?ix)
        Mediapartners-Google|
        AdsBot-Google|
        Googlebot|
        FeedFetcher-Google|
        Storebot-Google|
        BingBot|                    # Bing search
        BingPreview|
        Baiduspider|                # Baidu search
        Slurp|                      # Yahoo
        Sogou|                      # Sogou
        facebook|                   # facebook
        meta-|                      # meta/facebook
        ia_archiver|                # Alexa
        bots?([/\s\);]|$)|          # Generic bot
        spider([/\s\);]|$)|         # Generic spider
        Slack|                      # Slack - see https://api.slack.com/robots
        Calypso\sAppCrawler|        # Google indexing bot
        pingdom|                    # Pingdom
        lyticsbot|                  # Lytics
        AWS\sSecurity\sScanner|     # AWS Security Scanner causing DisallowedHost errors in Django, see
                                    # https://forums.aws.amazon.com/thread.jspa?messageID=932404
                                    # and https://github.com/getsentry/sentry-python/issues/641
        HubSpot\sCrawler|           # HubSpot web crawler (web-crawlers@hubspot.com)
        Bytespider|                 # Bytedance
        Better\sUptime|             # Better Uptime
        Cloudflare-Healthchecks|    # Cloudflare Health Checks
        GTmetrix|                   # GTmetrix
        BrightEdgeOnCrawl|          # BrightEdge - see https://www.brightedge.com/news/press-releases/brightedge-acquires-oncrawl-future-proof-web-30-strategies
        ELB-HealthChecker|          # AWS Elastic Load Balancing Health Checks
        naver.me/spd|               # Yeti/1.1 - naver.me
        ClaudeBot|                  # Anthropic - see https://support.anthropic.com/en/articles/8896518-does-anthropic-crawl-data-from-the-web-and-how-can-site-owners-block-the-crawler
        CCBot|                      # CCBot - see https://commoncrawl.org/ccbot
        OAI-SearchBot|              # OpenAI - see https://platform.openai.com/docs/bots
        GPTBot|                     # OpenAI - see https://platform.openai.com/docs/bots
        PerplexityBot|              # Perplexity - see https://docs.perplexity.ai/guides/bots
        Applebot|                   # Apple - see https://support.apple.com/en-us/119829
        DuckDuckBot                 # DuckDuckGo - see https://duckduckgo.com/duckduckgo-help-pages/results/duckduckbot
    "
    )
    .expect("Invalid web crawlers filter Regex")
});

static ALLOWED_WEB_CRAWLERS: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?ix)
        Slackbot\s1\.\d+|            # Slack - see https://api.slack.com/robots
        SentryUptimeBot|             # Uptime Checker https://docs.sentry.io/product/alerts/uptime-monitoring/
        ChatGPT-User                 # ChatGPT user prompted requests
    ",
    )
    .expect("Invalid allowed web crawlers filter Regex")
});

/// Checks if the event originates from a known web crawler.
fn matches(user_agent: &str) -> bool {
    WEB_CRAWLERS.is_match(user_agent) && !ALLOWED_WEB_CRAWLERS.is_match(user_agent)
}

/// Filters events originating from a known web crawler.
pub fn should_filter<F: Filterable>(item: &F, config: &FilterConfig) -> Result<(), FilterStatKey> {
    if !config.is_enabled {
        return Ok(());
    }

    let user_agent = item.user_agent();
    let family = user_agent.parsed.as_ref().map(|ua| ua.family.as_ref());

    // Use the raw user agent if it is available, as it is higher quality. For example some user
    // agents may be parsed as `Other` while the raw user agent would be filtered.
    //
    // Fallback to the parsed user agent as under circumstances only that may be available.
    if user_agent.raw.or(family).is_some_and(matches) {
        return Err(FilterStatKey::WebCrawlers);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{UserAgent, testutils};

    #[derive(Debug)]
    struct TestFilterable<'a>(UserAgent<'a>);

    impl<'a> Filterable for TestFilterable<'a> {
        fn user_agent(&self) -> UserAgent<'_> {
            self.0.clone()
        }
    }

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
            "Storebot-Google",
            "Mozilla/5.0 (X11; Linux x86_64; Storebot-Google/1.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            "BingBot",
            "BingPreview",
            "Baiduspider",
            "Slurp",
            "Sogou",
            "facebook",
            "facebookexternalhit/1.1 (+http://www.facebook.com/externalhit_uatext.php)",
            "facebookcatalog/1.0",
            "meta-externalagent/1.1 (+https://developers.facebook.com/docs/sharing/webmasters/crawler)",
            "meta-externalfetcher/1.1",
            "ia_archiver",
            "bots ",
            "bots;",
            "bots)",
            "spider ",
            "spider;",
            "spider)",
            "Calypso AppCrawler",
            "pingdom",
            "lyticsbot",
            "AWS Security Scanner",
            "Mozilla/5.0 (Linux; Android 6.0.1; Calypso AppCrawler Build/MMB30Y; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/53.0.2785.124 Mobile Safari/537.36",
            "Slackbot-LinkExpanding 1.0 (+https://api.slack.com/robots)",
            "Slack-ImgProxy 0.19 (+https://api.slack.com/robots)",
            "Twitterbot/1.0",
            "FeedFetcher-Google; (+http://www.google.com/feedfetcher.html)",
            "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
            "AdsBot-Google (+http://www.google.com/adsbot.html)",
            "Mozilla/5.0 (compatible; HubSpot Crawler; web-crawlers@hubspot.com)",
            "Mozilla/5.0 (Linux; Android 5.0) AppleWebKit/537.36 (KHTML, like Gecko) Mobile Safari/537.36 (compatible; Bytespider; spider-feedback@bytedance.com)",
            "Better Uptime Bot Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36",
            "Mozilla/5.0 (compatible;Cloudflare-Healthchecks/1.0;+https://www.cloudflare.com/; healthcheck-id: 0d1ca23e292c8c14)",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36 GTmetrix",
            "Mozilla/5.0 (compatible; BrightEdgeOnCrawl/1.0; +http://www.oncrawl.com)",
            "ELB-HealthChecker/2.0",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko; compatible; Yeti/1.1; +https://naver.me/spd) Chrome/127.0.0.0 Safari/537.36",
            "Mozilla/5.0; ClaudeBot",
            "Mozilla/5.0; CCBot",
            "; OAI-SearchBot/1.0; +https://openai.com/searchbot",
            "Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko); compatible; GPTBot/1.1; +https://openai.com/gptbot",
            "Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko; compatible; PerplexityBot/1.0; +https://perplexity.ai/perplexitybot)",
            "Mozilla/5.0 (Device; OS_version) AppleWebKit/WebKit_version (KHTML, like Gecko)Version/Safari_version [Mobile/Mobile_version] Safari/WebKit_version (Applebot/Applebot_version; +http://www.apple.com/go/applebot)",
            "DuckDuckBot/1.1; (+http://duckduckgo.com/duckduckbot.html)",
        ];

        for banned_user_agent in &user_agents {
            let event = testutils::get_event_with_user_agent(banned_user_agent);
            let filter_result = should_filter(&event, &FilterConfig { is_enabled: true });
            assert_ne!(
                filter_result,
                Ok(()),
                "Failed to filter web crawler with user agent '{banned_user_agent}'"
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
            "Slackbot 1.0(+https://api.slack.com/robots)",
            "SentryUptimeBot/1.0 (+http://docs.sentry.io/product/alerts/uptime-monitoring/)",
            "Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko); compatible; ChatGPT-User/1.0; +https://openai.com/bot",
        ];
        for user_agent in &normal_user_agents {
            let event = testutils::get_event_with_user_agent(user_agent);
            let filter_result = should_filter(&event, &FilterConfig { is_enabled: true });
            assert_eq!(
                filter_result,
                Ok(()),
                "Failed benign user agent '{user_agent}'"
            );
        }
    }

    #[test]
    fn test_filter_parsed_ua_only() {
        let ua = UserAgent {
            raw: None,
            parsed: Some(relay_ua::UserAgent {
                family: "Twitterbot".into(),
                ..Default::default()
            }),
        };

        let filter_result = should_filter(&TestFilterable(ua), &FilterConfig { is_enabled: true });
        assert_ne!(filter_result, Ok(()));
    }

    #[test]
    fn test_filter_parsed_ua_does_not_filter_default() {
        let ua = UserAgent {
            raw: None,
            // This may happen if the raw user agent cannot be parsed or is not available,
            // in this case the filter should not accidentally remove the event.
            parsed: Some(Default::default()),
        };

        let filter_result = should_filter(&TestFilterable(ua), &FilterConfig { is_enabled: true });
        assert_eq!(filter_result, Ok(()));
    }
}
