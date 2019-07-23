use crate::actors::project::FilterConfig;
use regex::{Regex, RegexBuilder};
use semaphore_general::protocol::{Event, Headers, Request};

use lazy_static::lazy_static;
use semaphore_general::types::Annotated;

pub fn web_crawlers_filter(event: &Event, config: &FilterConfig) -> Result<(), String> {
    if !config.is_enabled {
        return Ok(());
    }

    if let Some(user_agent) = get_user_agent(event) {
        if WEB_CRAWLERS.is_match(user_agent) {
            return Err("User agent is web crawler".to_string());
        }
    }
    return Ok(());
}

fn get_user_agent(event: &Event) -> Option<&str> {
    fn get_user_agent_from_headers(headers: &Headers) -> Option<&str> {
        for item in headers.iter() {
            if let Some((ref o_k, ref v)) = item.value() {
                if let Some(ref k) = o_k.as_str() {
                    if k.to_lowercase() == "user-agent" {
                        return v.as_str();
                    }
                }
            }
        }
        return None;
    }

    return event
        .request
        .value()
        .and_then(|req: &Request| req.headers.value())
        .and_then(|headers: &Headers| get_user_agent_from_headers(headers));
}

lazy_static! {
    static ref WEB_CRAWLERS_STR: String = [
                // Google spiders (Adsense and others)
            // https://support.google.com/webmasters/answer/1061943?hl=en
            r"Mediapartners-Google",
            r"AdsBot-Google",
            r"Googlebot",
            r"FeedFetcher-Google",
            // Bing search
            r"BingBot",
            r"BingPreview",
            // Baidu search
            r"Baiduspider",
            // Yahoo
            r"Slurp",
            // Sogou
            r"Sogou",
            // facebook
            r"facebook",
            // Alexa
            r"ia_archiver",
            // Generic bot
            r"bots?[/\s\);]",
            // Generic spider
            r"spider[/\s\);]",
            // Slack - see https://api.slack.com/robots
            r"Slack",
            // Google indexing bot
            r"Calypso AppCrawler",
            // Pingdom
            r"pingdom",
            // Lytics
            r"lyticsbot"
        ].join("|");

    static ref WEB_CRAWLERS: Regex =
    RegexBuilder::new(WEB_CRAWLERS_STR.as_str())
        .case_insensitive(true)
        .build()
        .expect("Invalid web crawlers filter Regex");
}

//#[cfg(test)]
mod tests {
    use super::*;
    use semaphore_general::protocol::PairList;

    fn get_f_config(is_enabled: bool) -> FilterConfig {
        FilterConfig { is_enabled }
    }

    fn get_event_with_user_agent(user_agent: &str) -> Event {
        let mut headers = Vec::new();

        headers.push(Annotated::new((
            Annotated::new("Accept".to_string().into()),
            Annotated::new("application/json".to_string().into()),
        )));

        headers.push(Annotated::new((
            Annotated::new("UsEr-AgeNT".to_string().into()),
            Annotated::new(user_agent.to_string().into()),
        )));
        headers.push(Annotated::new((
            Annotated::new("WWW-Authenticate".to_string().into()),
            Annotated::new("basic".to_string().into()),
        )));

        return Event {
            request: Annotated::new(Request {
                headers: Annotated::new(Headers(PairList(headers))),
                ..Default::default()
            }),
            ..Default::default()
        };
    }

    #[test]
    fn it_should_not_filter_events_when_disabled() {
        let evt = get_event_with_user_agent("Googlebot");
        let filter_result = web_crawlers_filter(&evt, &get_f_config(false));
        assert_eq!(
            filter_result,
            Ok(()),
            "Event filtered although filter should have been disabled"
        )
    }

    #[test]
    fn it_should_filter_events_from_banned_user_agents() {
        for banned_user_agent in [
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
        ]
        .iter()
        {
            let event = get_event_with_user_agent(banned_user_agent);
            let filter_result = web_crawlers_filter(&event, &get_f_config(true));
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
        for user_agent in ["some user agent", "IE", "ie", "opera", "safari"].iter() {
            let event = get_event_with_user_agent(user_agent);
            let filter_result = web_crawlers_filter(&event, &get_f_config(true));
            assert_eq!(
                filter_result,
                Ok(()),
                "Failed benign user agent '{}'",
                user_agent
            );
        }
    }
}
