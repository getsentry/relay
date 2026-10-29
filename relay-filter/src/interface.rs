//! This module contains the trait for items that can be filtered by Inbound Filters, plus
//! the implementation for [`Event`].
use relay_conventions::attributes::{
    BROWSER__NAME, BROWSER__VERSION, CLIENT__ADDRESS, SENTRY__IS_LOCALHOST, SENTRY__RELEASE,
    SENTRY__SEGMENT__NAME, URL__FULL, USER_AGENT__ORIGINAL,
};
use url::Url;

use relay_event_schema::protocol::{
    Attributes, Csp, Event, EventType, Exception, LogEntry, OurLog, Replay, SessionAggregates,
    SessionUpdate, Span, SpanV2, TraceMetric, Values,
};
/// A user agent returned from [`Filterable::user_agent`].
#[derive(Clone, Debug, Default)]
pub struct UserAgent<'a> {
    /// The raw, unparsed user agent string, if available.
    pub raw: Option<&'a str>,
    /// A already fully parsed user agent, if available.
    pub parsed: Option<relay_ua::UserAgent<'a>>,
}

impl<'a> UserAgent<'a> {
    /// Parses the raw user agent, if it is not already parsed and returns it.
    pub fn parsed(&mut self) -> Option<&'_ relay_ua::UserAgent<'a>> {
        match &mut self.parsed {
            Some(parsed) => Some(parsed),
            parsed @ None => {
                *parsed = Some(relay_ua::parse_user_agent(self.raw?));
                parsed.as_ref()
            }
        }
    }
}

/// A data item to which filters can be applied.
pub trait Filterable {
    /// The CSP report contained in the item. Only for CSP reports.
    fn csp(&self) -> Option<&Csp> {
        None
    }

    /// The exception values of the item. Only for error events.
    fn exceptions(&self) -> Option<&Values<Exception>> {
        None
    }

    /// The IP address of the client that sent the data.
    fn ip_addr(&self) -> Option<&str> {
        None
    }

    /// The logentry message. Only for error events.
    fn logentry(&self) -> Option<&LogEntry> {
        None
    }

    /// The release string of the data item.
    fn release(&self) -> Option<&str> {
        None
    }

    /// The transaction name. Only for transaction events.
    fn transaction(&self) -> Option<&str> {
        None
    }

    /// The URL from which the request originates. Used for localhost filtering.
    fn url(&self) -> Option<Url> {
        None
    }

    /// The user agent of the client that sent the data.
    fn user_agent(&self) -> UserAgent<'_> {
        Default::default()
    }

    /// Retrieves a request headers from the item.
    ///
    /// For example this is used for localhost filtering by inspecting relevant headers that may
    /// be included in the `request.headers` of an error.
    ///
    /// This **does not** return header information from the request that reached relay.
    fn header(&self, _header_name: &str) -> Option<&str> {
        None
    }
}

impl Filterable for Event {
    fn csp(&self) -> Option<&Csp> {
        if self.ty.value() != Some(&EventType::Csp) {
            return None;
        }
        self.csp.value()
    }

    fn exceptions(&self) -> Option<&Values<Exception>> {
        self.exceptions.value()
    }

    fn ip_addr(&self) -> Option<&str> {
        let user = self.user.value()?;
        Some(user.ip_address.value()?.as_ref())
    }

    fn logentry(&self) -> Option<&LogEntry> {
        self.logentry.value()
    }

    fn release(&self) -> Option<&str> {
        self.release.as_str()
    }

    fn transaction(&self) -> Option<&str> {
        if self.ty.value() != Some(&EventType::Transaction) {
            return None;
        }
        self.transaction.as_str()
    }

    fn url(&self) -> Option<Url> {
        let url_str = self.request.value()?.url.value()?;
        Url::parse(url_str).ok()
    }

    fn user_agent(&self) -> UserAgent<'_> {
        UserAgent {
            raw: self.user_agent(),
            parsed: None,
        }
    }

    fn header(&self, header_name: &str) -> Option<&str> {
        self.request
            .value()?
            .headers
            .value()?
            .get_header(header_name)
    }
}

impl Filterable for Replay {
    fn ip_addr(&self) -> Option<&str> {
        let user = self.user.value()?;
        Some(user.ip_address.value()?.as_ref())
    }

    fn release(&self) -> Option<&str> {
        self.release.as_str()
    }

    fn url(&self) -> Option<Url> {
        let url_str = self.request.value()?.url.value()?;
        Url::parse(url_str).ok()
    }

    fn user_agent(&self) -> UserAgent<'_> {
        UserAgent {
            raw: self.user_agent(),
            parsed: None,
        }
    }

    fn header(&self, header_name: &str) -> Option<&str> {
        self.request
            .value()?
            .headers
            .value()?
            .get_header(header_name)
    }
}

impl Filterable for Span {
    fn ip_addr(&self) -> Option<&str> {
        self.data
            .value()?
            .get_value_with_deprecations(CLIENT__ADDRESS)?
            .as_str()
    }

    fn release(&self) -> Option<&str> {
        self.data
            .value()?
            .get_value_with_deprecations(SENTRY__RELEASE)?
            .as_str()
    }

    fn transaction(&self) -> Option<&str> {
        self.data
            .value()?
            .get_value_with_deprecations(SENTRY__SEGMENT__NAME)?
            .as_str()
    }

    fn url(&self) -> Option<Url> {
        let url_str = self
            .data
            .value()?
            .get_value_with_deprecations(URL__FULL)?
            .as_str()?;
        Url::parse(url_str).ok()
    }

    fn user_agent(&self) -> UserAgent<'_> {
        let raw = self
            .data
            .value()
            .and_then(|data| data.get_value_with_deprecations(USER_AGENT__ORIGINAL))
            .and_then(|value| value.as_str());
        UserAgent { raw, parsed: None }
    }
}

impl Filterable for SessionUpdate {
    fn ip_addr(&self) -> Option<&str> {
        self.attributes
            .ip_address
            .as_ref()
            .map(|addr| addr.as_str())
    }

    fn release(&self) -> Option<&str> {
        Some(&self.attributes.release)
    }

    fn user_agent(&self) -> UserAgent<'_> {
        UserAgent {
            raw: self.attributes.user_agent.as_deref(),
            parsed: None,
        }
    }
}

impl Filterable for SessionAggregates {
    fn ip_addr(&self) -> Option<&str> {
        self.attributes
            .ip_address
            .as_ref()
            .map(|addr| addr.as_str())
    }

    fn release(&self) -> Option<&str> {
        Some(&self.attributes.release)
    }

    fn user_agent(&self) -> UserAgent<'_> {
        UserAgent {
            raw: self.attributes.user_agent.as_deref(),
            parsed: None,
        }
    }
}

macro_rules! impl_for_attributes {
    ($ty:ty) => {
        impl Filterable for $ty {
            fn ip_addr(&self) -> Option<&str> {
                let attributes = self.attributes.value()?;

                // If the SDK indicates this really is a localhost request, use this information
                // to provide the IP to filter on.
                let is_localhost = attributes
                    .get_value_with_deprecations(SENTRY__IS_LOCALHOST)
                    .and_then(|v| v.as_bool());
                if let Some(true) = is_localhost {
                    return Some("127.0.0.1");
                }

                // Otherwise fall back to the more generic client address.
                //
                // This may be inferred from Relay (even for requests happening on localhost),
                // and therefore is just the fallback.
                //
                // This may happen for example for single page applications running in the browser,
                // they really are on `localhost` but Relay infers the internet ip address.
                self.attributes
                    .value()?
                    .get_value_with_deprecations(CLIENT__ADDRESS)?
                    .as_str()
            }

            fn release(&self) -> Option<&str> {
                self.attributes
                    .value()?
                    .get_value_with_deprecations(SENTRY__RELEASE)?
                    .as_str()
            }

            fn transaction(&self) -> Option<&str> {
                self.attributes
                    .value()?
                    .get_value_with_deprecations(SENTRY__SEGMENT__NAME)?
                    .as_str()
            }

            fn url(&self) -> Option<Url> {
                let url = self
                    .attributes
                    .value()?
                    .get_value_with_deprecations(URL__FULL)?
                    .as_str()?;
                Url::parse(url).ok()
            }

            fn user_agent(&self) -> UserAgent<'_> {
                user_agent_from_attributes(&self.attributes)
            }
        }
    };
}

impl_for_attributes!(SpanV2);
impl_for_attributes!(OurLog);
impl_for_attributes!(TraceMetric);

fn user_agent_from_attributes(attributes: &relay_protocol::Annotated<Attributes>) -> UserAgent<'_> {
    let parsed = (|| {
        let attributes = attributes.value()?;

        let family = attributes
            .get_value_with_deprecations(BROWSER__NAME)?
            .as_str()?;
        let version = attributes
            .get_value_with_deprecations(BROWSER__VERSION)?
            .as_str()?;
        let mut parts = version.splitn(3, '.');

        Some(relay_ua::UserAgent {
            family: family.into(),
            major: parts.next().map(Into::into),
            minor: parts.next().map(Into::into),
            patch: parts.next().map(Into::into),
        })
    })();

    let raw = attributes
        .value()
        .and_then(|attr| attr.get_value_with_deprecations(USER_AGENT__ORIGINAL))
        .and_then(|ua| ua.as_str());

    UserAgent { raw, parsed }
}

#[cfg(test)]
mod tests {
    use super::*;
    use relay_event_schema::protocol::SpanData;
    use relay_protocol::{Annotated, Value};

    fn items(pairs: &[(&str, &str)]) -> [Box<dyn Filterable>; 4] {
        let mut attributes = Attributes::new();
        let mut data = SpanData::default();
        for &(key, value) in pairs {
            attributes.insert(key, value);
            data.insert_value(key, value.to_owned());
        }
        [
            Box::new(Span {
                data: Annotated::new(data),
                ..Default::default()
            }),
            Box::new(SpanV2 {
                attributes: Annotated::new(attributes.clone()),
                ..Default::default()
            }),
            Box::new(OurLog {
                attributes: Annotated::new(attributes.clone()),
                ..Default::default()
            }),
            Box::new(TraceMetric {
                attributes: Annotated::new(attributes),
                ..Default::default()
            }),
        ]
    }

    #[test]
    fn inbound_filters_read_deprecated_attributes() {
        for item in items(&[
            ("http.client_ip", "127.0.0.1"),
            ("release", "1.0"),
            ("transaction", "/users"),
            ("http.url", "http://localhost/users"),
            ("http.user_agent", "Googlebot"),
        ]) {
            assert_eq!(item.ip_addr(), Some("127.0.0.1"));
            assert_eq!(item.release(), Some("1.0"));
            assert_eq!(item.transaction(), Some("/users"));
            assert_eq!(
                item.url(),
                Some(Url::parse("http://localhost/users").unwrap())
            );
            assert_eq!(item.user_agent().raw, Some("Googlebot"));
        }
    }

    #[test]
    fn inbound_filters_prefer_current_attributes() {
        for item in items(&[
            ("http.client_ip", "127.0.0.1"),
            ("client.address", "192.0.2.1"),
            ("release", "old"),
            ("sentry.release", "current"),
            ("transaction", "/old"),
            ("sentry.segment.name", "/current"),
            ("http.url", "http://localhost/old"),
            ("url.full", "https://example.com/current"),
            ("http.user_agent", "old"),
            ("user_agent.original", "current"),
        ]) {
            assert_eq!(item.ip_addr(), Some("192.0.2.1"));
            assert_eq!(item.release(), Some("current"));
            assert_eq!(item.transaction(), Some("/current"));
            assert_eq!(
                item.url(),
                Some(Url::parse("https://example.com/current").unwrap())
            );
            assert_eq!(item.user_agent().raw, Some("current"));
        }
    }

    #[test]
    fn inbound_filters_handle_missing_attributes() {
        for item in items(&[]) {
            assert_eq!(item.ip_addr(), None);
            assert_eq!(item.release(), None);
            assert_eq!(item.transaction(), None);
            assert_eq!(item.url(), None);
            assert!(item.user_agent().raw.is_none());
            assert!(item.user_agent().parsed.is_none());
        }
    }

    #[test]
    fn parsed_browser_reads_deprecated_attributes() {
        for item in items(&[
            ("sentry.browser.name", "Internet Explorer"),
            ("sentry.browser.version", "8.0.1"),
        ])
        .into_iter()
        .skip(1)
        {
            let ua = item.user_agent().parsed.unwrap();
            assert_eq!(ua.family, "Internet Explorer");
            assert_eq!(ua.major.as_deref(), Some("8"));
            assert_eq!(ua.minor.as_deref(), Some("0"));
            assert_eq!(ua.patch.as_deref(), Some("1"));
        }
    }

    #[test]
    fn parsed_browser_prefers_current_attributes() {
        for item in items(&[
            ("sentry.browser.name", "Internet Explorer"),
            ("sentry.browser.version", "8.0"),
            ("browser.name", "Chrome"),
            ("browser.version", "120.1"),
        ])
        .into_iter()
        .skip(1)
        {
            let ua = item.user_agent().parsed.unwrap();
            assert_eq!(ua.family, "Chrome");
            assert_eq!(ua.major.as_deref(), Some("120"));
            assert_eq!(ua.minor.as_deref(), Some("1"));
        }
    }

    #[test]
    fn localhost_flag_takes_precedence_over_deprecated_address() {
        let mut attributes = Attributes::new();
        attributes.insert("sentry.is_localhost", true);
        attributes.insert("http.client_ip", "192.0.2.1");
        let span = SpanV2 {
            attributes: Annotated::new(attributes),
            ..Default::default()
        };
        assert_eq!(span.ip_addr(), Some("127.0.0.1"));
    }

    #[test]
    fn inbound_filters_do_not_skip_present_invalid_current_values() {
        let span =
            Annotated::<Span>::from_json(r#"{"data":{"sentry.release":42,"release":"old"}}"#)
                .unwrap()
                .into_value()
                .unwrap();
        assert_eq!(span.release(), None);

        let mut attributes = Attributes::new();
        attributes.insert("sentry.release", 42i64);
        attributes.insert("release", "old");
        let span = SpanV2 {
            attributes: Annotated::new(attributes),
            ..Default::default()
        };
        assert_eq!(span.release(), None);
        assert_eq!(
            span.attributes.value().unwrap().get_value("sentry.release"),
            Some(&Value::I64(42))
        );
    }
}
