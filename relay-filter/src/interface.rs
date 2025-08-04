//! This module contains the trait for items that can be filtered by Inbound Filters, plus
//! the implementation for [`Event`].
use url::Url;

use relay_event_schema::protocol::{
    Csp, Event, EventType, Exception, LogEntry, OurLog, Replay, SessionAggregates, SessionUpdate,
    Span, Values,
};

/// A user agent returned from [`Filterable::user_agent`].
pub enum UserAgent<'a> {
    /// The raw, unparsed user agent string.
    Raw(&'a str),
    /// A already parsed user agent.
    Parsed(relay_ua::UserAgent<'a>),
}

/// A data item to which filters can be applied.
pub trait Filterable {
    /// The CSP report contained in the item. Only for CSP reports.
    fn csp(&self) -> Option<&Csp>;

    /// The exception values of the item. Only for error events.
    fn exceptions(&self) -> Option<&Values<Exception>>;

    /// The IP address of the client that sent the data.
    fn ip_addr(&self) -> Option<&str>;

    /// The logentry message. Only for error events.
    fn logentry(&self) -> Option<&LogEntry>;

    /// The release string of the data item.
    fn release(&self) -> Option<&str>;

    /// The transaction name. Only for transaction events.
    fn transaction(&self) -> Option<&str>;

    /// The URL from which the request originates. Used for localhost filtering.
    fn url(&self) -> Option<Url>;

    /// The user agent of the client that sent the data.
    fn user_agent(&self) -> Option<UserAgent<'_>>;

    /// Retrieves a request headers from the item.
    ///
    /// For example this is used for localhost filtering by inspecting relevant headers that may
    /// be included in the `request.headers` of an error.
    ///
    /// This **does not** return header information from the request that reached relay.
    fn header(&self, header_name: &str) -> Option<&str>;
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

    fn user_agent(&self) -> Option<UserAgent<'_>> {
        self.user_agent().map(UserAgent::Raw)
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
    fn csp(&self) -> Option<&Csp> {
        None
    }

    fn exceptions(&self) -> Option<&Values<Exception>> {
        None
    }

    fn ip_addr(&self) -> Option<&str> {
        let user = self.user.value()?;
        Some(user.ip_address.value()?.as_ref())
    }

    fn logentry(&self) -> Option<&LogEntry> {
        None
    }

    fn release(&self) -> Option<&str> {
        self.release.as_str()
    }

    fn transaction(&self) -> Option<&str> {
        None
    }

    fn url(&self) -> Option<Url> {
        let url_str = self.request.value()?.url.value()?;
        Url::parse(url_str).ok()
    }

    fn user_agent(&self) -> Option<UserAgent<'_>> {
        self.user_agent().map(UserAgent::Raw)
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
    fn csp(&self) -> Option<&Csp> {
        // Only for events.
        None
    }

    fn exceptions(&self) -> Option<&Values<Exception>> {
        // Only for events.
        None
    }

    fn ip_addr(&self) -> Option<&str> {
        self.data.value()?.client_address.as_str()
    }

    fn logentry(&self) -> Option<&LogEntry> {
        // Only for events.
        None
    }

    fn release(&self) -> Option<&str> {
        self.data.value()?.release.as_str()
    }

    fn transaction(&self) -> Option<&str> {
        self.data.value()?.segment_name.as_str()
    }

    fn url(&self) -> Option<Url> {
        let url_str = self.data.value()?.url_full.as_str()?;
        Url::parse(url_str).ok()
    }

    fn user_agent(&self) -> Option<UserAgent<'_>> {
        self.data
            .value()?
            .user_agent_original
            .as_str()
            .map(UserAgent::Raw)
    }

    fn header(&self, _: &str) -> Option<&str> {
        None
    }
}

impl Filterable for SessionUpdate {
    fn csp(&self) -> Option<&Csp> {
        None
    }

    fn exceptions(&self) -> Option<&Values<Exception>> {
        None
    }

    fn ip_addr(&self) -> Option<&str> {
        self.attributes
            .ip_address
            .as_ref()
            .map(|addr| addr.as_str())
    }

    fn logentry(&self) -> Option<&LogEntry> {
        None
    }

    fn release(&self) -> Option<&str> {
        Some(&self.attributes.release)
    }

    fn transaction(&self) -> Option<&str> {
        None
    }

    fn url(&self) -> Option<Url> {
        None
    }

    fn user_agent(&self) -> Option<UserAgent<'_>> {
        self.attributes.user_agent.as_deref().map(UserAgent::Raw)
    }

    fn header(&self, _header_name: &str) -> Option<&str> {
        None
    }
}

impl Filterable for SessionAggregates {
    fn csp(&self) -> Option<&Csp> {
        None
    }

    fn exceptions(&self) -> Option<&Values<Exception>> {
        None
    }

    fn ip_addr(&self) -> Option<&str> {
        self.attributes
            .ip_address
            .as_ref()
            .map(|addr| addr.as_str())
    }

    fn logentry(&self) -> Option<&LogEntry> {
        None
    }

    fn release(&self) -> Option<&str> {
        Some(&self.attributes.release)
    }

    fn transaction(&self) -> Option<&str> {
        None
    }

    fn url(&self) -> Option<Url> {
        None
    }

    fn user_agent(&self) -> Option<UserAgent<'_>> {
        self.attributes.user_agent.as_deref().map(UserAgent::Raw)
    }

    fn header(&self, _header_name: &str) -> Option<&str> {
        None
    }
}

impl Filterable for OurLog {
    fn csp(&self) -> Option<&Csp> {
        None
    }

    fn exceptions(&self) -> Option<&Values<Exception>> {
        None
    }

    fn ip_addr(&self) -> Option<&str> {
        None
    }

    fn logentry(&self) -> Option<&LogEntry> {
        None
    }

    fn release(&self) -> Option<&str> {
        self.attributes
            .value()?
            .get_value("sentry.release")?
            .as_str()
    }

    fn transaction(&self) -> Option<&str> {
        None
    }

    fn url(&self) -> Option<Url> {
        None
    }

    fn user_agent(&self) -> Option<UserAgent<'_>> {
        let attributes = self.attributes.value()?;

        let family = attributes.get_value("sentry.browser.name")?.as_str()?;
        let version = attributes.get_value("sentry.browser.version")?.as_str()?;
        let mut parts = version.splitn(3, '.');

        Some(UserAgent::Parsed(relay_ua::UserAgent {
            family: family.into(),
            major: parts.next().map(Into::into),
            minor: parts.next().map(Into::into),
            patch: parts.next().map(Into::into),
        }))
    }

    fn header(&self, _header_name: &str) -> Option<&str> {
        None
    }
}
