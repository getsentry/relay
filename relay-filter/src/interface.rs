//! This module contains the trait for items that can be filtered by Inbound Filters, plus
//! the implementation for [`Event`].
use relay_conventions::{
    BROWSER_NAME, BROWSER_VERSION, RELEASE, SEGMENT_NAME, USER_AGENT_ORIGINAL,
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
        self.data.value()?.client_address.as_str()
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

    fn user_agent(&self) -> UserAgent<'_> {
        let raw = self
            .data
            .value()
            .and_then(|data| data.user_agent_original.as_str());
        UserAgent { raw, parsed: None }
    }
}

impl Filterable for SpanV2 {
    fn release(&self) -> Option<&str> {
        self.attributes.value()?.get_value(RELEASE)?.as_str()
    }

    fn transaction(&self) -> Option<&str> {
        self.attributes.value()?.get_value(SEGMENT_NAME)?.as_str()
    }

    fn user_agent(&self) -> UserAgent<'_> {
        user_agent_from_attributes(&self.attributes)
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

impl Filterable for OurLog {
    fn release(&self) -> Option<&str> {
        self.attributes.value()?.get_value(RELEASE)?.as_str()
    }

    fn user_agent(&self) -> UserAgent<'_> {
        user_agent_from_attributes(&self.attributes)
    }
}

impl Filterable for TraceMetric {
    fn release(&self) -> Option<&str> {
        self.attributes.value()?.get_value(RELEASE)?.as_str()
    }

    fn user_agent(&self) -> UserAgent<'_> {
        user_agent_from_attributes(&self.attributes)
    }
}

fn user_agent_from_attributes(attributes: &relay_protocol::Annotated<Attributes>) -> UserAgent<'_> {
    let parsed = (|| {
        let attributes = attributes.value()?;

        let family = attributes.get_value(BROWSER_NAME)?.as_str()?;
        let version = attributes.get_value(BROWSER_VERSION)?.as_str()?;
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
        .and_then(|attr| attr.get_value(USER_AGENT_ORIGINAL))
        .and_then(|ua| ua.as_str());

    UserAgent { raw, parsed }
}
