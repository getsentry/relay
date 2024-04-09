//! This module contains the trait for items that can be filtered by Inbound Filters, plus
//! the implementation for [`Event`].

use relay_event_schema::protocol::{Csp, Event, EventType, Exception, LogEntry, Values};
use url::Url;

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

    /// The URL from which the data originates. Used for localhost filtering.
    fn url(&self) -> Option<Url>;

    /// The user agent of the client that sent the data.
    fn user_agent(&self) -> Option<&str>;
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

    fn user_agent(&self) -> Option<&str> {
        self.user_agent()
    }
}
