//! TODO: docs

use relay_event_schema::protocol::{Csp, Event, EventType, Exception, LogEntry, Values};
use url::Url;

/// TODO: docs
pub trait Filterable {
    /// TODO: docs

    fn csp(&self) -> Option<&Csp>;
    fn exceptions(&self) -> Option<&Values<Exception>>;
    fn ip_addr(&self) -> Option<&str>;
    fn logentry(&self) -> Option<&LogEntry>;
    fn release(&self) -> Option<&str>;
    fn transaction(&self) -> Option<&str>;
    fn url(&self) -> Option<Url>;
    fn user_agent(&self) -> Option<&str>;

    // fn exceptions(&self) -> Option<&Exce
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
