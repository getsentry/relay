//! Defines the [`DataCategory`] type that classifies data Relay can handle.

use std::convert::TryInto;
use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::events::EventType;

/// Classifies the type of data that is being ingested.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
#[repr(i8)]
pub enum DataCategory {
    /// Reserved and unused.
    Default = 0,
    /// Error events and Events with an `event_type` not explicitly listed below.
    Error = 1,
    /// Transaction events.
    Transaction = 2,
    /// Events with an event type of `csp`, `hpkp`, `expectct` and `expectstaple`.
    Security = 3,
    /// An attachment. Quantity is the size of the attachment in bytes.
    Attachment = 4,
    /// Session updates. Quantity is the number of updates in the batch.
    Session = 5,
    /// Profile
    ///
    /// This is the category for processed profiles (all profiles, whether or not we store them).
    Profile = 6,
    /// Session Replays
    Replay = 7,
    /// DEPRECATED: A transaction for which metrics were extracted.
    ///
    /// This category is now obsolete because the `Transaction` variant will represent
    /// processed transactions from now on.
    TransactionProcessed = 8,
    /// Indexed transaction events.
    ///
    /// This is the category for transaction payloads that were accepted and stored in full. In
    /// contrast, `transaction` only guarantees that metrics have been accepted for the transaction.
    TransactionIndexed = 9,
    /// Monitor check-ins.
    Monitor = 10,
    /// Indexed Profile
    ///
    /// This is the category for indexed profiles that will be stored later.
    ProfileIndexed = 11,
    /// Span
    ///
    /// Used for spans processed independently of transactions.
    Span = 12,
    /// Monitor Seat
    ///
    /// Represents a monitor job that has scheduled monitor checkins. The seats are not ingested
    /// but we define it here to prevent clashing values since this data category enumeration
    /// is also used outside of Relay via the Python package.
    MonitorSeat = 13,
    /// User Feedback
    ///
    /// Represents a User Feedback processed.
    /// Currently standardized on name UserReportV2 to avoid clashing with the old UserReport.
    /// TODO(jferg): Rename this to UserFeedback once old UserReport is deprecated.
    UserReportV2 = 14,
    //
    // IMPORTANT: After adding a new entry to DataCategory, go to the `relay-cabi` subfolder and run
    // `make header` to regenerate the C-binding. This allows using the data category from Python.
    // Rerun this step every time the **code name** of the variant is updated.
    //
    /// Any other data category not known by this Relay.
    #[serde(other)]
    Unknown = -1,
}

impl DataCategory {
    /// Returns the data category corresponding to the given name.
    pub fn from_name(string: &str) -> Self {
        // TODO: This should probably use serde.
        match string {
            "default" => Self::Default,
            "error" => Self::Error,
            "transaction" => Self::Transaction,
            "security" => Self::Security,
            "attachment" => Self::Attachment,
            "session" => Self::Session,
            "profile" => Self::Profile,
            "profile_indexed" => Self::ProfileIndexed,
            "replay" => Self::Replay,
            "transaction_processed" => Self::TransactionProcessed,
            "transaction_indexed" => Self::TransactionIndexed,
            "monitor" => Self::Monitor,
            "span" => Self::Span,
            "monitor_seat" => Self::MonitorSeat,
            "feedback" => Self::UserReportV2,
            _ => Self::Unknown,
        }
    }

    /// Returns the canonical name of this data category.
    pub fn name(self) -> &'static str {
        // TODO: This should probably use serde.
        match self {
            Self::Default => "default",
            Self::Error => "error",
            Self::Transaction => "transaction",
            Self::Security => "security",
            Self::Attachment => "attachment",
            Self::Session => "session",
            Self::Profile => "profile",
            Self::ProfileIndexed => "profile_indexed",
            Self::Replay => "replay",
            Self::TransactionProcessed => "transaction_processed",
            Self::TransactionIndexed => "transaction_indexed",
            Self::Monitor => "monitor",
            Self::Span => "span",
            Self::MonitorSeat => "monitor_seat",
            Self::UserReportV2 => "feedback",
            Self::Unknown => "unknown",
        }
    }

    /// Returns true if the DataCategory refers to an error (i.e an error event).
    pub fn is_error(self) -> bool {
        matches!(self, Self::Error | Self::Default | Self::Security)
    }

    /// Returns the numeric value for this outcome.
    pub fn value(self) -> Option<u8> {
        // negative values (Internal and Unknown) cannot be sent as
        // outcomes (internally so!)
        (self as i8).try_into().ok()
    }

    /// Returns a dedicated category for indexing if this data can be converted to metrics.
    ///
    /// This returns `None` for most data categories.
    pub fn index_category(self) -> Option<Self> {
        match self {
            Self::Transaction => Some(Self::TransactionIndexed),
            _ => None,
        }
    }
}

impl fmt::Display for DataCategory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}

impl FromStr for DataCategory {
    type Err = ();

    fn from_str(string: &str) -> Result<Self, Self::Err> {
        Ok(Self::from_name(string))
    }
}

impl From<EventType> for DataCategory {
    fn from(ty: EventType) -> Self {
        match ty {
            EventType::Default | EventType::Error | EventType::Nel => Self::Error,
            EventType::Transaction => Self::Transaction,
            EventType::Csp | EventType::Hpkp | EventType::ExpectCt | EventType::ExpectStaple => {
                Self::Security
            }
            EventType::UserReportV2 => Self::UserReportV2,
        }
    }
}
