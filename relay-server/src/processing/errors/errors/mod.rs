use relay_event_schema::protocol::{Event, Metrics};
use relay_protocol::Annotated;
use relay_quotas::{DataCategory, RateLimits};

use crate::envelope::{AttachmentItem, EnvelopeHeaders, Item, UnrealReportItem, UserReportItem};
use crate::managed::{Counted, Quantities, RecordKeeper};
use crate::processing;
use crate::processing::errors::Result;
use crate::statsd::RelayCounters;

mod apple_crash_report;
mod generic;
mod gpu;
mod minidump;
mod nswitch;
mod playstation;
mod raw_security;
mod security;
mod unreal;
mod user_report_v2;
mod utils;

pub use self::apple_crash_report::*;
pub use self::generic::*;
pub use self::gpu::*;
pub use self::minidump::*;
pub use self::nswitch::*;
pub use self::playstation::*;
pub use self::raw_security::*;
pub use self::security::*;
pub use self::unreal::*;
pub use self::user_report_v2::*;

/// Context required to expand a [`SentryError`].
#[derive(Debug, Clone, Copy)]
pub struct Context<'a> {
    /// Context from the envelope.
    pub envelope: &'a EnvelopeHeaders,
    /// Processing context.
    pub processing: processing::Context<'a>,
}

#[cfg(test)]
impl Context<'static> {
    /// Returns a [`Context`] with default values for testing.
    pub fn for_test() -> Self {
        use std::sync::LazyLock;

        static ENVELOPE: LazyLock<Box<crate::envelope::Envelope>> =
            LazyLock::new(|| crate::testutils::new_envelope(false, ""));

        Self {
            envelope: ENVELOPE.headers(),
            processing: processing::Context::for_test(),
        }
    }
}

/// Serialized native error data. Unreal reports may be forwarded without expansion;
/// all other remaining native data is already known to consist of attachments.
pub enum ErrorPayload {
    Attachments(smallvec::SmallVec<[AttachmentItem; 2]>),
    UnrealReport(UnrealReportItem),
}

impl ErrorPayload {
    pub fn append_to_envelope(self, items: &mut Vec<Item>) {
        match self {
            Self::Attachments(attachments) => items.extend(attachments.into_iter().map(Item::from)),
            Self::UnrealReport(report) => items.push(report.into()),
        }
    }

    #[cfg(feature = "processing")]
    pub fn append_to_store(self, attachments: &mut Vec<AttachmentItem>) -> Result<()> {
        match self {
            Self::Attachments(native) => {
                attachments.extend(native);
                Ok(())
            }
            Self::UnrealReport(_) => {
                Err(crate::services::processor::ProcessingError::UnsupportedItem.into())
            }
        }
    }
}

/// A shape of error Sentry supports.
pub trait SentryError: Counted {
    /// The event category this error/event counts in.
    ///
    /// This category is used for outcomes and rate-limiting.
    fn event_category(&self) -> DataCategory;

    /// Attempts to parse this error from the passed `items`.
    ///
    /// If parsing modifies the parsed `items` it must either return an error, indicating the
    /// passed items are invalid, or it must return a fully constructed [`Self`].
    ///
    /// The parser may return `Ok(None)` when none of the passed items match this shape of error.
    fn try_expand(items: &mut Vec<Item>, ctx: Context<'_>) -> Result<Option<Expansion<Self>>>
    where
        Self: Sized;

    /// Applies rate limits to the error item.
    fn apply_rate_limit(
        &mut self,
        category: DataCategory,
        limits: RateLimits,
        records: &mut RecordKeeper<'_>,
    ) -> Result<()>;

    /// Serializes the error back into items, ready to be attached to an envelope.
    fn into_payload(self) -> ErrorPayload
    where
        Self: Sized;

    /// Returns a minidump item if this error holds a minidump.
    fn minidump_mut(&mut self) -> Option<&mut AttachmentItem>;
}

macro_rules! gen_error_kind {
    ($($name:ident,)*) => {
        #[derive(Debug)]
        pub enum ErrorKind {
            $($name($name),)*
        }

        impl SentryError for ErrorKind {
            fn event_category(&self) -> DataCategory {
                match self {
                    $(Self::$name(error) => error.event_category(),)*
                }
            }

            fn try_expand(items: &mut Vec<Item>, ctx: Context<'_>) -> Result<Option<Expansion<Self>>> {
                $(
                    if let Some(p) = <$name as SentryError>::try_expand(items, ctx)? {
                        relay_log::debug!("expanded event using: {name}", name = stringify!($name));

                        relay_statsd::metric!(
                            counter(RelayCounters::ErrorProcessed) += 1,
                            expansion = stringify!($name),
                        );

                        return Ok(Some(Expansion {
                            event: p.event,
                            attachments: p.attachments,
                            user_reports: p.user_reports,
                            intermediates: p.intermediates,
                            error: p.error.into(),
                            metrics: p.metrics,
                            fully_normalized: p.fully_normalized,
                        }))
                    };
                )*

                relay_statsd::metric!(
                    counter(RelayCounters::ErrorProcessed) += 1,
                    expansion = "none",
                );

                Ok(None)
            }

            fn apply_rate_limit(
                &mut self,
                category: DataCategory,
                limits: RateLimits,
                records: &mut RecordKeeper<'_>,
            ) -> Result<()> {
                match self {
                    $(Self::$name(error) => error.apply_rate_limit(category, limits, records),)*
                }
            }

            fn into_payload(self) -> ErrorPayload {
                match self { $(Self::$name(error) => error.into_payload(),)* }
            }

            fn minidump_mut(&mut self) -> Option<&mut AttachmentItem> {
                match self {
                    $(Self::$name(error) => error.minidump_mut(),)*
                }
            }
        }

        $(
            impl From<$name> for ErrorKind {
                fn from(value: $name) -> Self {
                    Self::$name(value)
                }
            }
        )*

        impl Counted for ErrorKind {
            fn quantities(&self) -> Quantities {
                match self {
                    $(Self::$name(error) => error.quantities(),)*
                }
            }
        }
    };
}

// Order of these types is important, from most specific to least specific.
//
// For example a Minidump crash may contain an error, which would also be picked up by the generic
// error.
gen_error_kind![
    Nswitch,
    Unreal,
    Minidump,
    GpuCrash,
    AppleCrashReport,
    Playstation,
    Security,
    RawSecurity,
    UserReportV2,
    Generic,
];

/// A successfully expanded error returned from [`SentryError::try_expand`].
#[derive(Debug)]
pub struct Expansion<T> {
    /// The event, may be empty.
    pub event: Box<Annotated<Event>>,
    /// A list of attachments, either from the original envelope or created through the expansion.
    pub attachments: Vec<AttachmentItem>,
    /// A list of user reports, either from the original envelope or created through the expansion.
    pub user_reports: Vec<UserReportItem>,
    /// Additional items extracted from the error that need to be processed in a separate pass.
    pub intermediates: Vec<Item>,
    /// The custom error data.
    pub error: T,
    /// Metrics about the error/event.
    pub metrics: Metrics,
    /// Whether the event can be considered fully normalized.
    ///
    /// Callers must still verify that the information can be trusted.
    pub fully_normalized: bool,
}

#[cfg(test)]
mod tests;
