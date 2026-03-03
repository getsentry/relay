use relay_event_schema::protocol::{Event, Metrics};
use relay_protocol::Annotated;
use relay_quotas::{DataCategory, RateLimits};

use crate::envelope::{EnvelopeHeaders, Item};
use crate::managed::{Counted, Quantities, RecordKeeper};
use crate::processing::errors::Result;
use crate::processing::{self, ForwardContext};
use crate::statsd::RelayCounters;

mod apple_crash_report;
mod generic;
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

/// A shape of error Sentry supports.
pub trait SentryError: Counted {
    /// The event category this error/event counts in.
    ///
    /// This category is used for outcomes and rate-limiting.
    ///
    /// This defaults to [`DataCategory::Error`].
    fn event_category(&self) -> DataCategory {
        DataCategory::Error
    }

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
    ) -> Result<()> {
        debug_assert!(
            false,
            "{} has quantities but does not implement `apply_rate_limit`",
            std::any::type_name_of_val(&self),
        );
        let _ = category;
        let _ = limits;
        let _ = records;

        Ok(())
    }

    /// Serializes the error back into items, ready to be attached to an envelope.
    ///
    /// The default implementation does not serialize any data.
    fn serialize_into(self, items: &mut Vec<Item>, ctx: ForwardContext<'_>) -> Result<()>
    where
        Self: Sized,
    {
        debug_assert!(
            self.quantities().is_empty(),
            "{} has quantities but does not implement `serialize_into`",
            std::any::type_name_of_val(&self),
        );
        let _ = items;
        let _ = ctx;
        Ok(())
    }

    /// Returns a minidump item if this error holds a minidump.
    fn minidump_mut(&mut self) -> Option<&mut Item> {
        None
    }
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

            fn serialize_into(self, items: &mut Vec<Item>, ctx: ForwardContext<'_>) -> Result<()> {
                match self {
                    $(Self::$name(error) => error.serialize_into(items, ctx),)*
                }
            }

            fn minidump_mut(&mut self) -> Option<&mut Item> {
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
    pub attachments: Vec<Item>,
    /// A list of user reports, either from the original envelope or created through the expansion.
    pub user_reports: Vec<Item>,
    /// The custom error data.
    pub error: T,
    /// Metrics about the error/event.
    pub metrics: Metrics,
    /// Whether the event can be considered fully normalized.
    ///
    /// Callers must still verify that the information can be trusted.
    pub fully_normalized: bool,
}
