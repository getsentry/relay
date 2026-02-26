use relay_event_schema::protocol::{Event, Metrics};
use relay_protocol::Annotated;

use crate::envelope::{EnvelopeHeaders, Item};
use crate::managed::{Counted, Quantities};
use crate::processing::errors::Result;
use crate::processing::{self, ForwardContext};

mod apple_crash_report;
mod attachments;
mod form_data;
mod generic;
mod minidump;
mod nnswitch;
mod playstation;
mod raw_security;
mod security;
mod unreal;
mod user_report_v2;
mod utils;

pub use self::apple_crash_report::*;
pub use self::attachments::*;
pub use self::form_data::*;
pub use self::generic::*;
pub use self::minidump::*;
pub use self::nnswitch::*;
pub use self::playstation::*;
pub use self::raw_security::*;
pub use self::security::*;
pub use self::unreal::*;
pub use self::user_report_v2::*;

#[derive(Debug)]
pub struct ErrorItems {
    pub attachments: Vec<Item>,
    pub user_reports: Vec<Item>,
    pub other: Vec<Item>,
}

impl From<ErrorItems> for Vec<Item> {
    fn from(value: ErrorItems) -> Self {
        let ErrorItems {
            attachments,
            user_reports,
            other,
        } = value;

        let mut items = attachments;
        items.reserve_exact(user_reports.len() + other.len());
        items.extend(user_reports);
        items.extend(other);

        items
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Context<'a> {
    pub envelope: &'a EnvelopeHeaders,
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

/// A shape of error Sentry supports.
pub trait SentryError: Counted {
    /// Attempts to parse this error from the passed [`items`].
    ///
    /// If parsing modifies the parsed `items` it must either return an error, indicating the
    /// passed items are invalid, or it must return a fully constructed [`Self`].
    ///
    /// The parser may return `Ok(None)` when none of the passed items match this shape of error.
    fn try_expand(items: &mut Vec<Item>, ctx: Context<'_>) -> Result<Option<ParsedError<Self>>>
    where
        Self: Sized;

    /// Serializes the error back into items, ready to be attached to an envelope.
    ///
    /// The default implementation serializes all items exposed through [`Self::as_ref_mut`].
    /// Errors which handle with more items must override this implementation.
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
}

macro_rules! gen_error_kind {
    ($($name:ident,)*) => {
        #[derive(Debug)]
        pub enum ErrorKind {
            $($name($name),)*
        }

        impl SentryError for ErrorKind {
            fn try_expand(items: &mut Vec<Item>, ctx: Context<'_>) -> Result<Option<ParsedError<Self>>> {
                $(
                    if let Some(p) = <$name as SentryError>::try_expand(items, ctx)? {
                        relay_log::debug!("expanded event using: {name}", name = stringify!($name));
                        return Ok(Some(ParsedError {
                            event: p.event,
                            attachments: p.attachments,
                            user_reports: p.user_reports,
                            error: p.error.into(),
                            metrics: p.metrics,
                            fully_normalized: p.fully_normalized,
                        }))
                    };
                )*

                Ok(None)
            }

            // fn process(&mut self, ctx: Context<'_>) -> Result<()> {
            //     match self {
            //         $(Self::$name(error) => error.process(ctx),)*
            //     }
            // }

            fn serialize_into(self, items: &mut Vec<Item>, ctx: ForwardContext<'_>) -> Result<()> {
                match self {
                    $(Self::$name(error) => error.serialize_into(items, ctx),)*
                }
            }
            //
            // fn as_ref(&self) -> ErrorRef<'_> {
            //     match self {
            //         $(Self::$name(error) => error.as_ref(),)*
            //     }
            // }
            //
            // fn as_ref_mut(&mut self) -> ErrorRefMut<'_> {
            //     match self {
            //         $(Self::$name(error) => error.as_ref_mut(),)*
            //     }
            // }
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
    Nnswitch,
    Unreal,
    Minidump,
    AppleCrashReport,
    Playstation,
    Security,
    RawSecurity,
    UserReportV2,
    FormData,
    Attachments,
    Generic,
];

// TODO: this may need a better name
#[derive(Debug)]
pub struct ParsedError<T> {
    pub event: Annotated<Event>,
    pub attachments: Vec<Item>,
    pub user_reports: Vec<Item>,
    pub error: T,
    pub metrics: Metrics,
    pub fully_normalized: bool,
}
