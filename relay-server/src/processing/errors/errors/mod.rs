use relay_event_schema::protocol::Event;
use relay_protocol::{Annotated, Empty};
use relay_quotas::DataCategory;

use crate::envelope::{ContentType, EnvelopeHeaders, Item, ItemType, Items};
use crate::managed::{Counted, Quantities};
use crate::processing::errors::Result;
use crate::processing::utils::event::event_category;
use crate::processing::{self, ForwardContext};
use crate::services::processor::ProcessingError;
use crate::statsd::RelayTimers;

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

#[derive(Debug, Copy, Clone)]
pub struct ErrorRef<'a> {
    pub event: &'a Annotated<Event>,
    pub attachments: &'a [Item],
    pub user_reports: &'a [Item],
}

impl ErrorRef<'_> {
    fn to_quantities(self) -> Quantities {
        let mut quantities = self.attachments.quantities();
        quantities.extend(self.user_reports.quantities());
        quantities.push((event_category(self.event).unwrap_or(DataCategory::Error), 1));
        quantities
    }
}

#[derive(Debug)]
pub struct ErrorRefMut<'a> {
    pub event: &'a mut Annotated<Event>,
    pub attachments: &'a mut Items,
    pub user_reports: Option<&'a mut Items>,
}

#[derive(Debug)]
pub struct ErrorItems {
    pub event: Option<Item>,
    pub attachments: Items,
    pub user_reports: Items,
    pub other: Items,
}

impl From<ErrorItems> for Items {
    fn from(value: ErrorItems) -> Self {
        let ErrorItems {
            event,
            attachments,
            user_reports,
            other,
        } = value;

        let mut items = attachments;
        items.reserve_exact(event.is_some() as usize + user_reports.len() + other.len());
        items.extend(user_reports);
        items.extend(other);
        if let Some(event) = event {
            items.push(event);
        }

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
    fn try_expand(items: &mut Items, ctx: Context<'_>) -> Result<Option<ParsedError<Self>>>
    where
        Self: Sized;

    /// Post expansion processing phase for the error.
    ///
    /// Most error events do not need a specific post processing phase and should prefer doing
    /// processing and validation during [expansion](Self::try_expand).
    fn process(&mut self, ctx: Context<'_>) -> Result<()> {
        let _ = ctx;
        Ok(())
    }

    /// Serializes the error back into [`Items`], ready to be attached to an envelope.
    ///
    /// The default implementation serializes all items exposed through [`Self::as_ref_mut`].
    /// Errors which handle with more items must override this implementation.
    fn serialize(mut self, _ctx: ForwardContext<'_>) -> Result<ErrorItems>
    where
        Self: Sized,
    {
        let ErrorRefMut {
            event,
            attachments,
            user_reports,
        } = self.as_ref_mut();

        let event = std::mem::take(event);
        let event = if !event.is_empty() {
            let data = relay_statsd::metric!(timer(RelayTimers::EventProcessingSerialization), {
                event.to_json().map_err(ProcessingError::SerializeFailed)?
            });

            let event_type = event
                .value()
                .and_then(|event| event.ty.value().copied())
                .unwrap_or_default();

            let mut item = Item::new(ItemType::from_event_type(event_type));
            item.set_payload(ContentType::Json, data);
            Some(item)
        } else {
            None
        };

        Ok(ErrorItems {
            event,
            attachments: std::mem::take(attachments),
            user_reports: user_reports.map(std::mem::take).unwrap_or_default(),
            other: Items::new(),
        })
    }

    /// A reference to the contained error data.
    fn as_ref(&self) -> ErrorRef<'_>;
    /// A mutable reference to the contained error data.
    fn as_ref_mut(&mut self) -> ErrorRefMut<'_>;

    /// A shorthand to access the contained error event.
    fn event(&self) -> &Annotated<Event> {
        self.as_ref().event
    }
    /// A shorthand to access the contained error event mutably.
    fn event_mut(&mut self) -> &mut Annotated<Event> {
        self.as_ref_mut().event
    }
}

macro_rules! gen_error_kind {
    ($($name:ident,)*) => {
        #[derive(Debug)]
        pub enum ErrorKind {
            $($name($name),)*
        }

        impl SentryError for ErrorKind {
            fn try_expand(items: &mut Items, ctx: Context<'_>) -> Result<Option<ParsedError<Self>>> {
                $(
                    if let Some(p) = <$name as SentryError>::try_expand(items, ctx)? {
                        return Ok(Some(ParsedError {
                            error: p.error.into(),
                            fully_normalized: p.fully_normalized,
                        }))
                    };
                )*

                Ok(None)
            }

            fn process(&mut self, ctx: Context<'_>) -> Result<()> {
                match self {
                    $(Self::$name(error) => error.process(ctx),)*
                }
            }

            fn serialize(self, ctx: ForwardContext<'_>) -> Result<ErrorItems> {
                match self {
                    $(Self::$name(error) => error.serialize(ctx),)*
                }
            }

            fn as_ref(&self) -> ErrorRef<'_> {
                match self {
                    $(Self::$name(error) => error.as_ref(),)*
                }
            }

            fn as_ref_mut(&mut self) -> ErrorRefMut<'_> {
                match self {
                    $(Self::$name(error) => error.as_ref_mut(),)*
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
    pub error: T,
    pub fully_normalized: bool,
}
