use relay_base_schema::events::EventType;
use relay_event_schema::protocol::{Event, Metrics};
use relay_protocol::Annotated;

use crate::envelope::{EnvelopeHeaders, Item, ItemType, Items};
use crate::managed::{Counted, Managed, Quantities, Rejected};
use crate::processing;
use crate::processing::errors::{Error, Result};
use crate::processing::utils::event::EventFullyNormalized;

mod generic;
mod nnswitch;
mod utils;

pub use self::generic::*;
pub use self::nnswitch::*;

#[derive(Debug, Copy, Clone)]
pub struct ErrorRef<'a> {
    pub event: &'a Annotated<Event>,
    pub attachments: &'a Items,
}

#[derive(Debug)]
pub struct ErrorRefMut<'a> {
    pub event: &'a mut Annotated<Event>,
    pub attachments: &'a mut Items,
}

#[derive(Debug)]
pub struct Flags {
    pub fully_normalized: EventFullyNormalized,
}

#[derive(Debug)]
pub struct ExpandedError {
    pub headers: EnvelopeHeaders,
    pub flags: Flags,
    pub metrics: Metrics,

    pub error: ErrorKind,
}

impl Counted for ExpandedError {
    fn quantities(&self) -> Quantities {
        todo!()
    }
}

impl processing::RateLimited for Managed<ExpandedError> {
    type Output = Self;
    type Error = Error;

    async fn enforce<R>(
        self,
        rate_limiter: R,
        ctx: processing::Context<'_>,
    ) -> Result<Self::Output, Rejected<Self::Error>>
    where
        R: processing::RateLimiter,
    {
        todo!()
    }
}

pub trait SentryError {
    fn try_parse(items: &mut Items) -> Result<Option<ParsedError<Self>>>
    where
        Self: Sized;

    fn as_ref(&self) -> ErrorRef<'_>;
    fn as_ref_mut(&mut self) -> ErrorRefMut<'_>;

    fn event(&self) -> &Annotated<Event> {
        self.as_ref().event
    }
    fn event_mut(&mut self) -> &mut Annotated<Event> {
        self.as_ref_mut().event
    }
}

macro_rules! gen_error_kind {
    ($($variant:ident => $ty:ty,)*) => {
        #[derive(Debug)]
        pub enum ErrorKind {
            $($variant($ty),)*
        }

        impl SentryError for ErrorKind {
            fn try_parse(items: &mut Items) -> Result<Option<ParsedError<Self>>> {
                $(
                    if let Some(p) = <$ty as SentryError>::try_parse(items)? {
                        return Ok(Some(ParsedError {
                            error: p.error.into(),
                            fully_normalized: p.fully_normalized,
                        }))
                    };
                )*

                Ok(None)
            }

            fn as_ref(&self) -> ErrorRef<'_> {
                match self {
                    $(Self::$variant(error) => error.as_ref(),)*
                }
            }

            fn as_ref_mut(&mut self) -> ErrorRefMut<'_> {
                match self {
                    $(Self::$variant(error) => error.as_ref_mut(),)*
                }
            }
        }

        $(
            impl From<$ty> for ErrorKind {
                fn from(value: $ty) -> Self {
                    Self::$variant(value)
                }
            }
        )*
    };
}

gen_error_kind!(
    Nnswitch => Nnswitch,
    Generic => Generic,

);

#[derive(Debug)]
pub struct ParsedError<T> {
    pub error: T,
    pub fully_normalized: bool,
}

pub struct UserFeedback {
    // Maybe this is generic
    // event
    // ???
}

pub struct MinidumpError {
    // event
    // minidump
    // other attachments?
}

pub struct AppleCrashReportError {
    // event
    // apple crash report
    // other attachments?
}

pub struct UnrealError {}

pub struct PlaystationError {}

pub struct MaybeSomethingWithOutEventJustToForward {
    // Playstation/prospero only converts in processing
    // Or allow Annotated::empty for events
}
// impl TryFrom<SerializedError> for ExpandedEvent {
//     type Error = Error;
//
//     fn try_from(value: SerializedError) -> Result<Self> {
//         todo!()
//     }
// }
