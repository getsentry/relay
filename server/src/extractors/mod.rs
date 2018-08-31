use actix_web::State;

use service::ServiceState;

mod event_meta;
mod forwarded_for;

pub use self::event_meta::*;
pub use self::forwarded_for::*;

/// An extractor for the entire service state.
pub type CurrentServiceState = State<ServiceState>;
