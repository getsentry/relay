use actix_web::State;

use crate::service::ServiceState;

mod event_meta;
mod forwarded_for;
mod signed_json;

pub use self::event_meta::*;
pub use self::forwarded_for::*;
pub use self::signed_json::*;

/// An extractor for the entire service state.
pub type CurrentServiceState = State<ServiceState>;
