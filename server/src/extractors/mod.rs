use actix_web::State;

use service::ServiceState;

mod event;
mod signed;

pub use self::event::*;
pub use self::signed::*;

/// An extractor for the entire service state.
pub type CurrentServiceState = State<ServiceState>;
