use actix_web::State;

use service::ServiceState;

mod signed;

pub use self::signed::*;

/// An extractor for the entire service state.
pub type CurrentServiceState = State<ServiceState>;
