use actix_web::State;

use service::ServiceState;

mod signed_json;

pub use self::signed_json::*;

/// An extractor for the entire service state.
pub type CurrentServiceState = State<ServiceState>;
