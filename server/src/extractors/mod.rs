use actix_web::State;

use service::ServiceState;

mod signed_json;
mod store_body;
mod forward_body;

pub use self::signed_json::*;
pub use self::store_body::*;
pub use self::forward_body::*;

/// An extractor for the entire service state.
pub type CurrentServiceState = State<ServiceState>;
