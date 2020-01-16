use actix_web::State;

use crate::service::ServiceState;

mod envelope_meta;
mod forwarded_for;
mod signed_json;
mod start_time;

pub use self::envelope_meta::*;
pub use self::forwarded_for::*;
pub use self::signed_json::*;
pub use self::start_time::*;

/// An extractor for the entire service state.
pub type CurrentServiceState = State<ServiceState>;
