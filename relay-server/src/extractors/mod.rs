use actix_web::State;

use crate::service::ServiceState;

mod forwarded_for;
mod request_meta;
mod shared_payload;
mod signed_json;
mod start_time;

pub use self::forwarded_for::*;
pub use self::request_meta::*;
pub use self::shared_payload::*;
pub use self::signed_json::*;
pub use self::start_time::*;

/// An extractor for the entire service state.
pub type CurrentServiceState = State<ServiceState>;
