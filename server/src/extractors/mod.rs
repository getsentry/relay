use actix_web::State;

use service::ServiceState;

mod event;
mod project;
mod signed;

pub use self::event::*;
pub use self::project::*;
pub use self::signed::*;

/// An extractor for the entire service state.
pub type CurrentServiceState = State<ServiceState>;
