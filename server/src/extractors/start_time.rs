use actix_web::{FromRequest, HttpRequest};

pub use crate::middlewares::StartTime;

impl<S> FromRequest<S> for StartTime {
    type Config = ();
    type Result = Self;

    fn from_request(request: &HttpRequest<S>, _cfg: &Self::Config) -> Self::Result {
        *request.extensions().get::<StartTime>().unwrap()
    }
}
