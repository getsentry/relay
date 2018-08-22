use std::cell::Ref;

use actix_web::error::{Error, ResponseError};
use actix_web::{FromRequest, HttpMessage, HttpRequest, HttpResponse};
use futures::{future, Future};

use semaphore_aorta::{ApiErrorResponse};
use semaphore_common::{Auth, AuthParseError, ProjectId, ProjectIdParseError};

use service::ServiceState;

use sentry;

#[derive(Fail, Debug)]
pub enum BadProjectRequest {
    #[fail(display = "invalid project path parameter")]
    BadProject(#[cause] ProjectIdParseError),
    #[fail(display = "bad x-sentry-auth header")]
    BadAuth(#[cause] AuthParseError),
}

impl ResponseError for BadProjectRequest {
    fn error_response(&self) -> HttpResponse {
        HttpResponse::BadRequest().json(&ApiErrorResponse::from_fail(self))
    }
}

/// A common extractor for handling project requests.
///
/// This extractor not only validates the authentication and project id
/// from the URL but also exposes that information in request extensions
/// so that other systems can access it.
///
/// In particular it wraps another extractor that can give convenient
/// access to the request's payload.
#[derive(Debug)]
pub struct ProjectRequest<T: FromRequest<ServiceState> + 'static> {
    http_req: HttpRequest<ServiceState>,
    payload: Option<T>,
}

impl<T: FromRequest<ServiceState> + 'static> ProjectRequest<T> {
    /// Returns the project identifier for this request.
    pub fn project_id(&self) -> ProjectId {
        *self.http_req.extensions().get().unwrap()
    }

    /// Returns the auth info
    pub fn auth(&self) -> Ref<Auth> {
        Ref::map(self.http_req.extensions(), |ext| ext.get().unwrap())
    }

    /// Extracts the embedded payload.
    ///
    /// This can only be called once.  Panics if there is no payload.
    pub fn take_payload(&mut self) -> T {
        self.payload.take().unwrap()
    }
}

fn get_auth_from_request<S>(req: &HttpRequest<S>) -> Result<Auth, BadProjectRequest> {
    // try auth from header
    let auth = req
        .headers()
        .get("x-sentry-auth")
        .and_then(|x| x.to_str().ok());

    if let Some(auth) = auth {
        match auth.parse::<Auth>() {
            Ok(val) => return Ok(val),
            Err(err) => return Err(BadProjectRequest::BadAuth(err)),
        }
    }

    // fall back to auth from url
    Auth::from_querystring(req.query_string().as_bytes()).map_err(BadProjectRequest::BadAuth)
}

impl<T: FromRequest<ServiceState> + 'static> FromRequest<ServiceState> for ProjectRequest<T> {
    type Config = <T as FromRequest<ServiceState>>::Config;
    type Result = Box<Future<Item = Self, Error = Error>>;

    fn from_request(req: &HttpRequest<ServiceState>, cfg: &Self::Config) -> Self::Result {
        let req = req.clone();
        let auth = match get_auth_from_request(&req) {
            Ok(auth) => auth,
            Err(err) => return Box::new(future::err(err.into())),
        };

        req.extensions_mut().insert(auth.clone());

        let project_id = match req
            .match_info()
            .get("project")
            .unwrap_or_default()
            .parse::<ProjectId>()
            .map_err(BadProjectRequest::BadProject)
        {
            Ok(project_id) => project_id,
            Err(err) => return Box::new(future::err(err.into())),
        };

        sentry::configure_scope(|scope| {
            scope.set_user(Some(sentry::User {
                id: Some(project_id.to_string()),
                ..Default::default()
            }));
        });

        req.extensions_mut().insert(project_id);

        Box::new(
            T::from_request(&req, cfg)
                .into()
                .map(move |payload| ProjectRequest {
                    http_req: req,
                    payload: Some(payload),
                }),
        )
    }
}
