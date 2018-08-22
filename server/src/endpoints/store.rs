//! Handles event store requests.
use actix_web::middleware::cors::Cors;
use actix_web::{http::Method, HttpRequest, HttpResponse, Json, ResponseError};
use sentry;
use uuid::Uuid;

use service::{ServiceApp, ServiceState};

use semaphore_aorta::ApiErrorResponse;
use semaphore_common::{Auth, AuthParseError, ProjectId, ProjectIdParseError};

#[derive(Serialize)]
struct StoreResponse {
    id: Option<Uuid>,
}

#[derive(Fail, Debug)]
enum BadStoreRequest {
    #[fail(display = "invalid project path parameter")]
    BadProject(#[cause] ProjectIdParseError),
    #[fail(display = "bad x-sentry-auth header")]
    BadAuth(#[cause] AuthParseError),
}

impl ResponseError for BadStoreRequest {
    fn error_response(&self) -> HttpResponse {
        HttpResponse::BadRequest().json(&ApiErrorResponse::from_fail(self))
    }
}

fn get_auth_from_request<S>(req: &HttpRequest<S>) -> Result<Auth, BadStoreRequest> {
    // try auth from header
    let auth = req
        .headers()
        .get("x-sentry-auth")
        .and_then(|x| x.to_str().ok());

    if let Some(auth) = auth {
        return auth.parse::<Auth>().map_err(BadStoreRequest::BadAuth);
    }

    Auth::from_querystring(req.query_string().as_bytes()).map_err(BadStoreRequest::BadAuth)
}

fn store_event(request: HttpRequest<ServiceState>) -> Result<Json<StoreResponse>, BadStoreRequest> {
    let auth = get_auth_from_request(&request)?;
    let project_id = request
        .match_info()
        .get("project")
        .unwrap_or_default()
        .parse::<ProjectId>()
        .map_err(BadStoreRequest::BadProject)?;

    sentry::configure_scope(|scope| {
        scope.set_user(Some(sentry::User {
            id: Some(project_id.to_string()),
            ..Default::default()
        }));
    });

    unimplemented!()

    // let changeset: StoreChangeset = request.take_payload().into();
    // let event_id = changeset.event.id();

    // metric!(counter(&format!("event.protocol.v{}", request.auth().version())) += 1);

    // if unimplemented!() {
    //     metric!(counter("event.accepted") += 1);
    //     Ok(Json(StoreResponse { id: event_id }))
    // } else {
    //     metric!(counter("event.rejected") += 1);
    //     Err(StoreRejected)
    // }
}

pub fn configure_app(app: ServiceApp) -> ServiceApp {
    Cors::for_app(app)
        .allowed_methods(vec!["POST"])
        .allowed_headers(vec![
            "x-sentry-auth",
            "x-requested-with",
            "origin",
            "accept",
            "content-type",
            "authentication",
        ])
        .max_age(3600)
        .resource(r"/api/{project:\d+}/store/", |r| {
            r.method(Method::POST).with(store_event);
        })
        .register()
}
