//! Handles event store requests.
use actix_web::middleware::cors::Cors;
use actix_web::{http::Method, FromRequest, HttpResponse, Json, ResponseError};
use uuid::Uuid;

use extractors::{IncomingEvent, ProjectRequest};
use service::{ServiceApp, ServiceState};

use semaphore_aorta::{ApiErrorResponse, StoreChangeset};

#[derive(Serialize)]
struct StoreResponse {
    id: Option<Uuid>,
}

#[derive(Fail, Debug)]
#[fail(display = "event submission rejected")]
struct StoreRejected;

impl ResponseError for StoreRejected {
    fn error_response(&self) -> HttpResponse {
        HttpResponse::Forbidden().json(&ApiErrorResponse::from_fail(self))
    }
}

fn store_event<I: FromRequest<ServiceState> + Into<StoreChangeset>>(
    mut request: ProjectRequest<I>,
) -> Result<Json<StoreResponse>, StoreRejected> {
    let changeset = request.take_payload().into();
    let event_id = changeset.event.id();

    metric!(counter(&format!("event.protocol.v{}", request.auth().version())) += 1);

    if request
        .get_or_create_project_state()
        .store_changeset(changeset)
    {
        metric!(counter("event.accepted") += 1);
        Ok(Json(StoreResponse { id: event_id }))
    } else {
        metric!(counter("event.rejected") += 1);
        Err(StoreRejected)
    }
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
            r.method(Method::POST).with(store_event::<IncomingEvent>);
        })
        .register()
}
