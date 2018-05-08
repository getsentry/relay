//! Handles event store requests.
use std::sync::Arc;

use actix_web::{FromRequest, HttpResponse, Json, ResponseError, http::Method};
use actix_web::middleware::cors::Cors;
use uuid::Uuid;

use service::ServiceApp;
use extractors::{IncomingEvent, IncomingForeignEvent, ProjectRequest};

use semaphore_trove::TroveState;
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

fn store_event<I: FromRequest<Arc<TroveState>> + Into<StoreChangeset>>(
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
        .resource(r"/api/{project:\d+}/{store_type:[a-z][a-z0-9-]*}/", |r| {
            r.method(Method::POST)
                .with(store_event::<IncomingForeignEvent>);
        })
        .register()
}
