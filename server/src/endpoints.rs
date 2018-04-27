use actix_web::{HttpResponse, Json};
use actix_web::error::ResponseError;
use http::StatusCode;
use uuid::Uuid;

use smith_aorta::ApiErrorResponse;

use extractors::StoreRequest;

#[derive(Serialize)]
pub struct StoreResponse {
    /// The ID of the stored event
    id: Uuid,
}

#[derive(Fail, Debug)]
#[fail(display = "event submission rejected (invalid or disabled public key)")]
pub struct StoreRejected;

impl ResponseError for StoreRejected {
    fn error_response(&self) -> HttpResponse {
        HttpResponse::build(StatusCode::FORBIDDEN).json(&ApiErrorResponse::from_fail(self))
    }
}

pub fn store(mut request: StoreRequest) -> Result<Json<StoreResponse>, StoreRejected> {
    let trove_state = request.trove_state();
    let mut event = request.take_payload().expect("Should not happen");
    let event_id = *event.id.get_or_insert_with(Uuid::new_v4);
    let project_state = trove_state.get_or_create_project_state(request.project_id());

    if project_state.handle_event(request.auth().public_key().into(), event) {
        Ok(Json(StoreResponse { id: event_id }))
    } else {
        Err(StoreRejected)
    }
}
