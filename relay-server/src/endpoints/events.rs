//! Returns captured events.

use actix_web::actix::MailboxError;
use actix_web::{http::Method, HttpResponse, Path};
use futures::TryFutureExt;

use relay_general::protocol::EventId;

use crate::actors::test_store::{GetCapturedEnvelope, TestStore};
use crate::envelope;
use crate::service::ServiceApp;

async fn get_captured_event(event_id: Path<EventId>) -> Result<HttpResponse, MailboxError> {
    let request = GetCapturedEnvelope {
        event_id: *event_id,
    };

    let envelope_opt = TestStore::from_registry()
        .send(request)
        .await
        .map_err(|_| MailboxError::Closed)?;

    let response = match envelope_opt {
        Some(Ok(envelope)) => HttpResponse::Ok()
            .content_type(envelope::CONTENT_TYPE)
            .body(envelope.to_vec().unwrap()),
        Some(Err(error)) => HttpResponse::BadRequest()
            .content_type("text/plain")
            .body(error),
        None => HttpResponse::NotFound().finish(),
    };

    Ok(response)
}

pub fn configure_app(app: ServiceApp) -> ServiceApp {
    app.resource("/api/relay/events/{event_id}/", |r| {
        r.name("internal-events");
        r.method(Method::GET)
            .with_async(|e| Box::pin(get_captured_event(e)).compat());
    })
}
