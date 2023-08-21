//! Endpoints for Network Error Logging reports.

use axum::extract::{DefaultBodyLimit, FromRequest, Query};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{post, MethodRouter};
use bytes::Bytes;
use relay_config::Config;
use relay_general::protocol::EventId;
use serde::Deserialize;
use serde_json::Value;

use crate::endpoints::common::{self, BadStoreRequest};
use crate::envelope::{ContentType, Envelope, Item, ItemType};
use crate::extractors::{Mime, RequestMeta};
use crate::service::ServiceState;

#[derive(Debug, Deserialize)]
struct NelReportQuery {
    sentry_release: Option<String>,
    sentry_environment: Option<String>,
}

#[derive(Debug, FromRequest)]
#[from_request(state(ServiceState))]
struct NelReportParams {
    meta: RequestMeta,
    #[from_request(via(Query))]
    query: NelReportQuery,
    body: Bytes,
}

impl NelReportParams {
    fn extract_envelope(self) -> Result<Box<Envelope>, BadStoreRequest> {
        let Self { meta, query, body } = self;

        println!("{meta:#?}");
        println!("{query:#?}");
        println!("{body:#?}");

        if body.is_empty() {
            return Err(BadStoreRequest::EmptyBody);
        }

        let items: Vec<Value> = serde_json::from_slice(&body).unwrap();
        println!("{items:#?}");

        let mut envelope = Envelope::from_request(Some(EventId::new()), meta);

        for item in items.into_iter() {
            let mut report_item = Item::new(ItemType::Nel);
            // let i = item.to_string().as_bytes().clone();
            report_item.set_payload(ContentType::Json, item.as_str().unwrap());
            envelope.add_item(report_item);
        }

        // for item in items.iter() {
        //     let mut report_item = Item::new(ItemType::Event);
        //     report_item.set_payload(ContentType::Json, serde_json::to_string(item));

        //     if let Some(sentry_release) = query.sentry_release {
        //         report_item.set_header("sentry_release", sentry_release);
        //     }

        //     if let Some(sentry_environment) = query.sentry_environment {
        //         report_item.set_header("sentry_environment", sentry_environment);
        //     }

        // }

        Ok(envelope)
    }
}

fn is_nel_mime(mime: Mime) -> bool {
    let ty = mime.type_().as_str();
    let subty = mime.subtype().as_str();
    let suffix = mime.suffix().map(|suffix| suffix.as_str());

    // println!("{ty:#?}");
    // println!("{subty:#?}");
    // println!("{suffix:#?}");

    matches!(
        (ty, subty, suffix),
        ("application", "json", None) | ("application", "reports", Some("json"))
    )
}

/// This handles all messages coming on the NEL endpoint.
async fn handle(
    state: ServiceState,
    mime: Mime,
    params: NelReportParams,
) -> Result<impl IntoResponse, BadStoreRequest> {
    if !is_nel_mime(mime) {
        return Ok(StatusCode::UNSUPPORTED_MEDIA_TYPE.into_response());
    }

    let envelope = params.extract_envelope()?;
    common::handle_envelope(&state, envelope).await?;
    Ok(().into_response())
}

pub fn route<B>(config: &Config) -> MethodRouter<ServiceState, B>
where
    B: axum::body::HttpBody + Send + 'static,
    B::Data: Send,
    B::Error: Into<axum::BoxError>,
{
    post(handle).route_layer(DefaultBodyLimit::max(config.max_event_size()))
}
