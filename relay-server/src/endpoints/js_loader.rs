use futures::prelude::*;

use actix::{MailboxError, ResponseFuture};
use actix_web::{HttpRequest, HttpResponse, ResponseError};
use failure::Fail;
use lazy_static::lazy_static;
use regex::{Captures, Regex};
use serde::Serialize;

use relay_common::clone;

use crate::actors::project_cache::FetchProjectState;
use crate::endpoints::common;
use crate::extractors::RequestMeta;
use crate::service::{ServiceApp, ServiceState};
use crate::utils::ApiErrorResponse;

lazy_static! {
    static ref VAR_RE: Regex = Regex::new(r"\b__(PUBLIC_KEY|CDN_URL|OPTIONS)__\b").unwrap();
}
static JS_LOADER_MINIFIED: &str = include_str!("loader.min.js");

#[derive(Fail, Debug)]
pub enum BadJsSdkRequest {
    #[fail(display = "could not schedule event processing")]
    ScheduleFailed(#[cause] MailboxError),
    #[fail(display = "javascript loader not found")]
    NotFound,
}

impl ResponseError for BadJsSdkRequest {
    fn error_response(&self) -> HttpResponse {
        let body = ApiErrorResponse::from_fail(self);
        match self {
            BadJsSdkRequest::ScheduleFailed(_) => {
                // These errors indicate that something's wrong with our actor system, most likely
                // mailbox congestion or a faulty shutdown. Indicate an unavailable service to the
                // client. It might retry event submission at a later time.
                HttpResponse::ServiceUnavailable().json(&body)
            }
            BadJsSdkRequest::NotFound => {
                // bad key or unauthenticated key
                HttpResponse::Forbidden().json(&body)
            }
        }
    }
}

#[derive(Serialize)]
struct JsSdkConfig<'a> {
    dsn: &'a str,
}

fn dump<S: Serialize>(s: &S) -> String {
    serde_json::to_string(s).unwrap()
}

fn js_sdk_loader(
    request: HttpRequest<ServiceState>,
    meta: RequestMeta,
) -> ResponseFuture<HttpResponse, BadJsSdkRequest> {
    let config = request.state().config();
    let project_manager = request.state().project_cache();
    let public_key = meta.public_key();

    let future = project_manager
        .send(FetchProjectState { public_key })
        .map_err(BadJsSdkRequest::ScheduleFailed)
        .and_then(clone!(public_key, config, |project_state| {
            let project_state = if let Ok(project_state) = project_state {
                project_state.state
            } else {
                return Err(BadJsSdkRequest::NotFound);
            };
            let key_config = project_state
                .get_public_key_config()
                .ok_or(BadJsSdkRequest::NotFound)?;
            let dsn = project_state
                .get_dsn(&config)
                .ok_or(BadJsSdkRequest::NotFound)?;
            Ok(HttpResponse::Ok()
                .content_type("text/javascript")
                .body(VAR_RE.replace_all(JS_LOADER_MINIFIED, |c: &Captures| {
                    match c.get(1).map(|x| x.as_str()) {
                        Some("PUBLIC_KEY") => dump(&public_key.as_str()),
                        Some("OPTIONS") => dump(&JsSdkConfig { dsn: &dsn }),
                        Some("CDN_URL") => {
                            // if sentry does not supply a js_sdk_url we just empty an empty string
                            // here.  This is not useful as this makes the loader fail but there
                            // is currently no point in hard coding an old loader version here.
                            // alternatively we could make the entire loader fail with a 404.
                            dump(&key_config.js_sdk_url.as_deref().unwrap_or(""))
                        }
                        _ => "null".to_string(),
                    }
                })))
        }));

    Box::new(future)
}

pub fn configure_app(app: ServiceApp) -> ServiceApp {
    common::cors(app)
        .resource(
            r"/js-sdk-loader/{sentry_key:[a-f0-9]+}{minified:(\.min)?}.js",
            |r| {
                r.name("js-sdk-loader");
                r.get().with(js_sdk_loader);
            },
        )
        .register()
}
