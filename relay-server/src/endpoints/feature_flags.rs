use actix::prelude::*;
use actix_web::{actix::ResponseFuture, HttpRequest, HttpResponse};
use futures01::Future;

use crate::actors::project_cache::{GetProjectState, ProjectCache};
use crate::endpoints::common::{self, BadStoreRequest};
use crate::extractors::RequestMeta;
use crate::service::{ServiceApp, ServiceState};
use crate::utils::ErrorBoundary;

use relay_feature_flags::FeatureDump;

fn fetch_feature_flags(
    meta: RequestMeta,
    _request: HttpRequest<ServiceState>,
) -> ResponseFuture<HttpResponse, BadStoreRequest> {
    // well this will block for `GetProjectState` to come back which is awful for fresh
    // relays.  Gotta figure out somethig better here later.
    let fut = ProjectCache::from_registry()
        .send(GetProjectState::new(meta.public_key()))
        .map_err(|_| BadStoreRequest::ScheduleFailed)
        .map(|state_result| {
            let feature_flags = match state_result {
                Ok(project_state) => project_state
                    .config()
                    .feature_flags
                    .iter()
                    .filter_map(|(k, v)| match v {
                        ErrorBoundary::Ok(val) if val.enabled => Some((k.to_string(), val.clone())),
                        _ => None,
                    })
                    .collect(),
                Err(_) => Default::default(),
            };
            HttpResponse::Ok().json(FeatureDump { feature_flags })
        });
    Box::new(fut)
}

pub fn configure_app(app: ServiceApp) -> ServiceApp {
    common::cors(app)
        .resource(
            &common::normpath(r"/api/{project:\d+}/feature-flags/"),
            |r| {
                r.name("fetch-feature-flags");
                r.post().with(fetch_feature_flags);
            },
        )
        .register()
}
