use actix_web::{actix::ResponseFuture, HttpRequest, HttpResponse};
use futures::{FutureExt, TryFutureExt};

use crate::endpoints::common::{self, BadStoreRequest};
use crate::extractors::RequestMeta;
use crate::service::{ServiceApp, ServiceState};

use relay_feature_flags::FeatureDump;

fn fetch_feature_flags(
    meta: RequestMeta,
    request: HttpRequest<ServiceState>,
) -> ResponseFuture<HttpResponse, BadStoreRequest> {
    let fut = async {
        Ok(HttpResponse::Ok().json(FeatureDump {
            feature_flags: Default::default(),
        }))
    };
    Box::new(fut.boxed_local().compat())
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
