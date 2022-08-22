use actix_web::{actix::ResponseFuture, HttpRequest, HttpResponse};
use futures::{FutureExt, TryFutureExt};

use crate::endpoints::common::{self, BadStoreRequest};
use crate::extractors::RequestMeta;
use crate::service::{ServiceApp, ServiceState};

use relay_feature_flags::{EvaluationRule, EvaluationType, FeatureDump, FeatureFlag};

fn fetch_feature_flags(
    _meta: RequestMeta,
    _request: HttpRequest<ServiceState>,
) -> ResponseFuture<HttpResponse, BadStoreRequest> {
    let fut = async {
        Ok(HttpResponse::Ok().json(FeatureDump {
            feature_flags: [(
                "accessToProfiling".into(),
                FeatureFlag {
                    tags: Default::default(),
                    evaluation: vec![
                        EvaluationRule {
                            ty: EvaluationType::Rollout,
                            percentage: Some(0.5),
                            result: Some(true.into()),
                            tags: Default::default(),
                        },
                        EvaluationRule {
                            ty: EvaluationType::Match,
                            percentage: None,
                            result: Some(true.into()),
                            tags: [("isSentryDev".into(), "true".into())].into(),
                        },
                    ],
                },
            )]
            .into(),
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
