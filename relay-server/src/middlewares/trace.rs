use relay_log::Level;
use tower_http::classify::{ServerErrorsAsFailures, SharedClassifier};
use tower_http::trace::{DefaultOnFailure, TraceLayer};

pub fn trace_http_layer() -> TraceLayer<SharedClassifier<ServerErrorsAsFailures>> {
    TraceLayer::new_for_http().on_failure(DefaultOnFailure::new().level(Level::DEBUG))
}
