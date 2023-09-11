use axum::http::Uri;
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use rust_embed::RustEmbed;

#[derive(RustEmbed)]
#[folder = "../relay-dashboard/dist/"]
struct Asset;

pub struct StaticFile<T>(pub T);

impl<T> IntoResponse for StaticFile<T>
where
    T: Into<String>,
{
    fn into_response(self) -> Response {
        let path = self.0.into();

        match Asset::get(path.as_str()) {
            Some(content) => {
                let mime = mime_guess::from_path(path).first_or_octet_stream();
                ([(header::CONTENT_TYPE, mime.as_ref())], content.data).into_response()
            }
            None => (StatusCode::NOT_FOUND, "404 Not Found").into_response(),
        }
    }
}

/// Handles only dashboard index page.
pub async fn index_handle() -> impl IntoResponse {
    handle("/index.html".parse::<Uri>().unwrap()).await
}

/// Handles all the incoming requests to the files in `/dashboard/` path.
pub async fn handle(uri: Uri) -> impl IntoResponse {
    let mut path = uri.path().trim_start_matches('/').to_string();

    if path.starts_with("dashboard/") {
        path = path.replace("dashboard/", "");
    }

    StaticFile(path)
}
