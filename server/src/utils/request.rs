use actix_web::{http::header, HttpRequest};

// Resolve the content length from HTTP request headers.
pub fn get_content_length<S>(req: &HttpRequest<S>) -> Option<usize> {
    req.headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|h| h.to_str().ok())
        .and_then(|s| s.parse().ok())
}
