use std::borrow::Cow;
use std::task::{Context, Poll};

use axum::http::{Request, Uri};
use axum::response::Response;
use once_cell::sync::Lazy;
use regex::Regex;
use tower::{Layer, Service};

/// Layer that applies [`NormalizePath`], which normalizes paths.
#[derive(Clone, Copy, Debug)]
pub struct NormalizePathLayer;

impl NormalizePathLayer {
    pub fn new() -> Self {
        Self
    }
}

impl<S> Layer<S> for NormalizePathLayer {
    type Service = NormalizePath<S>;

    fn layer(&self, inner: S) -> Self::Service {
        NormalizePath::new(inner)
    }
}

/// Normalizes URLs with redundant slashes.
///
/// Any groups of slashes will be collapsed into a single slash. For example, a request with
/// `//foo///` will be changed to `/foo/`.
#[derive(Clone, Copy, Debug)]
pub struct NormalizePath<S> {
    inner: S,
}

impl<S> NormalizePath<S> {
    pub fn new(inner: S) -> Self {
        Self { inner }
    }
}

impl<S, I, O> Service<Request<I>> for NormalizePath<S>
where
    S: Service<Request<I>, Response = Response<O>>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    #[inline]
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut req: Request<I>) -> Self::Future {
        fold_duplicate_slashes(req.uri_mut());
        self.inner.call(req)
    }
}

fn fold_duplicate_slashes(uri: &mut Uri) {
    static REPLACE: Lazy<Regex> = Lazy::new(|| Regex::new("/{2,}").unwrap());

    let Cow::Owned(new_path) = REPLACE.replace_all(uri.path(), "/") else {
        return;
    };

    let path_and_query = match uri.query() {
        Some(query) => format!("{new_path}?{query}"),
        None => new_path,
    };

    let mut builder = Uri::builder().path_and_query(path_and_query);
    if let Some(scheme) = uri.scheme() {
        builder = builder.scheme(scheme.clone());
    }
    if let Some(authority) = uri.authority() {
        builder = builder.authority(authority.clone());
    }

    if let Ok(new_uri) = builder.build() {
        *uri = new_uri;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use similar_asserts::assert_eq;

    #[test]
    fn root() {
        let mut uri = "/".parse().unwrap();
        fold_duplicate_slashes(&mut uri);
        assert_eq!("/", uri.to_string());
    }

    #[test]
    fn path() {
        let mut uri = "///hello///world///".parse().unwrap();
        fold_duplicate_slashes(&mut uri);
        assert_eq!("/hello/world/", uri.to_string());
    }

    #[test]
    fn no_trailing_slash() {
        let mut uri = "/hello".parse().unwrap();
        fold_duplicate_slashes(&mut uri);
        assert_eq!("/hello", uri.to_string());
    }

    #[test]
    fn query_and_fragment() {
        let mut uri = "//hello//?world=true///".parse().unwrap();
        fold_duplicate_slashes(&mut uri);
        assert_eq!("/hello/?world=true///", uri.to_string());
    }
}
