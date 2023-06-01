use axum::extract::Multipart;
use bytes::Bytes;

/// Adds the instrumentation on top of the [`bytes::Bytes`].
#[derive(Debug)]
pub struct InstrumentedBytes(pub Bytes);

#[axum::async_trait]
impl<S, B> axum::extract::FromRequest<S, B> for InstrumentedBytes
where
    Bytes: axum::extract::FromRequest<S, B>,
    B: Send + 'static,
    S: Send + Sync,
{
    type Rejection = <Bytes as axum::extract::FromRequest<S, B>>::Rejection;

    #[tracing::instrument(name = "middleware.axum.extractor", skip_all)]
    async fn from_request(req: axum::http::Request<B>, state: &S) -> Result<Self, Self::Rejection> {
        Ok(Self(Bytes::from_request(req, state).await?))
    }
}

/// Adds instrumentation on top of the [`axum::extract::Multipart`].
#[derive(Debug)]
pub struct InstrumentedMultipart(pub Multipart);

#[axum::async_trait]
impl<S, B> axum::extract::FromRequest<S, B> for InstrumentedMultipart
where
    Multipart: axum::extract::FromRequest<S, B>,
    B: Send + 'static,
    S: Send + Sync,
{
    type Rejection = <Multipart as axum::extract::FromRequest<S, B>>::Rejection;

    #[tracing::instrument(name = "middleware.axum.extractor", skip_all)]
    async fn from_request(req: axum::http::Request<B>, state: &S) -> Result<Self, Self::Rejection> {
        Ok(Self(Multipart::from_request(req, state).await?))
    }
}
