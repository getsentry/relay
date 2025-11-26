use std::marker::PhantomData;

use axum::RequestExt;
use axum::extract::rejection::BytesRejection;
use axum::extract::{FromRequest, Request};
use axum::response::IntoResponse;
use bytes::Bytes;
use relay_dynamic_config::Feature;

use crate::Envelope;
use crate::envelope::{Item, ItemType};
use crate::extractors::{BadEventMeta, RequestMeta};
use crate::integrations::Integration;
use crate::service::ServiceState;

/// Marker indicating a type has been configured on the [`IntegrationBuilder`].
pub struct HasType(());

/// A builder for integration endpoints.
///
/// The builder can be directly extracted from an axum handler.
#[derive(Clone, Debug)]
pub struct IntegrationBuilder<T = ()> {
    envelope: Box<Envelope>,
    payload: Bytes,
    _state: PhantomData<T>,
}

impl IntegrationBuilder<()> {
    /// Configures the [`Integration`] type.
    ///
    /// Setting the type is required.
    pub fn with_type(self, integration: impl Into<Integration>) -> IntegrationBuilder<HasType> {
        self.with_type_and_headers(integration, [])
    }

    /// Configures the [`Integration`] type with additional headers.
    ///
    /// Headers are stored in the item's `other` field and can be retrieved
    /// during processing via `item.get_header()`.
    pub fn with_type_and_headers<I>(
        mut self,
        integration: impl Into<Integration>,
        headers: I,
    ) -> IntegrationBuilder<HasType>
    where
        I: IntoIterator<Item = (String, relay_protocol::Value)>,
    {
        let integration = integration.into();

        self.envelope.add_item({
            let mut item = Item::new(ItemType::Integration);
            item.set_payload(integration.into(), self.payload.clone());
            for (name, value) in headers {
                item.set_header(name, value);
            }
            item
        });

        IntegrationBuilder {
            envelope: self.envelope,
            payload: self.payload,
            _state: Default::default(),
        }
    }
}

impl<T> IntegrationBuilder<T> {
    /// Optionally adds a required feature to the envelope.
    ///
    /// This can be used for integrations which are still feature flagged.
    pub fn with_required_feature(mut self, feature: Feature) -> Self {
        self.envelope.require_feature(feature);
        self
    }
}

impl IntegrationBuilder<HasType> {
    /// Returns the built integration envelope.
    pub fn build(mut self) -> Box<Envelope> {
        self.envelope
            .meta_mut()
            .set_client(crate::constants::CLIENT.to_owned());

        self.envelope
    }
}

impl FromRequest<ServiceState> for IntegrationBuilder {
    type Rejection = IntegrationBuilderRejection;

    async fn from_request(mut req: Request, state: &ServiceState) -> Result<Self, Self::Rejection> {
        let meta: RequestMeta = req.extract_parts_with_state(state).await?;
        let payload = req.extract().await?;

        let envelope = Envelope::from_request(None, meta);

        Ok(Self {
            envelope,
            payload,
            _state: Default::default(),
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum IntegrationBuilderRejection {
    #[error("bad event meta: {0}")]
    BadEventMeta(#[from] BadEventMeta),
    #[error("unable to read body: {0}")]
    Body(#[from] BytesRejection),
}

impl IntoResponse for IntegrationBuilderRejection {
    fn into_response(self) -> axum::response::Response {
        match self {
            Self::BadEventMeta(err) => err.into_response(),
            Self::Body(err) => err.into_response(),
        }
    }
}
