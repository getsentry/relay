use std::sync::Arc;

use crate::envelope::{EnvelopeHeaders, Item, ItemType, Items};
use crate::managed::{
    Counted, Managed, ManagedEnvelope, ManagedResult as _, OutcomeError, Rejected,
};
use crate::processing::{self, Context, Forward, Output, QuotaRateLimiter};
use crate::services::outcome::Outcome;
use crate::services::processor::ProcessingError;

mod filter;
mod process;
mod types;

use relay_event_normalization::GeoIpLookup;
pub use types::ExpandedError;

type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("TODO")]
    InvalidJson(serde_json::Error),
    #[error("envelope processor failed")]
    ProcessingFailed(#[from] ProcessingError),
}

impl OutcomeError for Error {
    type Error = Error;

    fn consume(self) -> (Option<Outcome>, Self::Error) {
        todo!()
    }
}

/// A processor for Error Events.
///
/// It processes all kinds of error events, user feedback, crashes, ...
pub struct ErrorsProcessor {
    limiter: Arc<QuotaRateLimiter>,
    geoip_lookup: GeoIpLookup,
}

impl ErrorsProcessor {
    /// Creates a new [`Self`].
    pub fn new(limiter: Arc<QuotaRateLimiter>, geoip_lookup: GeoIpLookup) -> Self {
        Self {
            limiter,
            geoip_lookup,
        }
    }
}

impl processing::Processor for ErrorsProcessor {
    type UnitOfWork = SerializedError;
    type Output = ErrorOutput;
    type Error = Error;

    fn prepare_envelope(
        &self,
        envelope: &mut ManagedEnvelope,
    ) -> Option<Managed<Self::UnitOfWork>> {
        let has_transaction = envelope
            .envelope()
            .items()
            .any(|item| item.ty() == &ItemType::Transaction);

        if has_transaction {
            return None;
        }

        // let em = envelope.envelope_mut();
        // // Currently these are processed by the error pipeline, but in the future, when we
        // // introduce a proper concept of intermediate products, it's thinkable that we have a
        // // dedicated processor pre-processing security reports.
        // let security_reports = em.take_items_by(|i| matches!(i.ty(), &ItemType::RawSecurity));
        // let require_event_items = em.take_items_by(Item::requires_event);
        //
        // if security_reports.is_empty() && require_event_items.is_empty() {
        //     return None;
        // }

        let items = envelope.envelope_mut().take_items_by(Item::requires_event);

        let errors = SerializedError {
            headers: envelope.envelope().headers().clone(),
            items,
        };

        Some(Managed::with_meta_from(envelope, errors))
    }

    async fn process(
        &self,
        error: Managed<Self::UnitOfWork>,
        ctx: Context<'_>,
    ) -> Result<Output<Self::Output>, Rejected<Self::Error>> {
        // Expand:
        //  - user reports (process_user_reports?)
        //  - unreal (processing)
        //  - playstation (processing)
        //  - nnswitch (processing)
        //  - event::extract (horror)
        let mut error = process::expand(error);

        // Process::
        //  - unreal
        //  - playstation
        //  - attachment create placeholders
        process::process(&mut error);

        // set and validate dsc?

        process::finalize(&mut error, ctx)?;
        process::normalize(&mut error, &self.geoip_lookup, ctx)?;

        let _filter_status = filter::filter(&error, ctx).reject(&error)?;

        // tag with ds decision
        // quotas

        process::scrub(&mut error, ctx)?;

        // serialize
        // emit feedback metrics (maybe useless)

        todo!()
    }
}

#[derive(Debug)]
pub struct SerializedError {
    /// Original envelope headers.
    headers: EnvelopeHeaders,
    // /// List of [`ItemType::RawSecurity`] items.
    // security_reports: Items,
    // require_event_items: Items,
    items: Items,
}

impl Counted for SerializedError {
    fn quantities(&self) -> crate::managed::Quantities {
        todo!()
    }
}

#[derive(Debug)]
pub struct ErrorOutput {}

impl Forward for ErrorOutput {
    fn serialize_envelope(
        self,
        ctx: processing::ForwardContext<'_>,
    ) -> Result<Managed<Box<crate::Envelope>>, Rejected<()>> {
        todo!()
    }

    #[cfg(feature = "processing")]
    fn forward_store(
        self,
        s: processing::StoreHandle<'_>,
        ctx: processing::ForwardContext<'_>,
    ) -> Result<(), Rejected<()>> {
        todo!()
    }
}
