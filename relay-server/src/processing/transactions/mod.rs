use std::sync::Arc;

use relay_event_normalization::GeoIpLookup;
use relay_event_schema::protocol::SpanV2;
use relay_quotas::DataCategory;

use crate::envelope::EnvelopeHeaders;
use crate::managed::{Counted, Managed, Quantities, TypedEnvelope};
use crate::processing::{Forward, Processor, QuotaRateLimiter};
use crate::services::processor::TransactionGroup;

mod process;

#[derive(Debug, thiserror::Error)]
pub enum Error {}

/// A processor for transactions.
pub struct TransactionProcessor {
    limiter: Arc<QuotaRateLimiter>,
    geo_lookup: GeoIpLookup,
}

impl Processor for TransactionProcessor {
    type UnitOfWork = SerializedTransaction;

    type Output = TransactionOutput;

    type Error = Error;

    fn prepare_envelope(
        &self,
        envelope: &mut crate::managed::ManagedEnvelope,
    ) -> Option<Managed<Self::UnitOfWork>> {
        todo!()
    }

    async fn process(
        &self,
        work: Managed<Self::UnitOfWork>,
        ctx: super::Context<'_>,
    ) -> Result<super::Output<Self::Output>, crate::managed::Rejected<Self::Error>> {
        todo!();
    }
}

/// A transaction in its serialized state, as transported in an envelope.
#[derive(Debug)]
pub struct SerializedTransaction {
    headers: EnvelopeHeaders,
    envelope: TypedEnvelope<TransactionGroup>,
}

impl Counted for SerializedTransaction {
    fn quantities(&self) -> Quantities {
        smallvec::smallvec![
            (DataCategory::Transaction, 1),
            (DataCategory::TransactionIndexed, 1),
        ]
    }
}

pub struct ExpandedTransaction {
    headers: EnvelopeHeaders,
    envelope: TypedEnvelope<TransactionGroup>,
    #[cfg(feature = "processing")]
    extracted_spans: Vec<SpanV2>,
}

impl Counted for ExpandedTransaction {
    fn quantities(&self) -> Quantities {
        smallvec::smallvec![
            (DataCategory::Transaction, 1),
            (DataCategory::TransactionIndexed, 1),
        ]
    }
}

pub enum TransactionOutput {
    NotProcessed(Managed<SerializedTransaction>),
    Processed(Managed<ExpandedTransaction>),
}

impl Forward for TransactionOutput {
    fn serialize_envelope(
        self,
        ctx: super::ForwardContext<'_>,
    ) -> Result<Managed<Box<crate::Envelope>>, crate::managed::Rejected<()>> {
        todo!()
    }

    #[cfg(feature = "processing")]
    fn forward_store(
        self,
        s: &relay_system::Addr<crate::services::store::Store>,
        ctx: super::ForwardContext<'_>,
    ) -> Result<(), crate::managed::Rejected<()>> {
        todo!()
    }
}
