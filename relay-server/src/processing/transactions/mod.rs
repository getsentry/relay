use std::sync::Arc;

use relay_event_normalization::GeoIpLookup;
use relay_event_schema::protocol::{Event, SpanV2};
use relay_quotas::DataCategory;
use sentry::protocol::Attachment;

use crate::envelope::{EnvelopeHeaders, Item};
use crate::managed::{Counted, Managed, ManagedEnvelope, Quantities, TypedEnvelope};
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
        envelope: &mut ManagedEnvelope,
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
    transaction_item: Item,
    attachment_items: Vec<Item>,
    // TODO: Other dependents?
}

impl Counted for SerializedTransaction {
    fn quantities(&self) -> Quantities {
        smallvec::smallvec![
            (DataCategory::Transaction, 1),
            (DataCategory::TransactionIndexed, 1),
            (DataCategory::AttachmentItem, self.attachment_items.len()),
            (
                DataCategory::Attachment,
                self.attachment_items.iter().map(Item::len).sum()
            ),
        ]
    }
}

pub struct ExpandedTransaction {
    headers: EnvelopeHeaders,
    transaction: Event,
    attachment_items: Vec<Item>,
    #[cfg(feature = "processing")]
    extracted_spans: Vec<SpanV2>,
}

impl Counted for ExpandedTransaction {
    fn quantities(&self) -> Quantities {
        smallvec::smallvec![
            (DataCategory::Transaction, 1),
            (DataCategory::TransactionIndexed, 1),
            (DataCategory::AttachmentItem, self.attachment_items.len()),
            (
                DataCategory::Attachment,
                self.attachment_items.iter().map(Item::len).sum()
            ),
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
