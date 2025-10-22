use std::sync::Arc;

use relay_event_normalization::GeoIpLookup;
use relay_event_schema::protocol::{Event, SpanV2};
use relay_quotas::DataCategory;
use smallvec::SmallVec;

use crate::Envelope;
use crate::envelope::ContainerItems;
use crate::envelope::{ContainerWriteError, EnvelopeHeaders, Item, ItemContainer, ItemType};
use crate::managed::{Counted, Managed, ManagedEnvelope, ManagedResult, Quantities, Rejected};
use crate::processing::{Forward, Processor, QuotaRateLimiter};
use crate::services::outcome::{DiscardReason, Outcome};
#[cfg(feature = "processing")]
use crate::{processing::spans::store, services::store::StoreEnvelope};

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
        let headers = envelope.envelope().headers().clone();

        let transaction_item = envelope
            .envelope_mut()
            .take_item_by(|item| matches!(*item.ty(), ItemType::Transaction))?;

        let attachment_items = envelope
            .envelope_mut()
            .take_items_by(|item| matches!(*item.ty(), ItemType::Attachment));

        let work = SerializedTransaction {
            headers,
            transaction_item,
            attachment_items,
        };

        Some(Managed::from_envelope(envelope, work))
    }

    async fn process(
        &self,
        work: Managed<Self::UnitOfWork>,
        ctx: super::Context<'_>,
    ) -> Result<super::Output<Self::Output>, Rejected<Self::Error>> {
        todo!();
    }
}

/// A transaction in its serialized state, as transported in an envelope.
#[derive(Debug)]
pub struct SerializedTransaction {
    headers: EnvelopeHeaders,
    transaction_item: Item,
    attachment_items: SmallVec<[Item; 3]>,
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

#[derive(Debug)]
pub struct ExpandedTransaction {
    headers: EnvelopeHeaders,
    transaction: Event,
    attachment_items: Vec<Item>,
    #[cfg(feature = "processing")]
    extracted_spans: ContainerItems<SpanV2>,
}

impl ExpandedTransaction {
    fn serialize_envelope(self) -> Result<Box<Envelope>, ContainerWriteError> {
        let Self {
            headers,
            transaction,
            attachment_items,
            extracted_spans,
        } = self;

        let mut envelope = Envelope::try_from_event(headers, transaction)?;

        if !extracted_spans.is_empty() {
            let mut item = Item::new(ItemType::Span);
            ItemContainer::from(extracted_spans).write_to(&mut item)?;
            envelope.add_item(item);
        }

        for item in attachment_items {
            envelope.add_item(item);
        }

        Ok(envelope)
    }
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

#[derive(Debug)]
pub struct TransactionOutput(Managed<ExpandedTransaction>);

impl Forward for TransactionOutput {
    fn serialize_envelope(
        self,
        ctx: super::ForwardContext<'_>,
    ) -> Result<Managed<Box<Envelope>>, Rejected<()>> {
        self.0.try_map(|output, _| {
            output
                .serialize_envelope()
                .map_err(drop)
                .with_outcome(Outcome::Invalid(DiscardReason::Internal))
        })
    }

    #[cfg(feature = "processing")]
    fn forward_store(
        self,
        s: &relay_system::Addr<crate::services::store::Store>,
        ctx: super::ForwardContext<'_>,
    ) -> Result<(), Rejected<()>> {
        let Self(output) = self;

        let (envelope, spans) = output.split_once(
            |ExpandedTransaction {
                 headers,
                 transaction,
                 attachment_items,
                 extracted_spans,
             }| {
                let mut envelope = Envelope::try_from_event(headers, transaction)
                    .map_err(|e| output.internal_error("failed to create envelope from event"))?;
                for item in attachment_items {
                    envelope.add_item(item);
                }

                (envelope, extracted_spans)
            },
        );

        // Send transaction & attachments in envelope:
        s.send(StoreEnvelope { envelope });

        // Send spans:
        let ctx = store::Context {
            server_sample_rate: spans.server_sample_rate,
            retention: ctx.retention(|r| r.span.as_ref()),
        };
        for span in spans.split(|v| v) {
            if let Ok(span) = span.try_map(|span, _| store::convert(span, &ctx)) {
                s.send(span);
            };
        }

        Ok(())
    }
}
