use crate::envelope::{AttachmentItem, Envelope, TransactionItem};

use bytes::Bytes;
use relay_base_schema::project::ProjectId;
use relay_event_schema::protocol::EventId;

use relay_sampling::DynamicSamplingContext;
use relay_system::Addr;

use crate::managed::ManagedEnvelope;

pub fn create_sampling_context(sample_rate: Option<f64>) -> DynamicSamplingContext {
    DynamicSamplingContext {
        trace_id: "67e5504410b1426f9247bb680e5fe0c8".parse().unwrap(),
        public_key: "12345678901234567890123456789012".parse().unwrap(),
        project_id: Some(ProjectId::new(42)),
        release: None,
        environment: None,
        transaction: None,
        sample_rate,
        user: Default::default(),
        replay_id: None,
        sampled: None,
        other: Default::default(),
    }
}

/// ugly hack to build an envelope with an optional trace context
pub fn new_envelope<T: Into<String>>(with_dsc: bool, transaction_name: T) -> Box<Envelope> {
    let transaction_name = transaction_name.into();
    let dsn = "https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42";
    let event_id = EventId::new();

    let raw_event = if with_dsc {
        format!(
            "{{\"transaction\": \"{}\", \"event_id\":\"{}\",\"dsn\":\"{}\", \"trace\": {}}}\n",
            transaction_name,
            event_id.0.as_simple(),
            dsn,
            serde_json::to_string(&create_sampling_context(None)).unwrap(),
        )
    } else {
        format!(
            "{{\"transaction\": \"{}\", \"event_id\":\"{}\",\"dsn\":\"{}\"}}\n",
            transaction_name,
            event_id.0.as_simple(),
            dsn,
        )
    };

    let bytes = Bytes::from(raw_event);

    let mut envelope = Envelope::parse_bytes(bytes).unwrap();

    let item1 = TransactionItem::new();
    envelope.add_item(item1);

    let item2 = AttachmentItem::new();
    envelope.add_item(item2);

    let item3 = AttachmentItem::new();
    envelope.add_item(item3);

    envelope
}

pub fn new_managed_envelope<T: Into<String>>(
    with_dsc: bool,
    transaction_name: T,
) -> ManagedEnvelope {
    ManagedEnvelope::new(new_envelope(with_dsc, transaction_name), Addr::dummy())
}
