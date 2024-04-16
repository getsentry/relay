use std::sync::Arc;

use bytes::Bytes;
use relay_cogs::Cogs;
use relay_config::Config;
use relay_dynamic_config::ErrorBoundary;
use relay_event_schema::protocol::EventId;
use relay_protocol::RuleCondition;
use relay_sampling::config::{DecayingFunction, RuleId, RuleType, SamplingRule, SamplingValue};

use relay_sampling::{DynamicSamplingContext, SamplingConfig};
use relay_system::Addr;
use relay_test::mock_service;

use crate::envelope::{Envelope, Item, ItemType};
use crate::extractors::RequestMeta;
use crate::services::global_config::GlobalConfigHandle;
use crate::services::outcome::TrackOutcome;
use crate::services::processor::{self, EnvelopeProcessorService};
use crate::services::project::ProjectState;
use crate::services::test_store::TestStore;
#[cfg(feature = "processing")]
use crate::utils::BufferGuard;

pub fn state_with_rule_and_condition(
    sample_rate: Option<f64>,
    rule_type: RuleType,
    condition: RuleCondition,
) -> ProjectState {
    let rules = match sample_rate {
        Some(sample_rate) => vec![SamplingRule {
            condition,
            sampling_value: SamplingValue::SampleRate { value: sample_rate },
            ty: rule_type,
            id: RuleId(1),
            time_range: Default::default(),
            decaying_fn: DecayingFunction::Constant,
        }],
        None => Vec::new(),
    };

    let mut state = ProjectState::allowed();
    state.config.sampling = Some(ErrorBoundary::Ok(SamplingConfig {
        rules,
        ..SamplingConfig::new()
    }));
    state
}

pub fn create_sampling_context(sample_rate: Option<f64>) -> DynamicSamplingContext {
    DynamicSamplingContext {
        trace_id: uuid::Uuid::new_v4(),
        public_key: "12345678901234567890123456789012".parse().unwrap(),
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

    let item1 = Item::new(ItemType::Transaction);
    envelope.add_item(item1);

    let item2 = Item::new(ItemType::Attachment);
    envelope.add_item(item2);

    let item3 = Item::new(ItemType::Attachment);
    envelope.add_item(item3);

    envelope
}

pub fn empty_envelope() -> Box<Envelope> {
    empty_envelope_with_dsn("e12d836b15bb49d7bbf99e64295d995b")
}

pub fn empty_envelope_with_dsn(dsn: &str) -> Box<Envelope> {
    let dsn = format!("https://{dsn}:@sentry.io/42").parse().unwrap();

    let mut envelope = Envelope::from_request(Some(EventId::new()), RequestMeta::new(dsn));
    envelope.add_item(Item::new(ItemType::Event));
    envelope
}

pub fn create_test_processor(config: Config) -> EnvelopeProcessorService {
    let (outcome_aggregator, _) = mock_service("outcome_aggregator", (), |&mut (), _| {});
    let (project_cache, _) = mock_service("project_cache", (), |&mut (), _| {});
    let (upstream_relay, _) = mock_service("upstream_relay", (), |&mut (), _| {});
    let (test_store, _) = mock_service("test_store", (), |&mut (), _| {});
    #[cfg(feature = "processing")]
    let (aggregator, _) = mock_service("aggregator", (), |&mut (), _| {});

    #[cfg(feature = "processing")]
    let redis = config
        .redis()
        .filter(|_| config.processing_enabled())
        .map(|redis_config| relay_redis::RedisPool::new(redis_config).unwrap());

    let config = Arc::new(config);
    EnvelopeProcessorService::new(
        Arc::clone(&config),
        GlobalConfigHandle::fixed(Default::default()),
        Cogs::noop(),
        #[cfg(feature = "processing")]
        redis,
        processor::Addrs {
            envelope_processor: Addr::dummy(),
            outcome_aggregator,
            project_cache,
            upstream_relay,
            test_store,
            #[cfg(feature = "processing")]
            aggregator: aggregator.clone(),
            #[cfg(feature = "processing")]
            store_forwarder: None,
        },
        #[cfg(feature = "processing")]
        crate::metric_stats::MetricStats::new(
            config,
            GlobalConfigHandle::fixed(Default::default()),
            aggregator,
        ),
        #[cfg(feature = "processing")]
        Arc::new(BufferGuard::new(usize::MAX)),
    )
}

pub fn processor_services() -> (Addr<TrackOutcome>, Addr<TestStore>) {
    let (outcome_aggregator, _) = mock_service("outcome_aggregator", (), |&mut (), _| {});
    let (test_store, _) = mock_service("test_store", (), |&mut (), _| {});
    (outcome_aggregator, test_store)
}
