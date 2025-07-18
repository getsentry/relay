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
#[cfg(feature = "processing")]
use relay_system::Service;
use relay_test::mock_service;

use crate::envelope::{Envelope, Item, ItemType};
use crate::managed::ManagedEnvelope;
use crate::metrics::{MetricOutcomes, MetricStats};
#[cfg(feature = "processing")]
use crate::service::create_redis_clients;
use crate::services::global_config::GlobalConfigHandle;
#[cfg(feature = "processing")]
use crate::services::global_rate_limits::GlobalRateLimitsService;
use crate::services::outcome::TrackOutcome;
use crate::services::processor::{self, EnvelopeProcessorService, EnvelopeProcessorServicePool};
use crate::services::projects::cache::ProjectCacheHandle;
use crate::services::projects::project::ProjectInfo;
use crate::services::test_store::TestStore;
use crate::utils::ThreadPoolBuilder;

pub fn state_with_rule_and_condition(
    sample_rate: Option<f64>,
    rule_type: RuleType,
    condition: RuleCondition,
) -> ProjectInfo {
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

    let mut state = ProjectInfo::default();
    state.config.sampling = Some(ErrorBoundary::Ok(SamplingConfig {
        rules,
        ..SamplingConfig::new()
    }));
    state
}

pub fn create_sampling_context(sample_rate: Option<f64>) -> DynamicSamplingContext {
    DynamicSamplingContext {
        trace_id: "67e5504410b1426f9247bb680e5fe0c8".parse().unwrap(),
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

pub fn new_managed_envelope<T: Into<String>>(
    with_dsc: bool,
    transaction_name: T,
) -> ManagedEnvelope {
    ManagedEnvelope::new(
        new_envelope(with_dsc, transaction_name),
        Addr::dummy(),
        Addr::dummy(),
    )
}

pub async fn create_test_processor(config: Config) -> EnvelopeProcessorService {
    let (outcome_aggregator, _) = mock_service("outcome_aggregator", (), |&mut (), _| {});
    let (aggregator, _) = mock_service("aggregator", (), |&mut (), _| {});
    let (upstream_relay, _) = mock_service("upstream_relay", (), |&mut (), _| {});
    let (test_store, _) = mock_service("test_store", (), |&mut (), _| {});

    #[cfg(feature = "processing")]
    let redis_clients = config
        .redis()
        .map(|c| create_redis_clients(c))
        .transpose()
        .unwrap();

    #[cfg(feature = "processing")]
    let global_rate_limits = redis_clients
        .as_ref()
        .map(|p| GlobalRateLimitsService::new(p.quotas.clone()).start_detached());

    let metric_outcomes = MetricOutcomes::new(MetricStats::test().0, outcome_aggregator.clone());

    let config = Arc::new(config);
    EnvelopeProcessorService::new(
        create_processor_pool(),
        Arc::clone(&config),
        GlobalConfigHandle::fixed(Default::default()),
        ProjectCacheHandle::for_test(),
        Cogs::noop(),
        #[cfg(feature = "processing")]
        redis_clients,
        processor::Addrs {
            outcome_aggregator,
            upstream_relay,
            test_store,
            #[cfg(feature = "processing")]
            store_forwarder: None,
            aggregator,
            #[cfg(feature = "processing")]
            global_rate_limits,
        },
        metric_outcomes,
    )
}

pub async fn create_test_processor_with_addrs(
    config: Config,
    addrs: processor::Addrs,
) -> EnvelopeProcessorService {
    #[cfg(feature = "processing")]
    let redis_clients = config
        .redis()
        .map(|c| create_redis_clients(c))
        .transpose()
        .unwrap();
    let metric_outcomes =
        MetricOutcomes::new(MetricStats::test().0, addrs.outcome_aggregator.clone());

    let config = Arc::new(config);
    EnvelopeProcessorService::new(
        create_processor_pool(),
        Arc::clone(&config),
        GlobalConfigHandle::fixed(Default::default()),
        ProjectCacheHandle::for_test(),
        Cogs::noop(),
        #[cfg(feature = "processing")]
        redis_clients,
        addrs,
        metric_outcomes,
    )
}

pub fn processor_services() -> (Addr<TrackOutcome>, Addr<TestStore>) {
    let (outcome_aggregator, _) = mock_service("outcome_aggregator", (), |&mut (), _| {});
    let (test_store, _) = mock_service("test_store", (), |&mut (), _| {});
    (outcome_aggregator, test_store)
}

fn create_processor_pool() -> EnvelopeProcessorServicePool {
    ThreadPoolBuilder::new("processor", tokio::runtime::Handle::current())
        .num_threads(1)
        .build()
        .unwrap()
}
