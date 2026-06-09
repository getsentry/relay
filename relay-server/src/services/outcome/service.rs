//! This module contains the service that tracks outcomes.
//!
//! Outcomes describe the final "fate" of an envelope item. As such, for every item exactly one
//! outcome must be emitted in the entire ingestion pipeline. Since Relay is only one part in this
//! pipeline, outcomes may not be emitted if the item is accepted.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::error::Error;
use std::mem;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

#[cfg(feature = "processing")]
use crate::service::ServiceError;
use crate::services::outcome::{DiscardReason, Outcome, OutcomeId, TrackOutcomeLike};
use crate::services::processor::{EnvelopeProcessor, SubmitClientReports};
use crate::services::upstream::{Method, SendQuery, UpstreamQuery, UpstreamRelay};
use crate::statsd::RelayCounters;
use crate::utils::SleepHandle;
use chrono::{DateTime, SecondsFormat, Utc};
use relay_base_schema::organization::OrganizationId;
use relay_base_schema::project::ProjectId;
use relay_common::time::UnixTimestamp;
use relay_config::{Config, EmitOutcomes};
use relay_event_schema::protocol::{ClientReport, DiscardedEvent, EventId};
#[cfg(feature = "processing")]
use relay_kafka::{ClientError, KafkaClient, KafkaTopic, SerializationOutput};
use relay_quotas::{DataCategory, Scoping};
use relay_statsd::metric;
use relay_system::{Addr, FromMessage, Interface, NoResponse, Service};
use serde::{Deserialize, Serialize};

/// Defines the structure of the HTTP outcomes requests
#[derive(Debug, Default, Deserialize, Serialize)]
pub struct SendOutcomes {
    #[serde(default)]
    pub outcomes: Vec<TrackRawOutcome>,
}

impl UpstreamQuery for SendOutcomes {
    type Response = SendOutcomesResponse;

    fn method(&self) -> Method {
        Method::POST
    }

    fn path(&self) -> Cow<'static, str> {
        Cow::Borrowed("/api/0/relays/outcomes/")
    }

    fn retry() -> bool {
        true
    }

    fn route(&self) -> &'static str {
        "outcomes"
    }
}

/// Defines the structure of the HTTP outcomes responses for successful requests
#[derive(Debug, Deserialize, Serialize)]
pub struct SendOutcomesResponse {
    // nothing yet, future features will go here
}

/// Tracks an [`Outcome`] of an Envelope item.
///
/// See the module level documentation for more information.
#[derive(Clone, Debug, Hash)]
pub struct TrackOutcome {
    /// The timespan of the event outcome.
    pub timestamp: DateTime<Utc>,
    /// Scoping of the request.
    pub scoping: Scoping,
    /// The outcome.
    pub outcome: Outcome,
    /// The event id.
    pub event_id: Option<EventId>,
    /// The client ip address.
    pub remote_addr: Option<IpAddr>,
    /// The event's data category.
    pub category: DataCategory,
    /// The number of events or total attachment size in bytes.
    pub quantity: u32,
}

impl TrackOutcomeLike for TrackOutcome {
    fn reason(&self) -> Option<Cow<'_, str>> {
        self.outcome.to_reason()
    }

    fn outcome_id(&self) -> OutcomeId {
        self.outcome.to_outcome_id()
    }

    fn quantity(&self) -> Option<u32> {
        Some(self.quantity)
    }

    fn category(&self) -> DataCategory {
        self.category
    }
}

impl Interface for TrackOutcome {}

impl FromMessage<Self> for TrackOutcome {
    type Response = NoResponse;

    fn from_message(message: Self, _: ()) -> Self {
        message
    }
}

/// Raw representation of an outcome for serialization.
///
/// The JSON serialization of this structure is placed on the Kafka topic and used in the HTTP
/// endpoints. To create a new outcome, use [`TrackOutcome`], instead.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TrackRawOutcome {
    /// The timespan of the event outcome.
    timestamp: String,
    /// Organization id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    org_id: Option<OrganizationId>,
    /// Project id.
    project_id: ProjectId,
    /// The DSN project key id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    key_id: Option<u64>,
    /// The outcome.
    outcome: OutcomeId,
    /// Reason for the outcome.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
    /// The event id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    event_id: Option<EventId>,
    /// The client ip address.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    remote_addr: Option<String>,
    /// The source of the outcome (which Relay sent it)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    source: Option<String>,
    /// The event's data category.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub category: Option<u8>,
    /// The number of events or total attachment size in bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quantity: Option<u32>,
}

impl TrackRawOutcome {
    fn from_outcome(msg: TrackOutcome, config: &Config) -> Self {
        let reason = msg.outcome.to_reason().map(|reason| reason.to_string());

        // convert to a RFC 3339 formatted date with the shape YYYY-MM-DDTHH:MM:SS.mmmmmmZ
        // e.g. something like: "2019-09-29T09:46:40.123456Z"
        let timestamp = msg.timestamp.to_rfc3339_opts(SecondsFormat::Micros, true);

        let org_id = match msg.scoping.organization_id.value() {
            0 => None,
            id => Some(OrganizationId::new(id)),
        };

        // since TrackOutcome objects come only from this Relay (and not any downstream
        // Relays), set the source to whatever our current outcome source is.
        let source = config.outcome_source().map(str::to_owned);

        TrackRawOutcome {
            timestamp,
            org_id,
            project_id: msg.scoping.project_id,
            key_id: msg.scoping.key_id,
            outcome: msg.outcome.to_outcome_id(),
            reason,
            event_id: msg.event_id,
            remote_addr: msg.remote_addr.map(|addr| addr.to_string()),
            source,
            category: msg.category.value(),
            quantity: Some(msg.quantity),
        }
    }

    #[cfg(feature = "processing")]
    fn is_billing(&self) -> bool {
        matches!(self.outcome, OutcomeId::ACCEPTED | OutcomeId::RATE_LIMITED)
    }
}

impl TrackOutcomeLike for TrackRawOutcome {
    fn reason(&self) -> Option<Cow<'_, str>> {
        self.reason.as_ref().map(|s| s.into())
    }

    fn outcome_id(&self) -> OutcomeId {
        self.outcome
    }

    fn quantity(&self) -> Option<u32> {
        self.quantity
    }

    fn category(&self) -> DataCategory {
        match self.category {
            Some(cat) => DataCategory::try_from(cat).unwrap_or(DataCategory::Unknown),
            None => DataCategory::Unknown,
        }
    }
}

impl Interface for TrackRawOutcome {}

impl FromMessage<Self> for TrackRawOutcome {
    type Response = NoResponse;

    fn from_message(message: Self, _: ()) -> Self {
        message
    }
}

/// Outcome producer backend via HTTP as [`TrackRawOutcome`].
#[derive(Debug)]
struct HttpOutcomeProducer {
    config: Arc<Config>,
    upstream_relay: Addr<UpstreamRelay>,
    unsent_outcomes: Vec<TrackRawOutcome>,
    flush_handle: SleepHandle,
}

impl HttpOutcomeProducer {
    pub fn new(config: Arc<Config>, upstream_relay: Addr<UpstreamRelay>) -> Self {
        Self {
            config,
            upstream_relay,
            unsent_outcomes: Vec::new(),
            flush_handle: SleepHandle::idle(),
        }
    }

    fn send_batch(&mut self) {
        self.flush_handle.reset();

        if self.unsent_outcomes.is_empty() {
            relay_log::warn!("unexpected send_batch scheduled with no outcomes to send");
            return;
        } else {
            relay_log::trace!(
                "sending outcome batch of size {}",
                self.unsent_outcomes.len()
            );
        }

        let request = SendOutcomes {
            outcomes: mem::take(&mut self.unsent_outcomes),
        };

        let upstream_relay = self.upstream_relay.clone();

        relay_system::spawn!(async move {
            match upstream_relay.send(SendQuery(request)).await {
                Ok(_) => relay_log::trace!("outcome batch sent"),
                Err(error) => {
                    relay_log::error!(error = &error as &dyn Error, "outcome batch sending failed")
                }
            }
        });
    }

    fn handle_message(&mut self, message: TrackRawOutcome) {
        relay_log::trace!("batching outcome");
        self.unsent_outcomes.push(message);

        if self.unsent_outcomes.len() >= self.config.outcome_batch_size() {
            self.send_batch();
        } else if self.flush_handle.is_idle() {
            self.flush_handle.set(self.config.outcome_batch_interval());
        }
    }
}

impl Service for HttpOutcomeProducer {
    type Interface = TrackRawOutcome;

    async fn run(mut self, mut rx: relay_system::Receiver<Self::Interface>) {
        loop {
            tokio::select! {
                // Prioritize flush over receiving messages to prevent starving.
                biased;

                () = &mut self.flush_handle => self.send_batch(),
                Some(message) = rx.recv() => self.handle_message(message),
                else => break,
            }
        }
    }
}

/// Outcome producer backend via HTTP as [`ClientReport`].
#[derive(Debug)]
struct ClientReportOutcomeProducer {
    flush_interval: Duration,
    unsent_reports: BTreeMap<Scoping, Vec<ClientReport>>,
    flush_handle: SleepHandle,
    envelope_processor: Addr<EnvelopeProcessor>,
}

impl ClientReportOutcomeProducer {
    fn new(config: &Config, envelope_processor: Addr<EnvelopeProcessor>) -> Self {
        Self {
            // Use same batch interval as outcome aggregator
            flush_interval: Duration::from_secs(config.outcome_aggregator().flush_interval),
            unsent_reports: BTreeMap::new(),
            flush_handle: SleepHandle::idle(),
            envelope_processor,
        }
    }

    fn flush(&mut self) {
        relay_log::trace!("flushing client reports");
        self.flush_handle.reset();

        let unsent_reports = mem::take(&mut self.unsent_reports);
        for (scoping, client_reports) in unsent_reports.into_iter() {
            self.envelope_processor.send(SubmitClientReports {
                client_reports,
                scoping,
            });
        }
    }

    fn handle_message(&mut self, msg: TrackOutcome) {
        let mut client_report = ClientReport {
            timestamp: Some(UnixTimestamp::from_secs(
                msg.timestamp.timestamp().try_into().unwrap_or(0),
            )),
            ..Default::default()
        };

        // The outcome type determines what field to place the outcome in:
        let discarded_events = match msg.outcome {
            Outcome::Filtered(_) => &mut client_report.filtered_events,
            Outcome::FilteredSampling(_) => &mut client_report.filtered_sampling_events,
            Outcome::RateLimited(_) => &mut client_report.rate_limited_events,
            Outcome::Invalid(DiscardReason::InvalidSignature | DiscardReason::MissingSignature) => {
                &mut client_report.discarded_events
            }
            _ => {
                relay_log::debug!(
                    "Outcome '{}' cannot be converted to client report",
                    msg.outcome
                );
                return;
            }
        };

        // Now that we know where to put it, let's create a DiscardedEvent
        let discarded_event = DiscardedEvent {
            reason: msg.outcome.to_reason().unwrap_or_default().to_string(),
            category: msg.category,
            quantity: msg.quantity,
        };
        discarded_events.push(discarded_event);

        self.unsent_reports
            .entry(msg.scoping)
            .or_default()
            .push(client_report);

        if self.flush_interval == Duration::ZERO {
            // Flush immediately. Useful for integration tests.
            self.flush();
        } else if self.flush_handle.is_idle() {
            self.flush_handle.set(self.flush_interval);
        }
    }
}

impl Service for ClientReportOutcomeProducer {
    type Interface = TrackOutcome;

    async fn run(mut self, mut rx: relay_system::Receiver<Self::Interface>) {
        loop {
            tokio::select! {
                // Prioritize flush over receiving messages to prevent starving.
                biased;

                () = &mut self.flush_handle => self.flush(),
                Some(message) = rx.recv() => self.handle_message(message),
                else => break,
            }
        }
    }
}

/// Outcomes producer backend for Kafka.
///
/// Internally, this type creates at least one Kafka producer for the cluster of the `outcomes`
/// topic assignment. If the `outcomes-billing` topic specifies a different cluster, it creates a
/// second producer.
///
/// Use `KafkaOutcomesProducer::billing` for outcomes that are critical to billing (see
/// `is_billing`), otherwise use `KafkaOutcomesProducer::default`. This will return the correct
/// producer instance internally.
#[cfg(feature = "processing")]
#[derive(Debug)]
struct KafkaOutcomesProducer {
    client: KafkaClient,
}

#[cfg(feature = "processing")]
impl KafkaOutcomesProducer {
    /// Creates and connects the Kafka producers.
    ///
    /// If the given Kafka configuration parameters are invalid, or an error
    /// happens while connecting to the broker, an error is returned.
    pub fn create(config: &Config) -> anyhow::Result<Self> {
        let mut client_builder = KafkaClient::builder();

        for topic in &[KafkaTopic::Outcomes, KafkaTopic::OutcomesBilling] {
            let kafka_config = config
                .kafka_configs(*topic)
                .map_err(|e| ServiceError::Kafka(e.to_string()))?;
            client_builder = client_builder
                .add_kafka_topic_config(*topic, &kafka_config, config.kafka_validate_topics())
                .map_err(|e| ServiceError::Kafka(e.to_string()))?;
        }

        Ok(Self {
            client: client_builder.build(),
        })
    }
}

/// Produces [`Outcome`]s to a configurable backend.
///
/// There are two variants based on the source of outcomes. When logging outcomes, [`TrackOutcome`]
/// should be heavily preferred. When processing outcomes from endpoints, [`TrackRawOutcome`] can be
/// used instead.
///
/// The backend is configured through the `outcomes` configuration object and can be:
///
///  1. Kafka in processing mode
///  2. Upstream Relay via batch HTTP request in point-of-presence configuration
///  3. Upstream Relay via client reports in external configuration
///  4. (default) Disabled
#[derive(Debug)]
pub enum OutcomeProducer {
    TrackOutcome(TrackOutcome),
    TrackRawOutcome(TrackRawOutcome),
}

impl Interface for OutcomeProducer {}

impl FromMessage<TrackOutcome> for OutcomeProducer {
    type Response = NoResponse;

    fn from_message(message: TrackOutcome, _: ()) -> Self {
        Self::TrackOutcome(message)
    }
}

impl FromMessage<TrackRawOutcome> for OutcomeProducer {
    type Response = NoResponse;

    fn from_message(message: TrackRawOutcome, _: ()) -> Self {
        Self::TrackRawOutcome(message)
    }
}

fn send_outcome_metric(message: &impl TrackOutcomeLike, to: &'static str) {
    if let Some(quantity) = message.quantity() {
        metric!(
            counter(RelayCounters::OutcomeQuantity) += quantity.into(),
            category = message.category().name(),
            outcome = message.tag_name(),
            to = to,
        );
    }
    metric!(
        counter(RelayCounters::Outcomes) += 1,
        reason = message.reason().as_deref().unwrap_or(""),
        outcome = message.tag_name(),
        to = to,
    );
}

#[derive(Debug)]
enum OutcomeBroker {
    ClientReport(Addr<TrackOutcome>),
    Http(Addr<TrackRawOutcome>),
    #[cfg(feature = "processing")]
    Kafka(KafkaOutcomesProducer),
    Disabled,
}

impl OutcomeBroker {
    fn handle_message(&self, message: OutcomeProducer, config: &Config) {
        relay_log::with_scope(
            |_| {},
            || match message {
                OutcomeProducer::TrackOutcome(msg) => self.handle_track_outcome(msg, config),
                OutcomeProducer::TrackRawOutcome(msg) => self.handle_track_raw_outcome(msg),
            },
        )
    }

    #[cfg(feature = "processing")]
    fn send_kafka_message(&self, producer: &KafkaOutcomesProducer, message: TrackRawOutcome) {
        relay_log::trace!("Tracking kafka outcome: {message:?}");

        // Dispatch to the correct topic and cluster based on the kind of outcome.
        let topic = if message.is_billing() {
            KafkaTopic::OutcomesBilling
        } else {
            KafkaTopic::Outcomes
        };

        if let Err(error) = producer.client.send_message(topic, &message) {
            relay_log::error!(error = &error as &dyn Error, "failed to produce outcome");
        }
    }

    fn handle_track_outcome(&self, message: TrackOutcome, config: &Config) {
        match self {
            #[cfg(feature = "processing")]
            Self::Kafka(kafka_producer) => {
                send_outcome_metric(&message, "kafka");
                let raw_message = TrackRawOutcome::from_outcome(message, config);
                self.send_kafka_message(kafka_producer, raw_message);
            }
            Self::ClientReport(producer) => {
                send_outcome_metric(&message, "client_report");
                producer.send(message);
            }
            Self::Http(producer) => {
                send_outcome_metric(&message, "http");
                producer.send(TrackRawOutcome::from_outcome(message, config));
            }
            Self::Disabled => (),
        }
    }

    fn handle_track_raw_outcome(&self, message: TrackRawOutcome) {
        match self {
            #[cfg(feature = "processing")]
            Self::Kafka(kafka_producer) => {
                send_outcome_metric(&message, "kafka");
                self.send_kafka_message(kafka_producer, message);
            }
            Self::Http(producer) => {
                send_outcome_metric(&message, "http");
                producer.send(message);
            }
            Self::ClientReport(_) => (),
            Self::Disabled => (),
        }
    }
}

#[cfg(feature = "processing")]
impl relay_kafka::Message for TrackRawOutcome {
    fn key(&self) -> Option<relay_kafka::Key> {
        // At the moment, we support outcomes with optional EventId.
        // Here we create a fake EventId, when we don't have the real one, so that we can
        // create a kafka message key that spreads the events nicely over all the
        // kafka consumer groups.
        let key = self.event_id.unwrap_or_default().0;
        Some(key.as_u128())
    }

    fn variant(&self) -> &'static str {
        "outcome"
    }

    fn headers(&self) -> Option<&BTreeMap<String, String>> {
        None
    }

    fn serialize(&self) -> Result<SerializationOutput<'_>, ClientError> {
        let serialized = serde_json::to_vec(self)?;
        Ok(SerializationOutput::Json(Cow::Owned(serialized)))
    }
}

#[derive(Debug)]
enum ProducerInner {
    #[cfg(feature = "processing")]
    Kafka(KafkaOutcomesProducer),
    Http(HttpOutcomeProducer),
    ClientReport(ClientReportOutcomeProducer),
    Disabled,
}

impl ProducerInner {
    fn start(self) -> OutcomeBroker {
        match self {
            #[cfg(feature = "processing")]
            ProducerInner::Kafka(inner) => OutcomeBroker::Kafka(inner),
            ProducerInner::Http(inner) => OutcomeBroker::Http(inner.start_detached()),
            ProducerInner::ClientReport(inner) => {
                OutcomeBroker::ClientReport(inner.start_detached())
            }
            ProducerInner::Disabled => OutcomeBroker::Disabled,
        }
    }
}

/// Service implementing the [`OutcomeProducer`] interface.
#[derive(Debug)]
pub struct OutcomeProducerService {
    config: Arc<Config>,
    inner: ProducerInner,
}

impl OutcomeProducerService {
    pub fn create(
        config: Arc<Config>,
        upstream_relay: Addr<UpstreamRelay>,
        envelope_processor: Addr<EnvelopeProcessor>,
    ) -> anyhow::Result<Self> {
        let inner = match config.emit_outcomes() {
            #[cfg(feature = "processing")]
            EmitOutcomes::AsOutcomes if config.processing_enabled() => {
                // We emit raw outcomes, and accept raw outcomes emitted by downstream Relays
                relay_log::info!("Configured to emit outcomes via kafka");
                ProducerInner::Kafka(KafkaOutcomesProducer::create(&config)?)
            }
            EmitOutcomes::AsOutcomes => {
                relay_log::info!("Configured to emit outcomes via http");
                ProducerInner::Http(HttpOutcomeProducer::new(
                    Arc::clone(&config),
                    upstream_relay,
                ))
            }
            EmitOutcomes::AsClientReports => {
                // We emit client reports, and we do NOT accept raw outcomes
                relay_log::info!("Configured to emit outcomes as client reports");
                ProducerInner::ClientReport(ClientReportOutcomeProducer::new(
                    &config,
                    envelope_processor,
                ))
            }
            EmitOutcomes::None => {
                relay_log::info!("Configured to drop all outcomes");
                ProducerInner::Disabled
            }
        };

        Ok(Self { config, inner })
    }
}

impl Service for OutcomeProducerService {
    type Interface = OutcomeProducer;

    async fn run(self, mut rx: relay_system::Receiver<Self::Interface>) {
        let Self { config, inner } = self;

        let broker = inner.start();

        relay_log::info!("OutcomeProducer started.");
        while let Some(message) = rx.recv().await {
            broker.handle_message(message, &config);
        }
        relay_log::info!("OutcomeProducer stopped.");
    }
}
