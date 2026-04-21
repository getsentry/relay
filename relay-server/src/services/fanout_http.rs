//! Fire-and-forget HTTP fanout tee for envelopes.
//!
//! When enabled via `fanout.http.*` configuration, every envelope that reaches
//! `submit_upstream` is also POSTed to a configurable HTTP endpoint in parallel with
//! the primary upstream forward. Used at Cursor to tee envelopes into a backend
//! endpoint that republishes them to Kafka, without re-implementing WarpStream
//! connectivity inside Relay.
//!
//! Strict guarantees:
//!
//! - The primary upstream forward is never blocked, delayed, or affected by tee state.
//! - The dispatch path uses [`tokio::sync::mpsc::Sender::try_send`]: on a full queue,
//!   envelopes are dropped and a counter is incremented; the call site never awaits.
//! - On request failure or timeout, a counter is incremented; no retries are attempted.
//! - When the `fanout-http` Cargo feature is off, this module is compiled out completely.

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use chrono::{DateTime, Utc};
use relay_config::{Config, HttpEncoding};
use relay_event_schema::protocol::EventId;
use relay_quotas::Scoping;
use relay_statsd::metric;
use smallvec::SmallVec;
use tokio::sync::{Semaphore, mpsc};

use crate::envelope::ItemType;
use crate::statsd::{RelayCounters, RelayDistributions, RelayTimers};

/// One envelope to dispatch to the HTTP fanout target.
#[derive(Debug)]
pub struct FanoutEnvelope {
    /// Already-encoded envelope bytes (matches what we send upstream).
    pub body: Bytes,
    /// Encoding applied to [`Self::body`]; mapped to the outgoing `Content-Encoding` header.
    pub content_encoding: HttpEncoding,
    /// Envelope scoping (project / org / key identifiers).
    pub scoping: Scoping,
    /// Top-level envelope event id, if any.
    pub event_id: Option<EventId>,
    /// Server-side receive time of the envelope.
    pub received_at: DateTime<Utc>,
    /// Item types observed in the envelope; included in headers and used for filtering.
    pub item_types: SmallVec<[ItemType; 4]>,
}

/// Cheap-to-clone handle held at the `submit_upstream` call sites.
///
/// Owns the bounded mpsc sender plus the small subset of config needed to make
/// the cheap drop decisions (sample rate, item-type allowlist, max body size)
/// before allocating the message.
#[derive(Clone, Debug)]
pub struct FanoutHttpHandle {
    tx: mpsc::Sender<FanoutEnvelope>,
    sample_rate: f32,
    include_item_types: Option<Arc<Vec<String>>>,
    max_body_bytes: usize,
}

impl FanoutHttpHandle {
    /// Cheap pre-flight decision about whether to dispatch this envelope.
    ///
    /// Returns `false` (and increments [`RelayCounters::FanoutHttpDropped`] with the matching
    /// reason tag) when:
    ///
    /// - the body exceeds `max_body_bytes`,
    /// - none of the envelope's item types are in the allowlist (when one is set),
    /// - the random sample rejects this envelope.
    pub fn should_send(&self, body_len: usize, item_types: &[ItemType]) -> bool {
        if body_len > self.max_body_bytes {
            metric!(counter(RelayCounters::FanoutHttpDropped) += 1, reason = "too_large");
            return false;
        }

        if let Some(allow) = self.include_item_types.as_ref()
            && !item_types
                .iter()
                .any(|ty| allow.iter().any(|a| a == ty.as_str()))
        {
            metric!(counter(RelayCounters::FanoutHttpDropped) += 1, reason = "item_type");
            return false;
        }

        if self.sample_rate < 1.0 {
            // For sample_rate <= 0.0 we always drop without consuming RNG.
            if self.sample_rate <= 0.0 || rand::random::<f32>() >= self.sample_rate {
                metric!(counter(RelayCounters::FanoutHttpDropped) += 1, reason = "sampled");
                return false;
            }
        }

        true
    }

    /// Dispatch an envelope. Drops on a full queue, never blocks.
    ///
    /// Should typically be called only after [`Self::should_send`] returns `true`, so that
    /// expensive cloning of the envelope bytes only happens for envelopes we mean to send.
    pub fn dispatch(&self, envelope: FanoutEnvelope) {
        // Sample current queue depth so dashboards can see how close we are to saturation.
        metric!(
            distribution(RelayDistributions::FanoutHttpQueueDepth) =
                self.tx.max_capacity().saturating_sub(self.tx.capacity()) as u64
        );

        match self.tx.try_send(envelope) {
            Ok(()) => {
                metric!(counter(RelayCounters::FanoutHttpSent) += 1);
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                metric!(counter(RelayCounters::FanoutHttpDropped) += 1, reason = "queue_full");
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                // Worker task has stopped. We can't usefully recover; treat as a drop.
                metric!(counter(RelayCounters::FanoutHttpDropped) += 1, reason = "closed");
            }
        }
    }
}

/// Spawned worker that drains the bounded queue and POSTs each envelope.
///
/// Returns `None` when the HTTP fanout is not enabled in `Config`. When it returns `Some`,
/// the worker task is already running and the returned handle is wired into the
/// `submit_upstream` call sites.
pub fn spawn(config: &Config) -> Option<FanoutHttpHandle> {
    let http = config.fanout_http()?;

    if http.url.is_empty() {
        relay_log::error!("fanout.http.enabled=true but fanout.http.url is empty; disabling");
        return None;
    }

    let queue_size = http.queue_size.max(1);
    let max_concurrent = http.max_concurrent.max(1);
    let timeout = Duration::from_millis(http.timeout_ms);

    let client = match reqwest::ClientBuilder::new()
        .connect_timeout(timeout)
        .timeout(timeout)
        .build()
    {
        Ok(c) => c,
        Err(err) => {
            relay_log::error!(
                error = &err as &dyn std::error::Error,
                "failed to build fanout http client; disabling"
            );
            return None;
        }
    };

    let (tx, mut rx) = mpsc::channel::<FanoutEnvelope>(queue_size);

    relay_log::info!(
        url = %http.url,
        sample_rate = http.sample_rate,
        max_concurrent = max_concurrent,
        queue_size = queue_size,
        timeout_ms = http.timeout_ms,
        "fanout http enabled",
    );

    let worker_cfg = Arc::new(WorkerConfig {
        url: http.url.clone(),
        header_auth: http.header_auth.clone(),
    });

    let semaphore = Arc::new(Semaphore::new(max_concurrent));
    let worker_client = client.clone();

    relay_system::spawn!(async move {
        while let Some(envelope) = rx.recv().await {
            // Backpressure: when all concurrency permits are taken, drop on the floor and
            // record the reason. We never block the receiving loop on outbound capacity,
            // so the bounded channel is the only thing holding pressure on producers.
            let permit = match Arc::clone(&semaphore).try_acquire_owned() {
                Ok(p) => p,
                Err(_) => {
                    metric!(
                        counter(RelayCounters::FanoutHttpDropped) += 1,
                        reason = "max_concurrent"
                    );
                    continue;
                }
            };

            let cfg = Arc::clone(&worker_cfg);
            let client = worker_client.clone();
            relay_system::spawn!(async move {
                let _permit = permit;
                send_one(client, cfg, envelope).await;
            });
        }
    });

    Some(FanoutHttpHandle {
        tx,
        sample_rate: http.sample_rate,
        include_item_types: http
            .include_item_types
            .as_ref()
            .map(|v| Arc::new(v.clone())),
        max_body_bytes: http.max_body_bytes,
    })
}

/// Per-request configuration shared between the worker loop and spawned send tasks.
struct WorkerConfig {
    url: String,
    header_auth: Option<String>,
}

async fn send_one(client: reqwest::Client, cfg: Arc<WorkerConfig>, envelope: FanoutEnvelope) {
    let mut req = client
        .post(&cfg.url)
        .header("content-type", "application/x-sentry-envelope");

    if let Some(name) = envelope.content_encoding.name() {
        req = req.header("content-encoding", name);
    }

    req = req
        .header(
            "x-sentry-project-id",
            envelope.scoping.project_id.to_string(),
        )
        .header(
            "x-sentry-organization-id",
            envelope.scoping.organization_id.to_string(),
        )
        .header(
            "x-sentry-project-key",
            envelope.scoping.project_key.to_string(),
        );

    if let Some(key_id) = envelope.scoping.key_id {
        req = req.header("x-sentry-key-id", key_id.to_string());
    }
    if let Some(event_id) = envelope.event_id {
        req = req.header("x-sentry-event-id", event_id.to_string());
    }
    req = req.header("x-sentry-received-at", envelope.received_at.to_rfc3339());

    if !envelope.item_types.is_empty() {
        let joined = envelope
            .item_types
            .iter()
            .map(|t| t.as_str())
            .collect::<Vec<_>>()
            .join(",");
        req = req.header("x-sentry-item-types", joined);
    }

    if let Some(token) = cfg.header_auth.as_deref() {
        req = req.header("authorization", format!("Bearer {token}"));
    }

    let body_len = envelope.body.len();
    req = req.body(envelope.body);

    let started = Instant::now();
    let result = req.send().await;
    let elapsed = started.elapsed();

    match result {
        Ok(resp) if resp.status().is_success() => {
            metric!(timer(RelayTimers::FanoutHttpRequest) = elapsed, outcome = "ok");
            relay_log::trace!(
                bytes = body_len,
                status = resp.status().as_u16(),
                "fanout http accepted",
            );
        }
        Ok(resp) => {
            let status = resp.status();
            let outcome = if status.is_client_error() {
                "status_4xx"
            } else {
                "status_5xx"
            };
            metric!(timer(RelayTimers::FanoutHttpRequest) = elapsed, outcome = outcome);
            metric!(counter(RelayCounters::FanoutHttpFailed) += 1, reason = outcome);
            relay_log::debug!(status = status.as_u16(), "fanout http rejected");
        }
        Err(err) => {
            let outcome = if err.is_timeout() {
                "timeout"
            } else {
                "transport"
            };
            metric!(timer(RelayTimers::FanoutHttpRequest) = elapsed, outcome = outcome);
            metric!(counter(RelayCounters::FanoutHttpFailed) += 1, reason = outcome);
            relay_log::debug!(
                error = &err as &dyn std::error::Error,
                "fanout http transport failure"
            );
        }
    }
}

