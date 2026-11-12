//! Operations shared by the envelope and bucket processors.

use std::borrow::Cow;
use std::collections::BTreeSet;
use std::error::Error;
use std::future::Future;
use std::io::Write;
use std::pin::Pin;

use brotli::CompressorWriter as BrotliEncoder;
use bytes::Bytes;
use chrono::Utc;
use flate2::Compression;
use flate2::write::{GzEncoder, ZlibEncoder};
use relay_base_schema::project::ProjectKey;
use relay_config::{ConfigSnapshot, HttpEncoding, UpstreamDescriptor};
use relay_event_schema::protocol::ClientReport;
use relay_metrics::{Bucket, MetricNamespace};
use relay_quotas::{RateLimits, Scoping};
use relay_system::Addr;
use zstd::stream::Encoder as ZstdEncoder;

use crate::envelope::{self, ClientReportItem, ContentType, Envelope, EnvelopeError};
use crate::extractors::{PartialDsn, RequestMeta};
use crate::managed::ManagedEnvelope;
use crate::metrics::{MetricOutcomes, MetricsLimiter};
use crate::services::outcome::{DiscardReason, Outcome, TrackOutcome};
use crate::services::projects::cache::ProjectCacheHandle;
use crate::services::projects::project::ProjectInfo;
use crate::services::upstream::{
    SendRequest, Sign, SignatureType, UpstreamRelay, UpstreamRequest, UpstreamRequestError,
};
use crate::{http, utils};
#[cfg(feature = "processing")]
use {relay_quotas::RedisRateLimiter, relay_redis::RedisClients};

/// Creates a Redis rate limiter with the configured cache and maximum limit.
#[cfg(feature = "processing")]
pub fn create_rate_limiter(
    config: &ConfigSnapshot,
    redis: Option<RedisClients>,
) -> Option<RedisRateLimiter> {
    redis.map(|redis| {
        RedisRateLimiter::new(redis.quotas)
            .max_limit(config.max_rate_limit())
            .cache(config.quota_cache_ratio(), config.quota_cache_max())
    })
}

/// Filters metric buckets using project configuration and cached rate limits.
pub fn check_buckets(
    project_key: ProjectKey,
    project_info: &ProjectInfo,
    rate_limits: &RateLimits,
    buckets: Vec<Bucket>,
    metric_outcomes: &MetricOutcomes,
) -> Vec<Bucket> {
    let Some(scoping) = project_info.scoping(project_key) else {
        relay_log::error!(
            tags.project_key = project_key.as_str(),
            "there is no scoping: dropping {} buckets",
            buckets.len(),
        );
        return Vec::new();
    };

    let mut buckets = super::metrics::remove_invalid_namespaces(buckets, metric_outcomes, scoping);

    let mut namespaces: BTreeSet<MetricNamespace> = buckets
        .iter()
        .filter_map(|bucket| bucket.name.try_namespace())
        .collect();

    // Never rate limit outcomes.
    namespaces.remove(&MetricNamespace::Outcomes);

    for namespace in namespaces {
        let limits = rate_limits
            .check_with_quotas(project_info.get_quotas(), &scoping.metric_bucket(namespace));

        if limits.is_limited() {
            let rejected;
            (buckets, rejected) = utils::split_off(buckets, |bucket| {
                bucket.name.try_namespace() == Some(namespace)
            });

            let reason_code = limits.longest().and_then(|limit| limit.reason_code.clone());
            metric_outcomes.track(scoping, &rejected, Outcome::RateLimited(reason_code));
        }
    }

    let quotas = project_info.config.quotas.clone();
    match MetricsLimiter::create(buckets, quotas, scoping) {
        Ok(mut bucket_limiter) => {
            bucket_limiter.enforce_limits(rate_limits, metric_outcomes);
            bucket_limiter.into_buckets()
        }
        Err(buckets) => buckets,
    }
}

/// Sends a client report to the upstream.
#[derive(Debug)]
pub struct SubmitClientReports {
    /// The client report to be sent.
    pub client_reports: Vec<ClientReport>,
    /// Scoping information for the client report.
    pub scoping: Scoping,
}

/// Serializes client reports and sends them to the upstream.
pub fn submit_client_reports(
    message: SubmitClientReports,
    config: &ConfigSnapshot,
    outcome_aggregator: &Addr<TrackOutcome>,
    upstream_relay: &Addr<UpstreamRelay>,
    project_cache: &ProjectCacheHandle,
) {
    let SubmitClientReports {
        client_reports,
        scoping,
    } = message;

    relay_log::trace!(
        "sending {} client report(s) to project id {}",
        client_reports.len(),
        scoping.project_id
    );

    if client_reports.is_empty() {
        return;
    }

    let upstream = config.upstream();
    let dsn = PartialDsn::outbound(&scoping, upstream);

    let mut envelope = Envelope::from_request(None, RequestMeta::outbound(dsn));
    for client_report in client_reports {
        match client_report.serialize() {
            Ok(payload) => {
                let mut item = ClientReportItem::new();
                item.set_payload(ContentType::Json, payload);
                envelope.add_item(item);
            }
            Err(error) => {
                relay_log::error!(
                    error = &error as &dyn std::error::Error,
                    "failed to serialize client report"
                );
            }
        }
    }

    let envelope = ManagedEnvelope::new(envelope, outcome_aggregator.clone());
    submit_envelope_upstream(envelope, config, None, upstream_relay, project_cache);
}

/// Serializes and sends an envelope to its HTTP upstream.
pub fn submit_envelope_upstream(
    mut envelope: ManagedEnvelope,
    config: &ConfigSnapshot,
    // Currently allowed to be optional as code is migrated to respect the upstream override
    // provided from the project config. Eventually must be available and is required.
    upstream: Option<UpstreamDescriptor>,
    upstream_relay: &Addr<UpstreamRelay>,
    project_cache: &ProjectCacheHandle,
) {
    if envelope.envelope_mut().is_empty() {
        envelope.accept();
        return;
    }

    // No code path should hit this.
    //
    // Any item which is produced by processing is handled in `submit_upstream`,
    // metrics are sent to the store directly and outcomes must be produced to Kafka
    // instead of being sent onward as client report.
    if config.processing_enabled() {
        relay_log::error!(
            "attempt to forward envelope to http upstream when processing is enabled"
        );
        return;
    }

    // Override the `sent_at` timestamp. Since the envelope went through basic
    // normalization, all timestamps have been corrected. We propagate the new
    // `sent_at` to allow the next Relay to double-check this timestamp and
    // potentially apply correction again. This is done as close to sending as
    // possible so that we avoid internal delays.
    envelope.envelope_mut().set_sent_at(Utc::now());

    relay_log::trace!("sending envelope to sentry endpoint");
    let http_encoding = config.http_encoding();
    let result = envelope.envelope().to_vec().and_then(|v| {
        encode_payload(&v.into(), http_encoding).map_err(EnvelopeError::PayloadIoFailed)
    });

    match result {
        Ok(body) => {
            upstream_relay.send(SendRequest(SendEnvelope {
                upstream,
                envelope,
                body,
                http_encoding,
                project_cache: project_cache.clone(),
            }));
        }
        Err(error) => {
            // Errors are only logged for what we consider an internal discard reason. These
            // indicate errors in the infrastructure or implementation bugs.
            relay_log::error!(
                error = &error as &dyn Error,
                tags.project_key = %envelope.scoping().project_key,
                "failed to serialize envelope payload"
            );

            envelope.reject(Outcome::Invalid(DiscardReason::Internal));
        }
    }
}

pub fn encode_payload(body: &Bytes, http_encoding: HttpEncoding) -> Result<Bytes, std::io::Error> {
    let envelope_body: Vec<u8> = match http_encoding {
        HttpEncoding::Identity => return Ok(body.clone()),
        HttpEncoding::Deflate => {
            let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
            encoder.write_all(body.as_ref())?;
            encoder.finish()?
        }
        HttpEncoding::Gzip => {
            let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
            encoder.write_all(body.as_ref())?;
            encoder.finish()?
        }
        HttpEncoding::Br => {
            // Use default buffer size (via 0), medium quality (5), and the default lgwin (22).
            let mut encoder = BrotliEncoder::new(Vec::new(), 0, 5, 22);
            encoder.write_all(body.as_ref())?;
            encoder.into_inner()
        }
        HttpEncoding::Zstd => {
            // Use the fastest compression level, our main objective here is to get the best
            // compression ratio for least amount of time spent.
            let mut encoder = ZstdEncoder::new(Vec::new(), 1)?;
            encoder.write_all(body.as_ref())?;
            encoder.finish()?
        }
    };

    Ok(envelope_body.into())
}

/// An upstream request that submits an envelope via HTTP.
#[derive(Debug)]
pub struct SendEnvelope {
    pub upstream: Option<UpstreamDescriptor>,
    pub envelope: ManagedEnvelope,
    pub body: Bytes,
    pub http_encoding: HttpEncoding,
    pub project_cache: ProjectCacheHandle,
}

impl UpstreamRequest for SendEnvelope {
    fn upstream(&self) -> Option<&UpstreamDescriptor> {
        self.upstream.as_ref()
    }

    fn method(&self) -> reqwest::Method {
        reqwest::Method::POST
    }

    fn path(&self) -> Cow<'_, str> {
        format!("/api/{}/envelope/", self.envelope.scoping().project_id).into()
    }

    fn route(&self) -> &'static str {
        "envelope"
    }

    fn build(&mut self, builder: &mut http::RequestBuilder) -> Result<(), http::HttpError> {
        let envelope_body = self.body.clone();

        let meta = &self.envelope.meta();
        let shard = self.envelope.partition_key().map(|p| p.to_string());
        builder
            .content_encoding(self.http_encoding)
            .header_opt("Origin", meta.origin().map(|url| url.as_str()))
            .header_opt("User-Agent", meta.user_agent())
            .header("X-Sentry-Auth", meta.auth_header())
            .header("X-Forwarded-For", meta.forwarded_for())
            .header("Content-Type", envelope::CONTENT_TYPE)
            .header_opt("X-Sentry-Relay-Shard", shard)
            .body(envelope_body);

        Ok(())
    }

    fn sign(&mut self) -> Option<Sign> {
        Some(Sign::Optional(SignatureType::RequestSign))
    }

    fn respond(
        self: Box<Self>,
        result: Result<http::Response, UpstreamRequestError>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + Sync>> {
        Box::pin(async move {
            let result = match result {
                Ok(mut response) => response.consume().await.map_err(UpstreamRequestError::Http),
                Err(error) => Err(error),
            };

            match result {
                Ok(()) => self.envelope.accept(),
                Err(error) if error.is_received() => {
                    let scoping = self.envelope.scoping();
                    self.envelope.accept();

                    if let UpstreamRequestError::RateLimited(limits) = error {
                        self.project_cache
                            .get(scoping.project_key)
                            .rate_limits()
                            .merge(limits.scope(&scoping));
                    }
                }
                Err(error) => {
                    // Errors are only logged for what we consider an internal discard reason. These
                    // indicate errors in the infrastructure or implementation bugs.
                    let mut envelope = self.envelope;
                    envelope.reject(Outcome::Invalid(DiscardReason::Internal));
                    relay_log::error!(
                        error = &error as &dyn Error,
                        tags.project_key = %envelope.scoping().project_key,
                        "error sending envelope"
                    );
                }
            }
        })
    }
}
