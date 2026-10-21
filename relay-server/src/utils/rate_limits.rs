use std::fmt::Write;

use relay_quotas::{
    DataCategory, DimensionMap, QuotaScope, RateLimit, RateLimitScope, RateLimits, ReasonCode,
    Scoping,
};

use crate::envelope::{AttachmentType, Envelope, Item, ItemType};

/// Name of the rate limits header.
pub const RATE_LIMITS_HEADER: &str = "X-Sentry-Rate-Limits";

/// Formats the `X-Sentry-Rate-Limits` header.
pub fn format_rate_limits(rate_limits: &RateLimits) -> String {
    let mut header = String::new();

    for rate_limit in rate_limits {
        if !header.is_empty() {
            header.push_str(", ");
        }

        write!(header, "{}:", rate_limit.retry_after.remaining_seconds()).ok();

        for (index, category) in rate_limit.categories.iter().enumerate() {
            if index > 0 {
                header.push(';');
            }
            write!(header, "{category}").ok();
        }

        write!(header, ":{}", rate_limit.scope.name()).ok();

        if let Some(ref reason_code) = rate_limit.reason_code {
            write!(header, ":{reason_code}").ok();
        } else if !rate_limit.namespaces.is_empty() {
            write!(header, ":").ok(); // delimits the empty reason code for namespaces
        }

        for (index, namespace) in rate_limit.namespaces.iter().enumerate() {
            header.push(if index == 0 { ':' } else { ';' });
            write!(header, "{namespace}").ok();
        }
    }

    header
}

/// Parses the `X-Sentry-Rate-Limits` header.
pub fn parse_rate_limits(scoping: &Scoping, string: &str) -> RateLimits {
    let mut rate_limits = RateLimits::new();

    for limit in string.split(',') {
        let limit = limit.trim();
        if limit.is_empty() {
            continue;
        }

        let mut components = limit.split(':');

        let retry_after = match components.next().and_then(|s| s.parse().ok()) {
            Some(retry_after) => retry_after,
            None => continue,
        };

        let categories = components
            .next()
            .unwrap_or("")
            .split(';')
            .filter(|category| !category.is_empty())
            .map(DataCategory::from_name)
            .collect();

        let quota_scope = QuotaScope::from_name(components.next().unwrap_or(""));
        let scope = RateLimitScope::for_quota(scoping, quota_scope);

        let reason_code = components
            .next()
            .filter(|s| !s.is_empty())
            .map(ReasonCode::new);

        let namespace = components
            .next()
            .unwrap_or("")
            .split(';')
            .filter(|s| !s.is_empty())
            .filter_map(|s| s.parse().ok())
            .collect();

        rate_limits.add(RateLimit {
            categories,
            scope,
            reason_code,
            retry_after,
            namespaces: namespace,
            // The wire format cannot express dimensions, so a limit parsed from it is always
            // a plain limit for the whole scope.
            dimensions: DimensionMap::default(),
        });
    }

    rate_limits
}

/// Infer the data category from an item.
///
/// Categories depend mostly on the item type, with a few special cases:
/// - `Event`: the category is inferred from the event type. This requires the `event_type` header
///   to be set on the event item.
/// - `Attachment`: If the attachment creates an event (e.g. for minidumps), the category is assumed
///   to be `Error`.
fn infer_event_category(item: &Item) -> Option<DataCategory> {
    match item.ty() {
        ItemType::Event => Some(DataCategory::Error),
        ItemType::Transaction => Some(DataCategory::Transaction),
        ItemType::Security | ItemType::RawSecurity => Some(DataCategory::Security),
        ItemType::UnrealReport => Some(DataCategory::Error),
        ItemType::UserReportV2 => Some(DataCategory::UserReportV2),
        ItemType::Attachment if item.creates_event() => Some(DataCategory::Error),
        ItemType::Attachment => None,
        ItemType::Session => None,
        ItemType::Sessions => None,
        ItemType::MetricBuckets => None,
        ItemType::FormData => None,
        ItemType::UserReport => None,
        ItemType::Profile => None,
        ItemType::ReplayEvent => None,
        ItemType::ReplayRecording => None,
        ItemType::ReplayVideo => None,
        ItemType::ClientReport => None,
        ItemType::CheckIn => None,
        ItemType::Log => None,
        ItemType::TraceMetric => None,
        ItemType::Span => None,
        ItemType::ProfileChunk => None,
        ItemType::Integration => None,
        ItemType::Unknown(_) => None,
    }
}

/// Collection of all transaction profile quantities.
#[derive(Clone, Copy, Debug, Default)]
pub struct ProfileQuantities {
    /// All transaction profiles in the backend category.
    pub backend: usize,
    /// All transaction profiles in the ui category.
    pub ui: usize,
    /// All transaction profiles, includes profiles in the backend and ui categories as well as
    /// profiles which are in neither category.
    pub total: usize,
}

/// A summary of `Envelope` contents.
///
/// Aggregates the shared event and individual item quantities for outcome accounting and telemetry.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default)]
pub struct EnvelopeSummary {
    /// The data category of the event in the envelope. `None` if there is no event.
    pub event_category: Option<DataCategory>,

    /// Total attachment storage across all parent types.
    pub attachment_bytes: usize,
    pub attachment_count: usize,

    /// The number of all session updates.
    pub session_quantity: usize,

    /// The number of profiles.
    pub profile_quantity: ProfileQuantities,

    /// The number of replays.
    pub replay_quantity: usize,

    /// The number of user reports (legacy item type for user feedback).
    pub user_report_quantity: usize,

    /// The number of monitor check-ins.
    pub monitor_quantity: usize,

    /// The number of log for the log product sent.
    pub log_item_quantity: usize,

    /// The number of log bytes for the log product sent, in bytes
    pub log_byte_quantity: usize,

    /// Secondary number of transactions.
    ///
    /// This is 0 for envelopes which contain a transaction,
    /// only secondary transaction quantity should be tracked here,
    /// these are for example transaction counts extracted from metrics.
    ///
    /// A "primary" transaction is contained within the envelope,
    /// marking the envelope data category a [`DataCategory::Transaction`].
    pub secondary_transaction_quantity: usize,

    /// See `secondary_transaction_quantity`.
    pub secondary_span_quantity: usize,

    /// The number of standalone spans.
    pub span_quantity: usize,

    /// The payload size of this envelope.
    pub payload_size: usize,

    /// The number of profile chunks in this envelope.
    pub profile_chunk_quantity: usize,
    /// The number of UI profile chunks in this envelope.
    pub profile_chunk_ui_quantity: usize,

    /// The number of trace metrics in this envelope.
    pub trace_metric_quantity: usize,

    /// The number of trace metric bytes in this envelope.
    pub trace_metric_byte_quantity: usize,
}

impl EnvelopeSummary {
    /// Creates an empty summary.
    pub fn empty() -> Self {
        Self::default()
    }

    /// Creates an envelope summary and aggregates the given envelope.
    pub fn compute(envelope: &Envelope) -> Self {
        Self::compute_items(envelope.items())
    }

    pub fn compute_items<'a>(items: impl IntoIterator<Item = &'a Item>) -> Self {
        let mut summary = Self::empty();

        for item in items {
            if item.creates_event() {
                summary.infer_category(item);
            }

            // If the item has been rate limited before, the quota has been consumed and outcomes
            // emitted. We can skip it here.
            if item.rate_limited() {
                continue;
            }

            if let Some(source_quantities) = item.source_quantities() {
                summary.secondary_transaction_quantity += source_quantities.transactions;
                summary.secondary_span_quantity += source_quantities.spans;
            }

            summary.payload_size += item.len();

            summary.add_quantities(item);

            // Special case since v1 and v2 share a data category.
            // Adding this in add_quantity would include v2 in the count.
            if item.ty() == &ItemType::UserReport {
                summary.user_report_quantity += 1;
            }
        }

        summary
    }

    fn add_quantities(&mut self, item: &Item) {
        // The Nintendo switch item is a special case which should've been modelled like the
        // `Unreal4Context` as potentially a separate item type which does not have its own data
        // category.
        //
        // Currently there is no outcome category for this item, as it will be dissolved into
        // multiple different items once processed.
        if item.attachment_type() == Some(AttachmentType::NintendoSwitchDyingMessage) {
            return;
        }

        for (category, quantity) in item.quantities() {
            let target_quantity = match category {
                DataCategory::Attachment => &mut self.attachment_bytes,
                DataCategory::AttachmentItem => &mut self.attachment_count,
                DataCategory::Session => &mut self.session_quantity,
                DataCategory::Profile => &mut self.profile_quantity.total,
                DataCategory::ProfileBackend => &mut self.profile_quantity.backend,
                DataCategory::ProfileUi => &mut self.profile_quantity.ui,
                DataCategory::Replay => &mut self.replay_quantity,
                DataCategory::DoNotUseReplayVideo => &mut self.replay_quantity,
                DataCategory::Monitor => &mut self.monitor_quantity,
                DataCategory::Span => &mut self.span_quantity,
                DataCategory::TraceMetric => &mut self.trace_metric_quantity,
                DataCategory::TraceMetricByte => &mut self.trace_metric_byte_quantity,
                DataCategory::LogItem => &mut self.log_item_quantity,
                DataCategory::LogByte => &mut self.log_byte_quantity,
                DataCategory::ProfileChunk => &mut self.profile_chunk_quantity,
                DataCategory::ProfileChunkUi => &mut self.profile_chunk_ui_quantity,
                // TODO: This catch-all looks dangerous
                _ => continue,
            };
            *target_quantity += quantity;
        }
    }

    /// Infers the appropriate [`DataCategory`] for the envelope [`Item`].
    ///
    /// The inferred category is only applied to the [`EnvelopeSummary`] if there is not yet
    /// a category set.
    fn infer_category(&mut self, item: &Item) {
        if matches!(self.event_category, None | Some(DataCategory::Default))
            && let Some(category) = infer_event_category(item)
        {
            self.event_category = Some(category);
        }
    }
}

#[cfg(test)]
mod tests {

    use relay_base_schema::organization::OrganizationId;
    use relay_base_schema::project::{ProjectId, ProjectKey};
    use relay_metrics::MetricNamespace;
    use relay_quotas::{DimensionMap, RetryAfter};
    use smallvec::smallvec;

    use super::*;
    use crate::envelope::SourceQuantities;
    use crate::extractors::RequestMeta;

    #[test]
    fn test_format_rate_limits() {
        let mut rate_limits = RateLimits::new();

        // Add a generic rate limit for all categories.
        rate_limits.add(RateLimit {
            categories: Default::default(),
            scope: RateLimitScope::Organization(OrganizationId::new(42)),
            reason_code: Some(ReasonCode::new("my_limit")),
            retry_after: RetryAfter::from_secs(42),
            namespaces: smallvec![],
            dimensions: DimensionMap::default(),
        });

        // Add a more specific rate limit for just one category.
        rate_limits.add(RateLimit {
            categories: [DataCategory::Transaction, DataCategory::Security].into(),
            scope: RateLimitScope::Project(ProjectId::new(21)),
            reason_code: None,
            retry_after: RetryAfter::from_secs(4711),
            namespaces: smallvec![],
            dimensions: DimensionMap::default(),
        });

        let formatted = format_rate_limits(&rate_limits);
        let expected = "42::organization:my_limit, 4711:transaction;security:project";
        assert_eq!(formatted, expected);
    }

    #[test]
    fn test_format_rate_limits_namespace() {
        let mut rate_limits = RateLimits::new();

        // Rate limit with reason code and namespace.
        rate_limits.add(RateLimit {
            categories: [DataCategory::MetricBucket].into(),
            scope: RateLimitScope::Organization(OrganizationId::new(42)),
            reason_code: Some(ReasonCode::new("my_limit")),
            retry_after: RetryAfter::from_secs(42),
            namespaces: smallvec![MetricNamespace::Transactions, MetricNamespace::Spans],
            dimensions: DimensionMap::default(),
        });

        // Rate limit without reason code.
        rate_limits.add(RateLimit {
            categories: [DataCategory::MetricBucket].into(),
            scope: RateLimitScope::Organization(OrganizationId::new(42)),
            reason_code: None,
            retry_after: RetryAfter::from_secs(42),
            namespaces: smallvec![MetricNamespace::Spans],
            dimensions: DimensionMap::default(),
        });

        let formatted = format_rate_limits(&rate_limits);
        let expected = "42:metric_bucket:organization:my_limit:transactions;spans, 42:metric_bucket:organization::spans";
        assert_eq!(formatted, expected);
    }

    #[test]
    fn test_parse_invalid_rate_limits() {
        let scoping = Scoping {
            organization_id: OrganizationId::new(42),
            project_id: ProjectId::new(21),
            project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            key_id: Some(17),
        };

        assert!(parse_rate_limits(&scoping, "").is_ok());
        assert!(parse_rate_limits(&scoping, "invalid").is_ok());
        assert!(parse_rate_limits(&scoping, ",,,").is_ok());
    }

    #[test]
    fn test_parse_rate_limits() {
        let scoping = Scoping {
            organization_id: OrganizationId::new(42),
            project_id: ProjectId::new(21),
            project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            key_id: Some(17),
        };

        // contains "foobar", an unknown scope that should be mapped to Unknown
        let formatted =
            "42::organization:my_limit, invalid, 4711:foobar;transaction;security:project";
        let rate_limits: Vec<RateLimit> =
            parse_rate_limits(&scoping, formatted).into_iter().collect();

        assert_eq!(
            rate_limits,
            vec![
                RateLimit {
                    categories: Default::default(),
                    scope: RateLimitScope::Organization(OrganizationId::new(42)),
                    reason_code: Some(ReasonCode::new("my_limit")),
                    retry_after: rate_limits[0].retry_after,
                    namespaces: smallvec![],
                    dimensions: DimensionMap::default(),
                },
                RateLimit {
                    categories: [
                        DataCategory::Unknown,
                        DataCategory::Transaction,
                        DataCategory::Security,
                    ]
                    .into(),
                    scope: RateLimitScope::Project(ProjectId::new(21)),
                    reason_code: None,
                    retry_after: rate_limits[1].retry_after,
                    namespaces: smallvec![],
                    dimensions: DimensionMap::default(),
                }
            ]
        );

        assert_eq!(42, rate_limits[0].retry_after.remaining_seconds());
        assert_eq!(4711, rate_limits[1].retry_after.remaining_seconds());
    }

    #[test]
    fn test_parse_rate_limits_namespace() {
        let scoping = Scoping {
            organization_id: OrganizationId::new(42),
            project_id: ProjectId::new(21),
            project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            key_id: Some(17),
        };

        let formatted = "42:metric_bucket:organization::transactions;spans";
        let rate_limits: Vec<RateLimit> =
            parse_rate_limits(&scoping, formatted).into_iter().collect();

        assert_eq!(
            rate_limits,
            vec![RateLimit {
                categories: [DataCategory::MetricBucket].into(),
                scope: RateLimitScope::Organization(OrganizationId::new(42)),
                reason_code: None,
                retry_after: rate_limits[0].retry_after,
                namespaces: smallvec![MetricNamespace::Transactions, MetricNamespace::Spans],
                dimensions: DimensionMap::default(),
            }]
        );
    }

    #[test]
    fn test_parse_rate_limits_empty_namespace() {
        let scoping = Scoping {
            organization_id: OrganizationId::new(42),
            project_id: ProjectId::new(21),
            project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            key_id: Some(17),
        };

        // notice the trailing colon
        let formatted = "42:metric_bucket:organization:some_reason:";
        let rate_limits: Vec<RateLimit> =
            parse_rate_limits(&scoping, formatted).into_iter().collect();

        assert_eq!(
            rate_limits,
            vec![RateLimit {
                categories: [DataCategory::MetricBucket].into(),
                scope: RateLimitScope::Organization(OrganizationId::new(42)),
                reason_code: Some(ReasonCode::new("some_reason")),
                retry_after: rate_limits[0].retry_after,
                namespaces: smallvec![],
                dimensions: DimensionMap::default(),
            }]
        );
    }

    #[test]
    fn test_parse_rate_limits_only_unknown() {
        let scoping = Scoping {
            organization_id: OrganizationId::new(42),
            project_id: ProjectId::new(21),
            project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            key_id: Some(17),
        };

        let formatted = "42:foo;bar:organization";
        let rate_limits: Vec<RateLimit> =
            parse_rate_limits(&scoping, formatted).into_iter().collect();

        assert_eq!(
            rate_limits,
            vec![RateLimit {
                categories: [DataCategory::Unknown, DataCategory::Unknown].into(),
                scope: RateLimitScope::Organization(OrganizationId::new(42)),
                reason_code: None,
                retry_after: rate_limits[0].retry_after,
                namespaces: smallvec![],
                dimensions: DimensionMap::default(),
            },]
        );
    }
    #[test]
    fn test_source_quantity_for_total_quantity() {
        let dsn = "https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42"
            .parse()
            .unwrap();
        let request_meta = RequestMeta::new(dsn);

        let mut envelope = Envelope::from_request(None, request_meta);

        let mut item = Item::new(ItemType::MetricBuckets);
        item.set_source_quantities(SourceQuantities {
            transactions: 5,
            spans: 0,
            buckets: 5,
        });
        envelope.add_item(item);

        let mut item = Item::new(ItemType::MetricBuckets);
        item.set_source_quantities(SourceQuantities {
            transactions: 2,
            spans: 0,
            buckets: 3,
        });
        envelope.add_item(item);

        let summary = EnvelopeSummary::compute(&envelope);

        assert_eq!(summary.secondary_transaction_quantity, 7);
    }
}
