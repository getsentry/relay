use std::fmt::Write;

use relay_profiling::ProfileType;
use relay_quotas::{
    DataCategory, DimensionMap, QuotaScope, RateLimit, RateLimitScope, RateLimits, ReasonCode,
    Scoping,
};

use crate::envelope::{
    AttachmentParentType, AttachmentType, ContentType, Envelope, Item, ItemType,
};
use crate::integrations::Integration;

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

/// Whether an attachment creates an event or is a plain attachment.
///
/// Summary construction and early retention must use the same classification.
pub enum AttachmentRole {
    CreatesEvent,
    Plain,
}

impl AttachmentRole {
    /// Classifies attachment types exhaustively, including an absent or unknown subtype.
    pub fn from_type(attachment_type: Option<AttachmentType>) -> Self {
        match attachment_type {
            Some(
                AttachmentType::AppleCrashReport
                | AttachmentType::Minidump
                | AttachmentType::EventPayload
                | AttachmentType::Prosperodump
                | AttachmentType::Breadcrumbs
                | AttachmentType::NvGpuDump
                | AttachmentType::NintendoSwitchDyingMessage,
            ) => Self::CreatesEvent,
            Some(
                AttachmentType::Attachment
                | AttachmentType::UnrealContext
                | AttachmentType::UnrealLogs
                | AttachmentType::ViewHierarchy
                | AttachmentType::Flamegraph
                | AttachmentType::NvShaderDebug,
            )
            | None => Self::Plain,
        }
    }
}

/// Event-creating items in an envelope and their attachment parent types.
///
/// Plain attachments do not affect this group. The first creator that survives attachment limits
/// determines the event category, even if a removable crash attachment precedes it. Otherwise,
/// the order of crash attachments determines the rejection reason.
#[derive(Clone, Copy, Debug, Default)]
pub enum EventGroup {
    /// No item creates an event; associated items can still arrive separately.
    #[default]
    Absent,
    /// A direct event or required event-parent attachment survives attachment limits.
    /// The first such item determines the event category.
    SurvivesAttachmentLimits(DataCategory),
    /// Only span-parent crash attachments create the error event.
    SpanEventCreators,
    /// Only trace-parent crash attachments create the error event.
    TraceEventCreators,
    /// Both parent types create the event, with a span-parent creator appearing first.
    BothSpanFirst,
    /// Both parent types create the event, with a trace-parent creator appearing first.
    BothTraceFirst,
}

impl EventGroup {
    /// Returns the category of the event, counted once per envelope.
    pub fn category(self) -> Option<DataCategory> {
        match self {
            Self::Absent => None,
            Self::SurvivesAttachmentLimits(category) => Some(category),
            Self::SpanEventCreators
            | Self::TraceEventCreators
            | Self::BothSpanFirst
            | Self::BothTraceFirst => Some(DataCategory::Error),
        }
    }

    fn add_event_creator(&mut self, category: DataCategory, parent: AttachmentParentType) {
        *self = match (*self, parent) {
            (Self::SurvivesAttachmentLimits(_), _) => return,
            // A removable crash attachment must not determine the category of a surviving
            // direct event. Otherwise removing it changes the category during accounting.
            (_, AttachmentParentType::Event) => Self::SurvivesAttachmentLimits(category),
            (Self::Absent, AttachmentParentType::Span) => Self::SpanEventCreators,
            (Self::Absent, AttachmentParentType::Trace) => Self::TraceEventCreators,
            (Self::SpanEventCreators, AttachmentParentType::Trace) => Self::BothSpanFirst,
            (Self::TraceEventCreators, AttachmentParentType::Span) => Self::BothTraceFirst,
            _ => return,
        };
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
/// Aggregates header-derived quantities and relationships for early filtering, accounting and
/// telemetry. Transaction payloads are never parsed to build this summary.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default)]
pub struct EnvelopeSummary {
    /// The event category and attachment dependencies of the event-creating items.
    pub event: EventGroup,

    /// Total attachment storage across all parent types.
    pub attachment_bytes: usize,
    pub attachment_count: usize,

    /// An attachment that does not create an event is present, even if already rate limited.
    pub has_plain_attachments: bool,
    /// Counted attachments are present for the corresponding parent type.
    pub has_event_attachments: bool,
    pub has_span_attachments: bool,
    pub has_trace_attachments: bool,

    /// Metrics items require client backoff even when they carry no accounting quantities.
    pub has_metrics: bool,

    /// The number of all session updates.
    pub session_quantity: usize,

    /// The number of profiles.
    pub profile_quantity: ProfileQuantities,

    /// The number of replays.
    pub replay_quantity: usize,

    /// The number of user reports (legacy item type for user feedback).
    pub user_report_quantity: usize,

    /// V1 reports participating in quota checks. Legacy accounting still counts a report with
    /// a misplaced Nintendo attachment header, although early filtering excludes it.
    pub has_user_reports: bool,

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

    /// Standalone spans, transaction roots and embedded spans declared in transaction headers.
    /// Missing embedded-span counts are calculated only when a transaction is rejected.
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

    /// Summarizes the original items without parsing payloads or constructing per-item quantities.
    pub fn compute_items<'a>(items: impl IntoIterator<Item = &'a Item>) -> Self {
        let mut summary = Self::empty();

        for item in items {
            let rate_limited = item.rate_limited();
            if !rate_limited {
                if let Some(source) = item.source_quantities() {
                    summary.secondary_transaction_quantity += source.transactions;
                    summary.secondary_span_quantity += source.spans;
                }
                summary.payload_size += item.len();
            }

            // Already-limited items and Nintendo input have no individual quantities here.
            // Event-creating items still count toward the event; metrics still advertise client limits.
            let attachment_type = item.attachment_type();
            let count_individual = !rate_limited
                && attachment_type != Some(AttachmentType::NintendoSwitchDyingMessage);
            let count = if count_individual {
                item.item_count().unwrap_or(1) as usize
            } else {
                0
            };

            match item.ty() {
                ItemType::Event | ItemType::UnrealReport => summary
                    .event
                    .add_event_creator(DataCategory::Error, AttachmentParentType::Event),
                ItemType::Security | ItemType::RawSecurity => summary
                    .event
                    .add_event_creator(DataCategory::Security, AttachmentParentType::Event),
                ItemType::UserReportV2 => summary
                    .event
                    .add_event_creator(DataCategory::UserReportV2, AttachmentParentType::Event),
                ItemType::UserReport => {
                    summary.user_report_quantity += usize::from(!rate_limited);
                    summary.has_user_reports |= count_individual;
                }
                ItemType::Session | ItemType::Sessions => summary.session_quantity += count,
                ItemType::ReplayEvent | ItemType::ReplayRecording | ItemType::ReplayVideo => {
                    summary.replay_quantity += count;
                }
                ItemType::CheckIn => summary.monitor_quantity += count,
                ItemType::Span => summary.span_quantity += count,
                ItemType::MetricBuckets => summary.has_metrics = true,
                ItemType::FormData | ItemType::ClientReport | ItemType::Unknown(_) => {}
                ItemType::Log => {
                    summary.log_item_quantity += count;
                    if count_individual {
                        summary.log_byte_quantity += item.len().max(1);
                    }
                }
                ItemType::TraceMetric => {
                    summary.trace_metric_quantity += count;
                    if count_individual {
                        summary.trace_metric_byte_quantity += item.len().max(1);
                    }
                }
                ItemType::Transaction => {
                    summary
                        .event
                        .add_event_creator(DataCategory::Transaction, AttachmentParentType::Event);
                    if count_individual {
                        summary.span_quantity += 1 + item.span_count().unwrap_or(0) as usize;
                    }
                }
                ItemType::Profile => {
                    summary.profile_quantity.total += count;
                    match item.profile_type() {
                        Some(ProfileType::Backend) => summary.profile_quantity.backend += count,
                        Some(ProfileType::Ui) => summary.profile_quantity.ui += count,
                        None => {}
                    }
                }
                ItemType::ProfileChunk => match item.profile_type() {
                    Some(ProfileType::Backend) => summary.profile_chunk_quantity += count,
                    Some(ProfileType::Ui) => summary.profile_chunk_ui_quantity += count,
                    None => {}
                },
                ItemType::Integration => match item.content_type() {
                    Some(ContentType::Integration(Integration::Spans(_))) => {
                        summary.span_quantity += count;
                    }
                    Some(ContentType::Integration(Integration::Logs(_))) => {
                        summary.log_item_quantity += count;
                        if count_individual {
                            summary.log_byte_quantity += item.len().max(1);
                        }
                    }
                    _ => {}
                },
                ItemType::Attachment => {
                    let parent = item.attachment_parent_type();
                    if count_individual {
                        summary.attachment_count += 1;
                        summary.attachment_bytes += item.attachment_body_size();
                        match parent {
                            AttachmentParentType::Event => summary.has_event_attachments = true,
                            AttachmentParentType::Span => summary.has_span_attachments = true,
                            AttachmentParentType::Trace => summary.has_trace_attachments = true,
                        }
                    }
                    match AttachmentRole::from_type(attachment_type) {
                        AttachmentRole::CreatesEvent => {
                            summary.event.add_event_creator(DataCategory::Error, parent);
                        }
                        AttachmentRole::Plain => summary.has_plain_attachments = true,
                    }
                }
            }
        }

        summary
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
