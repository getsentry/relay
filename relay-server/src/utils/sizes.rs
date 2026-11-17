use relay_config::ConfigSnapshot;

use crate::envelope::{Envelope, Item, ItemCount, ItemData};
use crate::integrations::Integration;
use crate::managed::Managed;
use crate::services::outcome::DiscardItemType;
use crate::statsd::RelayCounters;

/// Checks for size limits of items in this envelope.
///
/// Returns `Ok`, if the envelope adheres to the configured size limits. Otherwise, returns
/// an `Err` containing the offending item type, in which case the envelope should be discarded
/// and a `413 Payload Too Large` response should be given.
///
/// Each envelope item is checked against its limits defined in the [`ConfigSnapshot`].
pub fn check_envelope_size_limits(
    config: &ConfigSnapshot,
    envelope: &Envelope,
) -> Result<(), DiscardItemType> {
    // TODO(#6249 / RELAY-272): These limits are built from the old `limits` config,
    // we can think about reworking how limits are configured and exposing a similar structure
    // for these size limits also in the config.
    let mut empty = Limit {
        item_size: 0,
        total_count: 0,
        total_size: 0,
    };
    let mut event = Limit {
        item_size: config.max_event_size(),
        total_count: 1,
        total_size: config.max_event_size(),
    };
    let mut attachment = Limit {
        item_size: config.max_attachment_size(),
        total_count: config.max_attachment_count(),
        total_size: config.max_attachments_size(),
    };
    let mut replay_recording = Limit {
        item_size: config.max_replay_compressed_size(),
        total_count: 1,
        total_size: config.max_replay_compressed_size(),
    };
    let mut replay_video = Limit {
        item_size: config.max_replay_compressed_size(),
        total_count: 1,
        total_size: config.max_replay_compressed_size(),
    };
    let mut session = Limit {
        item_size: config.max_sessions_size(),
        total_count: config.max_session_count(),
        total_size: config.max_sessions_size(),
    };
    let mut client_report = Limit {
        item_size: config.max_client_reports_size(),
        total_count: config.max_client_reports_count(),
        total_size: config.max_client_reports_size(),
    };
    let mut profile = Limit {
        item_size: config.max_profile_size(),
        total_count: 1,
        total_size: config.max_profile_size(),
    };
    let mut check_in = Limit {
        item_size: config.max_check_in_size(),
        total_count: 1,
        total_size: config.max_check_in_size(),
    };
    let mut metric_bucket = Limit {
        item_size: config.max_metric_buckets_size(),
        total_count: 1,
        total_size: config.max_metric_buckets_size(),
    };
    let mut log = Limit {
        item_size: config.max_log_size(),
        total_count: 1,
        total_size: config.max_container_size(),
    };
    let mut log_integration = Limit {
        item_size: config.max_container_size(),
        total_count: 1,
        total_size: config.max_container_size(),
    };
    let mut trace_metric = Limit {
        item_size: config.max_trace_metric_size(),
        total_count: 1,
        total_size: config.max_container_size(),
    };
    let mut span = Limit {
        item_size: config.max_span_size(),
        total_count: 1,
        total_size: config.max_container_size(),
    };
    let mut span_integration = Limit {
        item_size: config.max_container_size(),
        total_count: 1,
        total_size: config.max_container_size(),
    };
    let mut span_legacy = Limit {
        item_size: config.max_span_size(),
        total_count: config.max_standalone_span_count(),
        total_size: config.max_container_size(),
    };

    for item in envelope.items() {
        let result = match item {
            Item::Event(item) => event.apply(item.data(), None),
            Item::Transaction(item) => event.apply(item.data(), None),
            Item::Security(item) => event.apply(item.data(), None),
            Item::ReplayEvent(item) => event.apply(item.data(), None),
            Item::RawSecurity(item) => event.apply(item.data(), None),
            Item::UserReportV2(item) => event.apply(item.data(), None),
            Item::FormData(item) => event.apply(item.data(), None),
            Item::Attachment(item) => attachment.apply(item.data(), None),
            Item::UnrealReport(item) => attachment.apply(item.data(), None),
            Item::UserReport(item) => attachment.apply(item.data(), None),
            Item::ReplayRecording(item) => replay_recording.apply(item.data(), None),
            Item::ReplayVideo(item) => replay_video.apply(item.data(), None),
            Item::Session(item) => session.apply(item.data(), item.item_count()),
            Item::Sessions(item) => session.apply(item.data(), item.item_count()),
            Item::ClientReport(item) => client_report.apply(item.data(), item.item_count()),
            Item::Profile(item) => profile.apply(item.data(), item.item_count()),
            Item::ProfileChunk(item) => profile.apply(item.data(), item.item_count()),
            Item::CheckIn(item) => check_in.apply(item.data(), item.item_count()),
            Item::MetricBuckets(item) => metric_bucket.apply(item.data(), item.item_count()),
            Item::Log(item) => log.apply(item.data(), item.item_count()),
            Item::TraceMetric(item) => trace_metric.apply(item.data(), item.item_count()),
            Item::Span(item) => match item.data().is_container() {
                true => span.apply(item.data(), item.item_count()),
                false => span_legacy.apply(item.data(), item.item_count()),
            },
            Item::Integration(item) => match item.integration() {
                Some(Integration::Errors(_)) => attachment.apply(item.data(), None),
                Some(Integration::Logs(_)) => log_integration.apply(item.data(), None),
                Some(Integration::Spans(_)) => span_integration.apply(item.data(), None),
                None => {
                    // Shouldn't be possible, if it still happens due to a bug, be defensive.
                    debug_assert!(false);
                    empty.apply(item.data(), None)
                }
            },
            // Unknown items are either filtered or forwarded as is.
            Item::Unknown(_) => continue,
        };

        if let Err(err) = result {
            relay_statsd::metric!(
                counter(RelayCounters::EnvelopeSizeLimited) += 1,
                item = item.ty().name(),
                limit = match err {
                    Rejected::ItemSize => "item_size",
                    Rejected::TotalCount => "total_count",
                    Rejected::TotalSize => "total_size",
                }
            );

            return Err(item.into());
        }
    }

    Ok(())
}

enum Rejected {
    ItemSize,
    TotalCount,
    TotalSize,
}

struct Limit {
    /// The maximum size of a single item.
    item_size: usize,
    /// The remaining total count of this item type allowed in an envelope.
    total_count: usize,
    /// The remaining total count of this item type in an envelope.
    total_size: usize,
}

impl Limit {
    fn apply(&mut self, data: &ItemData, item_count: Option<u32>) -> Result<(), Rejected> {
        let size = match data.is_container() {
            // For container items, check that the contained items obey the size limit on average.
            //
            // Individual size validation must then be done again after parsing.
            true => data.len() / item_count.unwrap_or(1).max(1) as usize,
            false => data.len(),
        };
        if size > self.item_size {
            return Err(Rejected::ItemSize);
        }

        self.total_count = self
            .total_count
            .checked_sub(1)
            .ok_or(Rejected::TotalCount)?;

        self.total_size = self
            .total_size
            .checked_sub(data.len())
            .ok_or(Rejected::TotalSize)?;

        Ok(())
    }
}

/// Checks for valid envelope items.
///
/// If Relay is configured to drop unknown items, this function removes them from the Envelope. All
/// known items will be retained.
pub fn remove_unknown_items(config: &ConfigSnapshot, envelope: &mut Managed<Box<Envelope>>) {
    if config.accept_unknown_items() {
        return;
    }

    envelope.modify(|envelope, records| {
        envelope.retain_items(|item| {
            if item.is_unknown() {
                relay_log::debug!("dropping unknown item");
                // Reject items silently, without outcomes.
                records.reject_err(None, &*item);
                false
            } else {
                true
            }
        });
    });
}

#[cfg(test)]
mod tests {
    use relay_config::Config;
    use serde_json::json;

    use super::*;

    #[test]
    fn test_check_envelope_size_limits_container_counts() {
        let config = Config::from_json_value(json!({
            "limits": {
                "max_event_size": 4,
                "max_log_size": 4,
                "max_span_size": 4,
                "max_container_size": 20,
            },
        }))
        .unwrap();

        for (tag, content_type, count, accepted) in [
            (
                "log",
                "application/vnd.sentry.items.log+json",
                json!(3),
                true,
            ),
            (
                "span",
                "application/vnd.sentry.items.span.v2+json",
                json!(3),
                true,
            ),
            ("log", "application/json", json!(3), false),
            ("span", "application/json", json!(3), false),
            (
                "transaction",
                "application/vnd.sentry.items.log+json",
                json!(3),
                false,
            ),
            (
                "log",
                "application/vnd.sentry.items.log+json",
                json!(0),
                false,
            ),
            (
                "log",
                "application/vnd.sentry.items.log+json",
                json!(null),
                false,
            ),
            (
                "log",
                "application/vnd.sentry.items.log+json",
                json!("invalid"),
                false,
            ),
        ] {
            let header = json!({
                "type": tag,
                "content_type": content_type,
                "item_count": count,
            });
            let envelope = Envelope::parse_bytes(
                format!("{{\"dsn\":\"https://e12d836b15bb49d7bbf99e64295d995b@sentry.io/42\"}}\n{header}\n123456789\n").into(),
            )
            .unwrap();
            assert_eq!(
                check_envelope_size_limits(&config.current(), &envelope).is_ok(),
                accepted,
                "{header}",
            );
        }
    }
}
