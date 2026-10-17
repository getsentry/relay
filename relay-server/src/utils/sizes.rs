use relay_config::ConfigSnapshot;

use crate::envelope::{AttachmentType, Envelope, Item, ItemType};
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
        let limit = match item.ty() {
            ItemType::Event
            | ItemType::Transaction
            | ItemType::Security
            | ItemType::ReplayEvent
            | ItemType::RawSecurity
            | ItemType::UserReportV2
            | ItemType::FormData => &mut event,
            ItemType::Attachment | ItemType::UnrealReport | ItemType::UserReport => &mut attachment,
            ItemType::ReplayRecording => &mut replay_recording,
            ItemType::ReplayVideo => &mut replay_video,
            ItemType::Session | ItemType::Sessions => &mut session,
            ItemType::ClientReport => &mut client_report,
            ItemType::Profile | ItemType::ProfileChunk => &mut profile,
            ItemType::CheckIn => &mut check_in,
            ItemType::MetricBuckets => &mut metric_bucket,
            ItemType::Log => &mut log,
            ItemType::TraceMetric => &mut trace_metric,
            ItemType::Span => match item.is_container() {
                true => &mut span,
                false => &mut span_legacy,
            },
            ItemType::Integration => match item.integration() {
                Some(Integration::Logs(_)) => &mut log_integration,
                Some(Integration::Spans(_)) => &mut span_integration,
                None => {
                    // Shouldn't be possible, if it still happens due to a bug, be defensive.
                    debug_assert!(false);
                    &mut empty
                }
            },
            // Unknown items are either filtered or forwarded as is.
            ItemType::Unknown(_) => continue,
        };

        let result = match (item.ty(), item.attachment_type()) {
            (ItemType::Attachment, Some(AttachmentType::Flamegraph)) => limit
                .apply_with_additional_limit(
                    item,
                    relay_profiling::flamegraph::MAX_FLAMEGRAPH_SIZE,
                ),
            _ => limit.apply(item),
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

            let discard_item: DiscardItemType = item
                .attachment_type()
                .map(|t| t.into())
                .unwrap_or_else(|| item.ty().into());
            return Err(discard_item);
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
    fn apply(&mut self, item: &Item) -> Result<(), Rejected> {
        self.apply_with_additional_limit(item, usize::MAX)
    }

    /// Applies a stricter per-item size limit while sharing the total count and size budgets.
    fn apply_with_additional_limit(
        &mut self,
        item: &Item,
        additional_limit: usize,
    ) -> Result<(), Rejected> {
        let size = match item.is_container() {
            // For container items, check that the contained items obey the size limit on average.
            //
            // Individual size validation must then be done again after parsing.
            true => item.len() / item.item_count().unwrap_or(1).max(1) as usize,
            false => item.len(),
        };
        if size > self.item_size.min(additional_limit) {
            return Err(Rejected::ItemSize);
        }

        self.total_count = self
            .total_count
            .checked_sub(1)
            .ok_or(Rejected::TotalCount)?;

        self.total_size = self
            .total_size
            .checked_sub(item.len())
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
    use super::*;
    use crate::envelope::AttachmentType;
    use bytes::Bytes;

    #[test]
    fn test_additional_item_size_limit() {
        let mut limit = Limit {
            item_size: usize::MAX,
            total_count: 2,
            total_size: usize::MAX,
        };
        let mut item = Item::new(ItemType::Attachment);
        item.set_attachment_type(AttachmentType::Flamegraph);
        item.set_payload_without_content_type("1234");
        assert!(matches!(
            limit.apply_with_additional_limit(&item, 3),
            Err(Rejected::ItemSize)
        ));
        assert_eq!(limit.total_count, 2);

        limit.item_size = 3;
        assert!(matches!(
            limit.apply_with_additional_limit(&item, 4),
            Err(Rejected::ItemSize)
        ));

        limit.item_size = 4;
        limit.total_size = 8;
        assert!(limit.apply_with_additional_limit(&item, 4).is_ok());
        assert_eq!(limit.total_count, 1);
        assert_eq!(limit.total_size, 4);
        item.set_attachment_type(AttachmentType::Attachment);
        assert!(limit.apply(&item).is_ok());
        assert_eq!(limit.total_count, 0);
        assert_eq!(limit.total_size, 0);
        assert!(matches!(limit.apply(&item), Err(Rejected::TotalCount)));

        limit.total_count = 1;
        limit.total_size = 3;
        item.set_attachment_type(AttachmentType::Flamegraph);
        assert!(matches!(
            limit.apply_with_additional_limit(&item, 4),
            Err(Rejected::TotalSize)
        ));
    }

    #[test]
    fn test_flamegraph_size_limit() {
        let config = relay_config::Config::default();
        let mut envelope = Envelope::parse_bytes(Bytes::from_static(
            b"{\"dsn\":\"https://e12d836b15bb49d7bbf99e64295d995b@sentry.io/42\"}\n",
        ))
        .unwrap();
        let mut item = Item::new(ItemType::Attachment);
        item.set_attachment_type(AttachmentType::Flamegraph);
        item.set_payload_without_content_type(vec![b' '; 10 * 1024 * 1024]);
        envelope.add_item(item.clone());
        assert!(check_envelope_size_limits(&config.current(), &envelope).is_ok());
        item.set_payload_without_content_type(vec![b' '; 10 * 1024 * 1024 + 1]);
        envelope.take_items_by(|_| true);
        envelope.add_item(item);
        assert_eq!(
            check_envelope_size_limits(&config.current(), &envelope),
            Err(DiscardItemType::Attachment(
                crate::services::outcome::DiscardAttachmentType::Flamegraph
            ))
        );
    }
}
