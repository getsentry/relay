use std::error::Error;
use std::time::Instant;

use relay_config::ConfigSnapshot;
use relay_pii::{PiiAttachmentsProcessor, PiiConfig, SelectorPathItem, SelectorSpec};
use relay_statsd::metric;

#[cfg(feature = "processing")]
use crate::envelope::AttachmentPlaceholder;
use crate::envelope::{AttachmentType, ContentType, Item, ItemType};
use crate::managed::{Counted, Managed, RecordKeeper, RetainMut};
use crate::processing::Context;
use crate::services::processor::ProcessingError;
use crate::statsd::RelayTimers;

use crate::services::projects::project::ProjectInfo;
use crate::utils::sample;
use relay_dynamic_config::Feature;

/// Drops attachments that require an explicit event from envelopes without one.
pub fn validate_standalone_attachments<T, V>(
    managed: &mut Managed<T>,
    select: impl FnOnce(&mut T) -> &mut V,
) where
    T: Counted,
    V: RetainMut<Item>,
{
    managed.retain(select, |attachment, _| match attachment.attachment_type() {
        Some(AttachmentType::Flamegraph) => Err(ProcessingError::InvalidFlamegraph),
        _ => Ok(()),
    });
}

/// Validates the attachments and drops any invalid ones.
///
/// Flame graph transport checks apply on every Relay. Payload and placeholder validation only
/// run on processing Relays.
pub fn validate_attachments<T, V>(
    managed: &mut Managed<T>,
    select: impl FnOnce(&mut T) -> &mut V,
    ctx: Context<'_>,
) where
    T: Counted,
    V: RetainMut<Item>,
{
    managed.retain(select, |attachment, _| {
        if attachment.attachment_type() == Some(AttachmentType::Flamegraph)
            && !attachment.has_valid_flamegraph_headers()
        {
            return Err(ProcessingError::InvalidFlamegraph);
        }
        if ctx.is_processing() {
            validate(attachment, ctx.config)
        } else {
            Ok(())
        }
    });
}

#[cfg_attr(not(feature = "processing"), expect(unused_variables))]
fn validate(item: &Item, config: &ConfigSnapshot) -> Result<(), ProcessingError> {
    #[cfg(not(feature = "processing"))]
    return Ok(());

    #[cfg(feature = "processing")]
    {
        use crate::services::upload::{Final, SignedLocation};

        if item.attachment_type() == Some(AttachmentType::Flamegraph) {
            if !item.has_valid_flamegraph_headers() {
                return Err(ProcessingError::InvalidFlamegraph);
            }
            relay_profiling::flamegraph::validate(&item.payload())
                .map_err(|_| ProcessingError::InvalidFlamegraph)?;
            return Ok(());
        }

        if !item.is_attachment_ref() {
            return Ok(());
        }

        let payload = item.payload();
        let payload: AttachmentPlaceholder =
            serde_json::from_slice(&payload).map_err(|_| ProcessingError::InvalidAttachmentRef)?;
        let signed_location: SignedLocation<Final> = SignedLocation::try_from_str(payload.location)
            .ok_or(ProcessingError::InvalidAttachmentRef)?;
        // NOTE: Using the received timestamp here breaks tests without a pop-relay.
        let location = signed_location
            .verify(chrono::Utc::now(), config)
            .map_err(|_| ProcessingError::InvalidAttachmentRef)?;
        let signed_length = location.kind.length;

        match item.attachment_body_size() == signed_length {
            true => Ok(()),
            false => Err(ProcessingError::InvalidAttachmentRef),
        }
    }
}

/// Apply data privacy rules to attachments in the envelope.
///
/// This only applies the new PII rules that explicitly select `ValueType::Binary` or one of the
/// attachment types. When special attachments are detected, these are scrubbed with custom
/// logic; otherwise the entire attachment is treated as a single binary blob.
///
/// Requires `record_keeper` since scrubbing a view hierarchy might change the size of it and hence
/// modifies the attachment quantity.
pub fn scrub<'a>(
    attachments: impl Iterator<Item = &'a mut Item>,
    project_info: &ProjectInfo,
    mut record_keeper: Option<&mut RecordKeeper>,
) {
    let _ = record_keeper;
    if let Some(ref config) = project_info.config.pii_config {
        let view_hierarchy_scrubbing_enabled = project_info
            .config
            .features
            .has(Feature::ViewHierarchyScrubbing);
        for item in attachments {
            debug_assert_eq!(item.ty(), &ItemType::Attachment);
            if item.is_attachment_ref() {
                relay_log::trace!("Skip attachment scrubbing for placeholder");
                continue;
            } else if view_hierarchy_scrubbing_enabled
                && item.attachment_type() == Some(AttachmentType::ViewHierarchy)
            {
                let old_size = item.payload().len();
                scrub_view_hierarchy(item, config);
                let new_size = item.payload().len();

                if let Some(record_keeper) = record_keeper.as_mut() {
                    record_keeper.modify_by(
                        relay_quotas::DataCategory::Attachment,
                        new_size as isize - old_size as isize,
                    );
                }
            } else if item.attachment_type() == Some(AttachmentType::Minidump) {
                scrub_minidump(project_info, item, config)
            } else if item.ty() == &ItemType::Attachment && has_simple_attachment_selector(config) {
                // We temporarily only scrub attachments to projects that have at least one simple attachment rule,
                // such as `$attachments.'foo.txt'`.
                // After we have assessed the impact on performance we can relax this condition.
                scrub_attachment(project_info, item, config)
            }
        }
    }
}

fn scrub_minidump(project_info: &ProjectInfo, item: &mut Item, config: &PiiConfig) {
    debug_assert_eq!(item.attachment_type(), Some(AttachmentType::Minidump));
    let filename = item.filename().unwrap_or_default();
    let mut payload = item.payload().to_vec();

    let processor = PiiAttachmentsProcessor::new(config.compiled());

    // Minidump scrubbing can fail if the minidump cannot be parsed. In this case, we
    // must be conservative and treat it as a plain attachment. Under extreme
    // conditions, this could destroy stack memory.
    let start = Instant::now();
    match processor.scrub_minidump(filename, &mut payload) {
        Ok(modified) => {
            metric!(
                timer(RelayTimers::MinidumpScrubbing) = start.elapsed(),
                status = if modified { "ok" } else { "n/a" },
            );
            if modified && sample(0.1).is_keep() {
                relay_log::info!(
                    sentry_project = ?project_info.project_id,
                    "Minidump changed by scrubbing rules",
                );
            }
        }
        Err(scrub_error) => {
            metric!(
                timer(RelayTimers::MinidumpScrubbing) = start.elapsed(),
                status = "error"
            );
            relay_log::debug!(
                error = &scrub_error as &dyn Error,
                "failed to scrub minidump",
            );
            let start = Instant::now();
            let modified = processor.scrub_attachment(filename, &mut payload);
            metric!(
                timer(RelayTimers::AttachmentScrubbing) = start.elapsed(),
                attachment_type = "minidump",
                status = if modified { "ok" } else { "n/a" },
            );
            if modified {
                relay_log::info!(
                    sentry_project = ?project_info.project_id,
                    "Minidump changed by fallback rules",
                );
            }
        }
    }

    item.set_default_content_type(ContentType::Minidump);
    item.set_payload_without_content_type(payload);
}

fn scrub_view_hierarchy(item: &mut Item, config: &PiiConfig) {
    let processor = PiiAttachmentsProcessor::new(config.compiled());

    let payload = item.payload();
    let start = Instant::now();
    match processor.scrub_json(&payload) {
        Ok(output) => {
            metric!(
                timer(RelayTimers::ViewHierarchyScrubbing) = start.elapsed(),
                status = if output != payload { "ok" } else { "n/a" }
            );
            item.set_default_content_type(ContentType::Json);
            item.set_payload_without_content_type(output);
        }
        Err(e) => {
            relay_log::debug!(error = &e as &dyn Error, "failed to scrub view hierarchy",);
            metric!(
                timer(RelayTimers::ViewHierarchyScrubbing) = start.elapsed(),
                status = "error"
            )
        }
    }
}

pub fn has_simple_attachment_selector(config: &PiiConfig) -> bool {
    for application in &config.applications {
        if let SelectorSpec::Path(vec) = &application.0 {
            let Some([a, b]) = vec.get(0..2) else {
                continue;
            };
            if matches!(
                a,
                SelectorPathItem::Type(relay_event_schema::processor::ValueType::Attachments)
            ) && matches!(b, SelectorPathItem::Key(_))
            {
                return true;
            }
        }
    }
    false
}

fn scrub_attachment(project_info: &ProjectInfo, item: &mut Item, config: &PiiConfig) {
    let filename = item.filename().unwrap_or_default();
    let mut payload = item.payload().to_vec();

    let processor = PiiAttachmentsProcessor::new(config.compiled());
    let attachment_type_tag = match item.attachment_type() {
        Some(t) => t.to_string(),
        None => "".to_owned(),
    };
    let start = Instant::now();
    let modified = processor.scrub_attachment(filename, &mut payload);
    metric!(
        timer(RelayTimers::AttachmentScrubbing) = start.elapsed(),
        attachment_type = attachment_type_tag,
        status = if modified { "ok" } else { "n/a" },
    );
    if modified {
        relay_log::info!(
            sentry_project = ?project_info.project_id,
            "Attachment changed by scrubbing rules",
        );
    }

    item.set_payload_without_content_type(payload);
}

#[cfg(test)]
mod tests {
    use relay_pii::PiiConfig;

    use super::*;

    fn flamegraph_item(payload: serde_json::Value) -> Item {
        let mut item = Item::new(ItemType::Attachment);
        item.set_attachment_type(AttachmentType::Flamegraph);
        item.set_filename("flamegraph.json");
        item.set_payload(ContentType::Json, serde_json::to_vec(&payload).unwrap());
        item
    }

    fn flamegraph_payload() -> serde_json::Value {
        serde_json::json!({
            "version": "1", "platform": "python",
            "frames": [{"function": "main"}],
            "trees": [{"roots": [{"frame_id": 0, "sample_count": 10, "children": []}]}]
        })
    }

    #[test]
    fn test_flamegraph_headers_on_non_processing_relay() {
        let mut invalid = flamegraph_item(flamegraph_payload());
        invalid.set_filename("wrong.json");
        let valid = flamegraph_item(flamegraph_payload());
        let mut ordinary = Item::new(ItemType::Attachment);
        ordinary.set_payload(ContentType::Text, "keep me");
        let rejected_size = invalid.len() as u64;
        let (mut attachments, mut handle) =
            Managed::for_test(vec![invalid, valid, ordinary]).build();

        validate_attachments(&mut attachments, |a| a, Context::for_test());

        assert_eq!(attachments.len(), 2);
        assert_eq!(attachments[0].filename(), Some("flamegraph.json"));
        assert_eq!(attachments[1].payload().as_ref(), b"keep me");
        attachments.accept(|_| ());
        let outcome = crate::services::outcome::Outcome::Invalid(
            crate::services::outcome::DiscardReason::InvalidFlamegraph,
        );
        handle.assert_outcome(
            &outcome,
            relay_quotas::DataCategory::Attachment,
            rejected_size,
        );
        handle.assert_outcome(&outcome, relay_quotas::DataCategory::AttachmentItem, 1);
    }

    #[test]
    fn test_flamegraph_without_length_header() {
        let payload = serde_json::to_string(&flamegraph_payload()).unwrap();
        let headers = r#"{"type":"attachment","attachment_type":"event.flamegraph","filename":"flamegraph.json","content_type":"application/json"}"#;
        let (item, _) = Item::parse(bytes::Bytes::from(format!("{headers}\n{payload}\n"))).unwrap();
        let (mut attachments, _handle) = Managed::for_test(vec![item]).build();

        validate_attachments(&mut attachments, |a| a, Context::for_test());

        assert_eq!(attachments.len(), 1);
        assert_eq!(attachments[0].payload().as_ref(), payload.as_bytes());
        attachments.accept(|_| ());
    }

    #[test]
    fn test_flamegraph_requires_explicit_event() {
        let flamegraph = flamegraph_item(flamegraph_payload());
        let rejected_size = flamegraph.len() as u64;
        let mut ordinary = Item::new(ItemType::Attachment);
        ordinary.set_payload(ContentType::Text, "keep me");
        let (mut attachments, mut handle) = Managed::for_test(vec![flamegraph, ordinary]).build();

        validate_standalone_attachments(&mut attachments, |a| a);

        assert_eq!(attachments.len(), 1);
        assert_eq!(attachments[0].payload().as_ref(), b"keep me");
        attachments.accept(|_| ());
        let outcome = crate::services::outcome::Outcome::Invalid(
            crate::services::outcome::DiscardReason::InvalidFlamegraph,
        );
        handle.assert_outcome(
            &outcome,
            relay_quotas::DataCategory::Attachment,
            rejected_size,
        );
        handle.assert_outcome(&outcome, relay_quotas::DataCategory::AttachmentItem, 1);
    }

    #[cfg(feature = "processing")]
    #[test]
    fn test_flamegraph_invalid_payloads() {
        use serde_json::json;
        let config = relay_config::Config::default();
        let cases = [
            ("/version", json!("2")),
            ("/version", json!(1)),
            ("/platform", json!("unknown")),
            ("/frames", json!([])),
            ("/frames", json!([{}])),
            ("/frames", json!([{"function": 1}])),
            ("/frames", json!([{"instruction_addr": "not an address"}])),
            ("/trees", json!([])),
            ("/trees/0/roots", json!([])),
            ("/trees/0/roots/0/frame_id", json!(1)),
            ("/trees/0/roots/0/frame_id", json!(-1)),
            ("/trees/0/roots/0/frame_id", json!(0.0)),
            ("/trees/0/roots/0/sample_count", json!(0)),
            ("/trees/0/roots/0/sample_count", json!(-1)),
            ("/trees/0/roots/0/sample_count", json!(1.5)),
            ("/trees/0/roots/0/sample_count", json!(9007199254740992u64)),
            (
                "/trees/0/roots/0/children",
                json!([{"frame_id": 0, "sample_count": 11}]),
            ),
            (
                "/trees/0/roots/0/children",
                json!([
                    {"frame_id": 0, "sample_count": 6}, {"frame_id": 0, "sample_count": 5}
                ]),
            ),
            ("/trees/0/roots/0/children", json!(null)),
            (
                "/trees/0",
                json!({"thread_id": 42, "roots": [{"frame_id": 0, "sample_count": 1}]}),
            ),
            (
                "/trees/0",
                json!({"thread_attributed": "true", "roots": [{"frame_id": 0, "sample_count": 1}]}),
            ),
            ("/platform", json!("cocoa")),
        ];
        for (path, value) in cases {
            let mut payload = flamegraph_payload();
            *payload.pointer_mut(path).unwrap() = value;
            assert!(
                validate(&flamegraph_item(payload), &config.current()).is_err(),
                "{path}"
            );
        }
        for field in ["version", "platform", "frames", "trees"] {
            let mut payload = flamegraph_payload();
            payload.as_object_mut().unwrap().remove(field);
            assert!(
                validate(&flamegraph_item(payload), &config.current()).is_err(),
                "{field}"
            );
        }
        let mut item = flamegraph_item(flamegraph_payload());
        item.set_payload(ContentType::Json, "not json");
        assert!(validate(&item, &config.current()).is_err());
    }

    #[cfg(feature = "processing")]
    #[test]
    fn test_flamegraph_invalid_headers() {
        let config = relay_config::Config::default();
        let mut item = flamegraph_item(flamegraph_payload());
        item.set_filename("other.json");
        assert!(validate(&item, &config.current()).is_err());
        let mut item = flamegraph_item(flamegraph_payload());
        item.set_payload(ContentType::Text, item.payload());
        assert!(validate(&item, &config.current()).is_err());
        let payload = serde_json::to_string(&flamegraph_payload()).unwrap();
        for headers in [
            r#"{"type":"attachment","attachment_type":"event.flamegraph","content_type":"application/json"}"#,
            r#"{"type":"attachment","attachment_type":"event.flamegraph","filename":"flamegraph.json"}"#,
        ] {
            let (item, _) =
                Item::parse(bytes::Bytes::from(format!("{headers}\n{payload}\n"))).unwrap();
            assert!(validate(&item, &config.current()).is_err());
        }
    }

    #[cfg(feature = "processing")]
    #[test]
    fn test_flamegraph_preserves_unknown_fields_and_recursive_frames() {
        let config = relay_config::Config::default();
        let mut payload = flamegraph_payload();
        payload["future"] = serde_json::json!({"keep": "this"});
        payload["trees"][0]["roots"][0]["children"] = serde_json::json!([
            {"frame_id": 0, "sample_count": 6, "future": true},
            {"frame_id": 0, "sample_count": 4}
        ]);
        let item = flamegraph_item(payload);
        let original = item.payload();
        validate(&item, &config.current()).unwrap();
        assert_eq!(item.payload(), original);
    }

    #[cfg(feature = "processing")]
    #[test]
    fn test_flamegraph_native_inputs_and_frame_budget() {
        use serde_json::json;
        let config = relay_config::Config::default();
        let mut payload = flamegraph_payload();
        payload["platform"] = json!("cocoa");
        payload["debug_meta"] = json!({"images": [{
            "type": "macho", "debug_id": "84a04d24-0e60-3810-a8c0-90a65e2df61a",
            "code_file": "ExampleApp", "image_addr": "0x100000000",
            "image_size": 8192, "arch": "arm64", "future": true
        }]});
        let frame = json!({
            "instruction_addr": "0x100001000", "function": "main", "package": "ExampleApp",
            "filename": "Sources/Main.swift", "lineno": 42, "future": "preserved"
        });
        payload["frames"] = json!(vec![frame; 2501]);
        payload["trees"][0]["roots"][0]["frame_id"] = json!(2500);
        payload["trees"][0]["roots"][0]["sample_count"] = json!(9007199254740991u64);
        payload["trees"][0]["thread_id"] = json!("42");
        payload["trees"][0]["thread_attributed"] = json!(true);
        for mode in ["abs", "rel:0", "rel:84a04d24-0e60-3810-a8c0-90a65e2df61a"] {
            payload["frames"][0]["addr_mode"] = json!(mode);
            let item = flamegraph_item(payload.clone());
            let original = item.payload();
            validate(&item, &config.current()).unwrap();
            assert_eq!(item.payload(), original);
        }
        for mode in ["bogus", "rel:1", "rel:invalid"] {
            payload["frames"][0]["addr_mode"] = json!(mode);
            assert!(validate(&flamegraph_item(payload.clone()), &config.current()).is_err());
        }
        payload["frames"][0]["addr_mode"] = json!("abs");
        payload["debug_meta"] = json!({"images": [{"type": "macho", "debug_id": "invalid"}]});
        assert!(validate(&flamegraph_item(payload), &config.current()).is_err());
    }

    #[test]
    fn matches_attachment_selector() {
        let config = r#"{
            "rules": {"0": {"type": "ip", "redaction": {"method": "remove"}}},
            "applications": {"$attachments.'foo.txt'": ["0"]}
        }"#;
        let config: PiiConfig = serde_json::from_str(config).unwrap();
        assert!(has_simple_attachment_selector(&config));
    }

    #[test]
    fn does_not_match_wildcard() {
        let config = r#"{
            "rules": {},
            "applications": {"$attachments.**":["0"]}
        }"#;
        let config: PiiConfig = serde_json::from_str(config).unwrap();
        assert!(!has_simple_attachment_selector(&config));
    }

    #[test]
    fn does_not_match_empty() {
        let config = r#"{
            "rules": {},
            "applications": {}
        }"#;
        let config: PiiConfig = serde_json::from_str(config).unwrap();
        assert!(!has_simple_attachment_selector(&config));
    }

    #[test]
    fn does_not_match_something_else() {
        let config = r#"{
            "rules": {},
            "applications": {
                "**": ["0"]
            }
        }"#;
        let config: PiiConfig = serde_json::from_str(config).unwrap();
        assert!(!has_simple_attachment_selector(&config));
    }
}
