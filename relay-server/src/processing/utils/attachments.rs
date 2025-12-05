use std::error::Error;
use std::time::Instant;

use relay_pii::{PiiAttachmentsProcessor, SelectorPathItem, SelectorSpec};
use relay_statsd::metric;

use crate::envelope::{AttachmentType, ContentType, Item, ItemType};
use crate::statsd::RelayTimers;

use crate::services::projects::project::ProjectInfo;
use relay_dynamic_config::Feature;

/// Apply data privacy rules to attachments in the envelope.
///
/// This only applies the new PII rules that explicitly select `ValueType::Binary` or one of the
/// attachment types. When special attachments are detected, these are scrubbed with custom
/// logic; otherwise the entire attachment is treated as a single binary blob.
pub fn scrub<'a>(attachments: impl Iterator<Item = &'a mut Item>, project_info: &ProjectInfo) {
    if let Some(ref config) = project_info.config.pii_config {
        let view_hierarchy_scrubbing_enabled = project_info
            .config
            .features
            .has(Feature::ViewHierarchyScrubbing);
        for item in attachments {
            debug_assert_eq!(item.ty(), &ItemType::Attachment);
            if view_hierarchy_scrubbing_enabled
                && item.attachment_type() == Some(&AttachmentType::ViewHierarchy)
            {
                scrub_view_hierarchy(item, config)
            } else if item.attachment_type() == Some(&AttachmentType::Minidump) {
                scrub_minidump(item, config)
            } else if item.ty() == &ItemType::Attachment && has_simple_attachment_selector(config) {
                // We temporarily only scrub attachments to projects that have at least one simple attachment rule,
                // such as `$attachments.'foo.txt'`.
                // After we have assessed the impact on performance we can relax this condition.
                scrub_attachment(item, config)
            }
        }
    }
}

fn scrub_minidump(item: &mut crate::envelope::Item, config: &relay_pii::PiiConfig) {
    debug_assert_eq!(item.attachment_type(), Some(&AttachmentType::Minidump));
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
            metric!(
                timer(RelayTimers::AttachmentScrubbing),
                attachment_type = "minidump",
                {
                    processor.scrub_attachment(filename, &mut payload);
                }
            )
        }
    }

    let content_type = item
        .content_type()
        .unwrap_or(&ContentType::Minidump)
        .clone();

    item.set_payload(content_type, payload);
}

fn scrub_view_hierarchy(item: &mut crate::envelope::Item, config: &relay_pii::PiiConfig) {
    let processor = PiiAttachmentsProcessor::new(config.compiled());

    let payload = item.payload();
    let start = Instant::now();
    match processor.scrub_json(&payload) {
        Ok(output) => {
            metric!(
                timer(RelayTimers::ViewHierarchyScrubbing) = start.elapsed(),
                status = "ok"
            );
            let content_type = item.content_type().unwrap_or(&ContentType::Json).clone();
            item.set_payload(content_type, output);
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

pub fn has_simple_attachment_selector(config: &relay_pii::PiiConfig) -> bool {
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

fn scrub_attachment(item: &mut crate::envelope::Item, config: &relay_pii::PiiConfig) {
    let filename = item.filename().unwrap_or_default();
    let mut payload = item.payload().to_vec();

    let processor = PiiAttachmentsProcessor::new(config.compiled());
    let attachment_type_tag = match item.attachment_type() {
        Some(t) => t.to_string(),
        None => "".to_owned(),
    };
    metric!(
        timer(RelayTimers::AttachmentScrubbing),
        attachment_type = &attachment_type_tag,
        {
            processor.scrub_attachment(filename, &mut payload);
        }
    );

    item.set_payload_without_content_type(payload);
}

#[cfg(test)]
mod tests {
    use relay_pii::PiiConfig;

    use super::*;

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
