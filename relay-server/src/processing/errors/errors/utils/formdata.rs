use serde_json::Value as SerdeValue;

use crate::envelope::Item;
use crate::utils::{self, ChunkedFormDataAggregator, FormDataIter};

pub fn merge_formdata(target: &mut SerdeValue, item: &Item) {
    let payload = item.payload();
    let mut aggregator = ChunkedFormDataAggregator::new();

    for entry in FormDataIter::new(&payload) {
        if entry.key() == "sentry" || entry.key().starts_with("sentry___") {
            // Custom clients can submit longer payloads and should JSON encode event data into
            // the optional `sentry` field or a `sentry___<namespace>` field.
            match serde_json::from_str(entry.value()) {
                Ok(event) => utils::merge_values(target, event),
                Err(_) => relay_log::debug!("invalid json event payload in sentry form field"),
            }
        } else if let Some(index) = utils::get_sentry_chunk_index(entry.key(), "sentry__") {
            // Electron SDK splits up long payloads into chunks starting at sentry__1 with an
            // incrementing counter. Assemble these chunks here and then decode them below.
            aggregator.insert(index, entry.value());
        } else if let Some(keys) = utils::get_sentry_entry_indexes(entry.key()) {
            // Try to parse the nested form syntax `sentry[key][key]` This is required for the
            // Breakpad client library, which only supports string values of up to 64
            // characters.
            let limit = 7; // Aligns with most `max_depth`:s found in `Event`.
            if keys.len() > limit {
                relay_log::debug!(
                    "too many nested form-data entry keys: {} > {}",
                    keys.len(),
                    limit
                );
                continue;
            }
            utils::update_nested_value(target, &keys, entry.value());
        } else {
            // Merge additional form fields from the request with `extra` data from the event
            // payload and set defaults for processing. This is sent by clients like Breakpad or
            // Crashpad.
            utils::update_nested_value(target, &["extra", entry.key()], entry.value());
        }
    }

    if !aggregator.is_empty() {
        match serde_json::from_str(&aggregator.join()) {
            Ok(event) => utils::merge_values(target, event),
            Err(_) => relay_log::debug!("invalid json event payload in sentry__* form fields"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::envelope::{ContentType, ItemType};
    use crate::utils::FormDataWriter;

    fn form_data_item(entries: &[(&str, &str)]) -> Item {
        let mut writer = FormDataWriter::new();
        for (key, value) in entries {
            writer.append(key, value);
        }

        let mut item = Item::new(ItemType::FormData);
        item.set_payload(ContentType::Text, writer.into_inner());
        item
    }

    #[test]
    fn test_merge_formdata_accepts_nested_key_within_limit() {
        let item = form_data_item(&[("sentry[a][b][c][d][e][f][g]", "deep")]);

        let mut target = SerdeValue::Object(Default::default());
        merge_formdata(&mut target, &item);

        assert_eq!(
            target,
            serde_json::json!({
                "a": { "b": { "c": { "d": { "e": { "f": { "g": "deep" } } } } } }
            })
        );
    }

    #[test]
    fn test_merge_formdata_drops_nested_key_over_limit() {
        let deep_key = format!("sentry{}", "[a]".repeat(50_000));
        let item = form_data_item(&[(deep_key.as_str(), "too deep")]);

        let mut target = SerdeValue::Object(Default::default());
        merge_formdata(&mut target, &item);

        assert_eq!(target, serde_json::json!({}));
    }
}
