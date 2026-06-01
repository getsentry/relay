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
