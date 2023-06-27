use std::collections::BTreeMap;

use relay_general::protocol::Span;

use crate::metrics_extraction::spans::types::SpanTagKey;

pub(crate) fn extract_cache_span_tags(_span: &Span) -> BTreeMap<SpanTagKey, String> {
    let mut tags = BTreeMap::new();

    tags.insert(SpanTagKey::Module, "cache".to_owned());

    tags
}
