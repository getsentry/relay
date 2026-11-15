use relay_event_schema::protocol::{Attribute, SpanV2};
use relay_protocol::{Annotated, Value};

/// Returns the size of a SpanV2.
pub fn calculate_size(span: &SpanV2) -> u64 {
    let mut total_size = 0u64;

    if let Some(attributes) = span.attributes.value() {
        total_size += attributes
            .0
            .iter()
            .map(|(k, v)| k.len() + attribute_size(v))
            .sum::<usize>() as u64;
    }

    total_size += span
        .other
        .iter()
        .map(|(k, v)| k.len() + value_size(v))
        .sum::<usize>() as u64;

    total_size
}

fn attribute_size(v: &Annotated<Attribute>) -> usize {
    v.value().map(|v| &v.value.value).map_or(0, value_size)
}

fn value_size(v: &Annotated<Value>) -> usize {
    let Some(v) = v.value() else {
        return 0;
    };

    match v {
        Value::Bool(_) => 1,
        Value::I64(_) => 8,
        Value::U64(_) => 8,
        Value::F64(_) => 8,
        Value::String(v) => v.len(),
        Value::Array(v) => v.iter().map(value_size).sum(),
        Value::Object(v) => v.iter().map(|(k, v)| k.len() + value_size(v)).sum(),
    }
}
