use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use relay_event_normalization::{NormalizationConfig, normalize_event};
use relay_event_schema::processor::{ProcessingState, process_value};
use relay_event_schema::protocol::{Event, Replay};
use relay_pii::{PiiConfig, PiiProcessor};
use relay_protocol::{Annotated, Meta, Remark, RemarkType};
use serde_json::json;

#[global_allocator]
static ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn metadata(c: &mut Criterion) {
    let empty = Meta::default();
    let mut marked = Meta::default();
    marked.add_remark(Remark::new(RemarkType::Removed, "@anything:remove"));
    marked.set_original_length(Some(20));

    c.bench_function("meta/clone_empty", |b| b.iter(|| black_box(&empty).clone()));
    c.bench_function("meta/clone_marked", |b| {
        b.iter(|| black_box(&marked).clone())
    });
    c.bench_function("meta/create_and_write", |b| {
        b.iter(|| {
            let mut value = Meta::default();
            value.set_original_length(Some(20));
            value.add_remark(Remark::new(RemarkType::Removed, "@anything:remove"));
            value
        })
    });
    c.bench_function("meta/clone_then_write", |b| {
        b.iter(|| {
            let mut copy = black_box(&marked).clone();
            copy.add_remark(Remark::new(RemarkType::Removed, "second-rule"));
            copy
        })
    });
}

fn transaction(release: &str) -> String {
    let spans: Vec<_> = (1..=50)
        .map(|i| {
            json!({
                "trace_id": "a0fa8803753e40fd8124b21eeb2986b5",
                "span_id": format!("{i:016x}"),
                "parent_span_id": "968cff94913ebb07",
                "op": "cache.get",
                "start_timestamp": 1700000000,
                "timestamp": 1700000001,
            })
        })
        .collect();
    json!({
        "type": "transaction", "transaction": "GET /items", "release": release,
        "start_timestamp": 1700000000, "timestamp": 1700000001,
        "contexts": {"trace": {"op": "http.server", "trace_id": "a0fa8803753e40fd8124b21eeb2986b5", "span_id": "968cff94913ebb07"}},
        "spans": spans,
    }).to_string()
}

fn events(c: &mut Criterion) {
    let basic = include_str!("../../tests/fixtures/basic-event-input.json");
    let extended = include_str!("../../tests/fixtures/extended-event-input.json");
    let annotated = include_str!("../../tests/fixtures/extended-event-output.json");
    let scrubbed = json!({"message":"validation failed","extra": {
        "details": vec!["send to person@example.com"; 64],
    }})
    .to_string();
    let rules: PiiConfig = serde_json::from_value(json!({
        "applications": {"$string": ["@email:replace"]}
    }))
    .unwrap();
    let compiled = rules.compiled();
    let config = NormalizationConfig {
        enrich_spans: true,
        max_tag_value_length: 32,
        ..Default::default()
    };
    let clean = transaction("1.0");
    let truncated = transaction(&"a".repeat(80));

    for (name, input, pii) in [
        ("basic", basic, None),
        ("extended", extended, None),
        ("extended_with_meta", annotated, None),
        ("scrub_64_strings", scrubbed.as_str(), Some(compiled)),
        ("transaction_50", clean.as_str(), None),
        ("transaction_50_truncated_release", truncated.as_str(), None),
    ] {
        c.bench_function(&format!("event/{name}"), |b| {
            b.iter(|| {
                let mut event = Annotated::<Event>::from_json(black_box(input)).unwrap();
                normalize_event(&mut event, &config);
                if let Some(pii) = pii {
                    process_value(
                        &mut event,
                        &mut PiiProcessor::new(pii),
                        ProcessingState::root(),
                    )
                    .unwrap();
                }
                event.to_json().unwrap()
            })
        });
    }
    let replay = include_str!("../../tests/fixtures/replay.json");
    c.bench_function("replay/decode_encode", |b| {
        b.iter(|| {
            Annotated::<Replay>::from_json(black_box(replay))
                .unwrap()
                .to_json()
                .unwrap()
        })
    });
}

criterion_group!(benches, metadata, events);
criterion_main!(benches);
