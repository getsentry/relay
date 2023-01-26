use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::{criterion_group, criterion_main};

use relay_replays::recording::Event;

fn bench_recording(c: &mut Criterion) {
    let payload = include_bytes!("../tests/fixtures/rrweb-event-5.json");

    c.bench_with_input(BenchmarkId::new("rrweb", 1), &payload, |b, &_| {
        b.iter(|| ::serde_json::from_slice::<Vec<Event>>(payload).unwrap());
    });
}

criterion_group!(benches, bench_recording);
criterion_main!(benches);
