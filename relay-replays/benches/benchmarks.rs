use std::io::Read;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use flate2::{bufread::ZlibEncoder, Compression};
use relay_replays::recording::transcode_replay;

fn bench_recording(c: &mut Criterion) {
    let payload = include_bytes!("../tests/fixtures/rrweb.json");

    // Compress the payload to mimic real-world behavior. The replay processor can also handle
    // uncompressed payloads, but those happen infrequently.
    let mut compressed = Vec::new();
    let mut encoder = ZlibEncoder::new(payload.as_slice(), Compression::default());
    encoder.read_to_end(&mut compressed).unwrap();

    c.bench_with_input(BenchmarkId::new("rrweb", 1), &compressed, |b, &_| {
        b.iter(|| {
            let mut buf = Vec::new();
            transcode_replay(criterion::black_box(&compressed), usize::MAX, &mut buf).ok();
            buf
        });
    });
}

criterion_group!(benches, bench_recording);
criterion_main!(benches);
