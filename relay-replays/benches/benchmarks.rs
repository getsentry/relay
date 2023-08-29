use std::io::Read;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use flate2::bufread::ZlibEncoder;
use flate2::Compression;
use relay_pii::DataScrubbingConfig;
use relay_replays::recording::RecordingScrubber;

fn bench_recording(c: &mut Criterion) {
    let payload = include_bytes!("../tests/fixtures/rrweb.json");

    // Compress the payload to mimic real-world behavior. The replay processor can also handle
    // uncompressed payloads, but those happen infrequently.
    let mut compressed = Vec::new();
    let mut encoder = ZlibEncoder::new(payload.as_slice(), Compression::default());
    encoder.read_to_end(&mut compressed).unwrap();

    let mut scrubbing_config = DataScrubbingConfig::default();
    scrubbing_config.scrub_data = true;
    scrubbing_config.scrub_defaults = true;
    scrubbing_config.scrub_ip_addresses = true;
    let pii_config = scrubbing_config.pii_config_uncached().unwrap().unwrap();

    let mut scrubber = RecordingScrubber::new(usize::MAX, Some(&pii_config), None);

    c.bench_with_input(BenchmarkId::new("rrweb", 1), &compressed, |b, &_| {
        b.iter(|| {
            let mut buf = Vec::new();
            scrubber.transcode_replay(&compressed, &mut buf).ok();
            buf
        });
    });
}

criterion_group!(benches, bench_recording);
criterion_main!(benches);
