use std::fmt;
use std::fs;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};

use relay_general::pii::{DataScrubbingConfig, PiiProcessor};
use relay_general::processor::{process_value, SelectorSpec};
use relay_general::protocol::{Event, IpAddr};
use relay_general::store::{StoreConfig, StoreProcessor};
use relay_general::types::Annotated;

fn load_all_fixtures() -> Vec<BenchmarkInput<String>> {
    let mut rv = Vec::new();

    for entry in fs::read_dir("tests/fixtures/payloads/").unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();

        rv.push(BenchmarkInput {
            name: path.file_stem().unwrap().to_str().unwrap().to_string(),
            data: fs::read_to_string(path).unwrap(),
        });
    }

    assert!(!rv.is_empty());
    rv
}

struct BenchmarkInput<T> {
    name: String,
    data: T,
}

impl<T> BenchmarkInput<T> {
    fn new(name: impl Into<String>, data: T) -> BenchmarkInput<T> {
        BenchmarkInput {
            name: name.into(),
            data,
        }
    }
}

impl<T> fmt::Display for BenchmarkInput<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name)
    }
}

fn bench_from_value(c: &mut Criterion) {
    let mut group = c.benchmark_group("bench_from_value");

    for input in load_all_fixtures() {
        group.bench_with_input(BenchmarkId::from_parameter(&input), &input, |b, input| {
            b.iter(|| Annotated::<Event>::from_json(&input.data).expect("failed to deserialize"));
        });
    }

    group.finish();
}

fn bench_to_json(c: &mut Criterion) {
    let mut group = c.benchmark_group("bench_to_json");

    for BenchmarkInput { name, data } in load_all_fixtures() {
        let event = Annotated::<Event>::from_json(&data).expect("failed to deserialize");
        let input = BenchmarkInput { name, data: event };

        group.bench_with_input(BenchmarkId::from_parameter(&input), &input, |b, input| {
            b.iter(|| input.data.to_json().expect("failed to serialize"));
        });
    }

    group.finish();
}

fn bench_store_processor(c: &mut Criterion) {
    let config = StoreConfig {
        project_id: Some(4711),
        client_ip: Some(IpAddr("127.0.0.1".to_string())),
        client: Some("sentry.tester".to_string()),
        key_id: Some("feedface".to_string()),
        protocol_version: Some("8".to_string()),
        max_secs_in_future: Some(3600),
        max_secs_in_past: Some(2_592_000),
        enable_trimming: Some(true),
        grouping_config: None,
        is_renormalize: Some(false),
        normalize_user_agent: Some(false),
        remove_other: Some(true),
        user_agent: None,
        sent_at: None,
        received_at: None,
        breakdowns_v2: None,
    };

    let mut processor = StoreProcessor::new(config, None);

    let mut group = c.benchmark_group("bench_store_processor");
    for BenchmarkInput { name, data } in load_all_fixtures() {
        let event = Annotated::<Event>::from_json(&data).expect("failed to deserialize");
        let input = BenchmarkInput { name, data: event };

        group.bench_with_input(BenchmarkId::from_parameter(&input), &input, |b, input| {
            b.iter(|| {
                let mut event = input.data.clone();
                process_value(&mut event, &mut processor, &Default::default()).unwrap();
                event
            })
        });
    }

    group.finish();
}

fn datascrubbing_config() -> DataScrubbingConfig {
    let mut config = DataScrubbingConfig::new_disabled();
    config.exclude_fields = vec!["safe1".to_owned(), "safe2".to_owned(), "safe3".to_owned()];
    config.sensitive_fields = vec![
        "sensitive1".to_owned(),
        "sensitive2".to_owned(),
        "sensitive3".to_owned(),
    ];
    config.scrub_defaults = true;
    config.scrub_data = true;
    config.scrub_ip_addresses = true;
    config
}

fn bench_pii_stripping(c: &mut Criterion) {
    let mut group = c.benchmark_group("bench_pii_stripping");

    let datascrubbing_config = datascrubbing_config();
    let config_name = "simple_enabled";

    group.bench_with_input(
        BenchmarkId::new("convert_config", config_name),
        &datascrubbing_config,
        |b, datascrubbing_config| b.iter(|| datascrubbing_config.pii_config_uncached()),
    );

    let pii_config = datascrubbing_config.pii_config_uncached().unwrap();

    group.bench_with_input(
        BenchmarkId::new("compile_pii_config", config_name),
        &pii_config,
        |b, pii_config| b.iter(|| pii_config.compiled_uncached()),
    );

    let compiled_pii_config = pii_config.compiled_uncached();

    group.bench_with_input(
        BenchmarkId::new("new_processor", config_name),
        &compiled_pii_config,
        |b, compiled_pii_config| b.iter(|| PiiProcessor::new(compiled_pii_config)),
    );

    let mut processor = PiiProcessor::new(&compiled_pii_config);

    for BenchmarkInput { name, data } in load_all_fixtures() {
        let event = Annotated::<Event>::from_json(&data).expect("failed to deserialize");

        group.bench_with_input(
            BenchmarkId::new("run_processor", name),
            &event,
            |b, event| {
                b.iter(|| {
                    let mut event = event.clone();
                    process_value(&mut event, &mut processor, &Default::default()).unwrap();
                    event
                })
            },
        );
    }

    group.finish();
}

fn bench_parse_pii_selector(c: &mut Criterion) {
    let mut group = c.benchmark_group("bench_parse_pii_selector");

    let mut bench = |input: BenchmarkInput<&str>| {
        group.bench_with_input(BenchmarkId::from_parameter(&input), &input, |b, input| {
            b.iter(|| input.data.parse::<SelectorSpec>().unwrap())
        });
    };

    bench(BenchmarkInput::new("complex_legacy", "(($string | $number | $array) & (~(debug_meta.** | $frame.filename | $frame.abs_path | $logentry.formatted)))"));

    group.finish();
}

criterion_group!(
    benches,
    bench_from_value,
    bench_to_json,
    bench_store_processor,
    bench_pii_stripping,
    bench_parse_pii_selector,
);
criterion_main!(benches);
