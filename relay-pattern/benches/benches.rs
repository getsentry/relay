use criterion::measurement::WallTime;
use criterion::{criterion_group, criterion_main, BenchmarkGroup, Criterion};

use relay_pattern::Pattern;

fn bench(group: &mut BenchmarkGroup<'_, WallTime>, haystack: &str, needle: &str) {
    group.bench_function("pattern", |b| {
        let pattern = Pattern::new(needle).unwrap();
        b.iter(|| assert!(pattern.is_match(haystack)))
    });
}

fn literal_match(c: &mut Criterion) {
    let mut group = c.benchmark_group("prefix_match");

    const HAYSTACK: &str = "foobarwithacrazylongprefixandanditactuallymatches";

    bench(&mut group, HAYSTACK, HAYSTACK);

    group.finish();
}

fn prefix_match(c: &mut Criterion) {
    let mut group = c.benchmark_group("prefix_match");

    const HAYSTACK: &str = "foobarwithacrazylongprefixandanditactuallymatches";
    const NEEDLE: &str = "foobarwithacrazylongprefixand*";

    bench(&mut group, HAYSTACK, NEEDLE);

    group.finish();
}

fn suffix_match(c: &mut Criterion) {
    let mut group = c.benchmark_group("suffix_match");

    const HAYSTACK: &str = "foobarwithacrazylongprefixandanditactuallymatches";
    const NEEDLE: &str = "*andanditactuallymatches";

    bench(&mut group, HAYSTACK, NEEDLE);

    group.finish();
}

fn contains_match(c: &mut Criterion) {
    let mut group = c.benchmark_group("contains_match");

    const HAYSTACK: &str = "foobarwithacrazylongprefixandanditactuallymatches";
    const NEEDLE: &str = "*withacrazylongprefixand*";

    bench(&mut group, HAYSTACK, NEEDLE);

    group.finish();
}

fn wildcard_match(c: &mut Criterion) {
    let mut group = c.benchmark_group("contains_match");

    const HAYSTACK: &str = "foobarwithacrazylongprefixandanditactuallymatches";
    const NEEDLE: &str = "*";

    bench(&mut group, HAYSTACK, NEEDLE);

    group.finish();
}

criterion_group!(
    benches,
    literal_match,
    prefix_match,
    suffix_match,
    contains_match,
    wildcard_match,
);
criterion_main!(benches);
