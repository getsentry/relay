#[cfg(feature = "redis")]
mod redis_impl;

#[cfg(feature = "redis")]
use redis_impl::*;

#[cfg(feature = "redis")]
criterion::criterion_group!(
    benches,
    bench_simple,
    bench_big_set_small_queries,
    bench_high_cardinality,
    bench_cache_never_full,
    bench_cache_worst_case,
);
#[cfg(feature = "redis")]
criterion::criterion_main!(benches);

#[cfg(not(feature = "redis"))]
fn main() {}
