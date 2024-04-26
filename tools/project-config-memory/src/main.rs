use std::alloc::System;
use std::collections::BTreeMap;

use memory_stats::memory_stats;

use relay_dynamic_config::ProjectConfig;
use serde::Deserialize;
use stats_alloc::{Region, StatsAlloc, INSTRUMENTED_SYSTEM};

#[derive(Deserialize)]
struct Container {
    configs: BTreeMap<String, Option<OuterConfig>>,
}

#[derive(Deserialize)]
struct OuterConfig {
    config: ProjectConfig,
}

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

fn main() {
    let mut region = Region::new(GLOBAL);

    let args: Vec<_> = std::env::args().collect();
    let path = &args[1];
    let json = std::fs::read_to_string(path).unwrap();

    let before = memory_stats().unwrap().physical_mem;

    region.reset();
    let configs: Container = serde_json::from_str(&json).unwrap();
    println!("Stats: {:#?}", region.change());

    let after = memory_stats().unwrap().physical_mem;
    let diff = after - before;

    println!(
        "{},{},{}",
        json.len(),
        diff,
        diff as f64 / json.len() as f64
    );
    drop(configs);
}
