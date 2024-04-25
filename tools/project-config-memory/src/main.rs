use std::collections::BTreeMap;

use memory_stats::memory_stats;

use relay_dynamic_config::ProjectConfig;
use serde::Deserialize;

#[derive(Deserialize)]
struct Container {
    configs: BTreeMap<String, Option<OuterConfig>>,
}

#[derive(Deserialize)]
struct OuterConfig {
    config: ProjectConfig,
}

fn main() {
    let args: Vec<_> = std::env::args().collect();
    let path = &args[1];
    let json = std::fs::read_to_string(path).unwrap();

    let before = memory_stats().unwrap().physical_mem;

    let configs: Container = serde_json::from_str(&json).unwrap();

    // Make sure we parsed the right thing:
    configs
        .configs
        .values()
        .next()
        .unwrap()
        .as_ref()
        .unwrap()
        .config
        .event_retention
        .unwrap();

    let after = memory_stats().unwrap().physical_mem;
    println!("{},{}", json.len(), after - before);
}
