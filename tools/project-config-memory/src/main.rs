use memory_stats::memory_stats;

use relay_dynamic_config::ProjectConfig;

fn main() {
    let args: Vec<_> = std::env::args().collect();
    let path = &args[1];
    let json = std::fs::read_to_string(path).unwrap();
    println!("JSON size: {}", json.len());

    let before = memory_stats().unwrap().physical_mem;
    println!("Physical memory usage before parsing: {before}",);

    let _config: ProjectConfig = serde_json::from_str(&json).unwrap();

    let after = memory_stats().unwrap().physical_mem;
    println!("Physical memory usage after parsing: {after}",);
    println!("Parsed size: {:?}", after - before);
}
