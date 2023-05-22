use tracing_subscriber::EnvFilter;

// Import CRATE_NAMES, which lists all crates in the workspace.
include!(concat!(env!("OUT_DIR"), "/constants.gen.rs"));

#[doc(hidden)]
pub fn __init_test() {
    let mut env_filter = EnvFilter::new("ERROR");

    // Add all internal modules with maximum log-level.
    for name in CRATE_NAMES {
        env_filter = env_filter.add_directive(format!("{name}=TRACE").parse().unwrap());
    }

    tracing_subscriber::fmt::fmt()
        .with_env_filter(env_filter)
        .with_target(true)
        .with_test_writer()
        .compact()
        .try_init()
        .ok();
}

/// Initialize the logger for testing.
///
/// This logs to the stdout registered by the Rust test runner, and only captures logs from the
/// calling crate.
///
/// # Example
///
/// ```
/// relay_log::init_test!();
/// ```
#[macro_export]
macro_rules! init_test {
    () => {
        $crate::__init_test();
    };
}
