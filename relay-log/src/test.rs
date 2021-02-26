use log::LevelFilter;

#[doc(hidden)]
pub fn __init_test(module_path: &'static str) {
    let crate_name = module_path.split("::").next().unwrap();

    env_logger::builder()
        .filter(Some(crate_name), LevelFilter::Trace)
        .is_test(true)
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
        $crate::__init_test(::std::module_path!());
    };
}
