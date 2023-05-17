#[doc(hidden)]
pub fn __init_test(module_path: &'static str) {
    let crate_name = module_path.split("::").next().unwrap();

    // TODO(ja): Fix this somehow.
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
