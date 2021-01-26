use relay_ffi::with_last_error;

#[relay_ffi::catch_unwind]
unsafe fn returns_unit() {}

#[relay_ffi::catch_unwind]
unsafe fn returns_int() -> i32 {
    "42".parse()?
}

#[relay_ffi::catch_unwind]
unsafe fn returns_error() -> i32 {
    "invalid".parse()?
}

#[relay_ffi::catch_unwind]
unsafe fn panics() {
    panic!("this is fine");
}

#[test]
fn test_unit() {
    unsafe { returns_unit() }
    assert!(with_last_error(|_| ()).is_none())
}

#[test]
fn test_ok() {
    unsafe { returns_int() };
    assert!(with_last_error(|_| ()).is_none())
}

#[test]
fn test_error() {
    unsafe { returns_error() };
    assert_eq!(
        with_last_error(|e| e.to_string()).as_deref(),
        Some("invalid digit found in string")
    )
}

#[test]
fn test_panics() {
    relay_ffi::set_panic_hook();

    unsafe { panics() };

    let last_error = with_last_error(|e| e.to_string()).expect("returned error");
    assert!(last_error.starts_with("panic: thread \'test_panics\' panicked with \'this is fine\'"));
}
