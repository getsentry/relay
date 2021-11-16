//! TODO: Documentation

// ignore warnings in generated code from bindgen
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(deref_nullptr)]

use std::ffi::CString;

include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

/// TODO: Doc
pub fn init(dsn: Option<String>, release: Option<&str>) {
    unsafe {
        let options = sentry_options_new();

        if let Some(dsn) = dsn {
            let dsn_cstr = CString::new(dsn).unwrap();
            sentry_options_set_dsn(options, dsn_cstr.as_ptr());
        }

        if let Some(release) = release {
            let release_cstr = CString::new(release).unwrap();
            sentry_options_set_release(options, release_cstr.as_ptr());
        }

        // TODO: Set a database path via config.
        // sentry_options_set_database_path(options, "sentry-db-directory");

        sentry_init(options);
    }
}
