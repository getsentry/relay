use std::path::{Path, PathBuf};
use std::process::Command;

fn main() {
    // sentry-native is only built on macOS and Linux.
    let target_os = std::env::var("CARGO_CFG_TARGET_OS").unwrap();
    if !matches!(target_os.as_str(), "macos" | "linux") {
        return; // allow building with --all-features, fail during runtime
    }

    if !Path::new("sentry-native/.git").exists() {
        let _ = Command::new("git")
            .args(["submodule", "update", "--init", "--recursive"])
            .status();
    }

    let destination = cmake::Config::new("sentry-native")
        // we never need a debug build of sentry-native
        .profile("RelWithDebInfo")
        // always build breakpad regardless of platform defaults
        .define("SENTRY_BACKEND", "breakpad")
        // inject a custom transport instead of curl
        .define("SENTRY_TRANSPORT", "none")
        // build a static library
        .define("BUILD_SHARED_LIBS", "OFF")
        // disable additional targets
        .define("SENTRY_BUILD_TESTS", "OFF")
        .define("SENTRY_BUILD_EXAMPLES", "OFF")
        // cmake sets the install dir to `lib64` on some platforms. since we are not installing to
        // `/usr` or `/usr/local`, we can choose a stable directory for the link-search below.
        .define("CMAKE_INSTALL_LIBDIR", "lib")
        .build();

    println!(
        "cargo:rustc-link-search=native={}",
        destination.join("lib").display()
    );
    println!("cargo:rustc-link-lib=static=breakpad_client");
    println!("cargo:rustc-link-lib=static=sentry");

    // Link the C++ standard library that breakpad/sentry-native depend on. This must come *after*
    // the static libs above so the linker can resolve their symbols (static archive linking is
    // order-sensitive). On Linux we link it statically so the binary runs on minimal base images
    // that don't ship `libstdc++.so.6` (e.g. the distroless `static` image).
    match target_os.as_str() {
        "macos" => println!("cargo:rustc-link-lib=dylib=c++"),
        "linux" => {
            // `static=stdc++` makes rustc resolve `libstdc++.a` itself, but it lives in a
            // gcc-internal, arch/version-specific directory that is not on rustc's default link
            // search path (only the `cc` driver knows it). Ask the compiler where it is.
            let cc = std::env::var("CC").unwrap_or_else(|_| "cc".to_string());
            let path = Command::new(&cc)
                .arg("-print-file-name=libstdc++.a")
                .output()
                .ok()
                .and_then(|o| String::from_utf8(o.stdout).ok());
            // `-print-file-name` echoes the bare filename back when it can't resolve it.
            if let Some(dir) = path
                .as_deref()
                .map(str::trim)
                .filter(|p| *p != "libstdc++.a")
                .and_then(|p| Path::new(p).parent())
            {
                println!("cargo:rustc-link-search=native={}", dir.display());
            }
            println!("cargo:rustc-link-lib=static=stdc++");
        }
        _ => unreachable!(),
    }

    let bindings = bindgen::Builder::default()
        .header("sentry-native/include/sentry.h")
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()))
        .generate()
        .expect("Unable to generate bindings");

    let out_dir = PathBuf::from(std::env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_dir.join("bindings.rs"))
        .expect("Couldn't write bindings");
}
