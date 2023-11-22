#![allow(missing_docs)]

use std::env;
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::Path;
use std::process::{Command, Stdio};

fn emit_release_var() -> Result<(), io::Error> {
    let cmd = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .stderr(Stdio::inherit())
        .output()?;

    if !cmd.status.success() {
        return Err(io::Error::new(
            io::ErrorKind::Other,
            format!("`git rev-parse' failed: {}", cmd.status),
        ));
    }

    let version = std::env::var("CARGO_PKG_VERSION").unwrap();
    let revision = String::from_utf8_lossy(&cmd.stdout);
    println!("cargo:rustc-env=RELAY_RELEASE=relay@{version}+{revision}");

    Ok(())
}

fn list_crates() -> Vec<String> {
    let mut crates = Vec::new();

    for result in fs::read_dir("../").unwrap() {
        let entry = result.unwrap();

        if !entry.file_type().unwrap().is_dir() {
            continue;
        }

        if let Some(s) = entry.file_name().to_str() {
            if s.starts_with("relay") {
                crates.push(s.to_owned());
            }
        }
    }

    crates
}

fn emit_crate_list() -> Result<(), io::Error> {
    let crates = list_crates();

    let out_dir = env::var("OUT_DIR").unwrap();
    let dest_path = Path::new(&out_dir).join("constants.gen.rs");
    let mut f = File::create(dest_path).unwrap();

    write!(f, "const CRATE_NAMES: &[&str] = &[")?;
    for name in &crates {
        write!(f, "\"{name}\",")?;
    }
    writeln!(f, "];")?;

    Ok(())
}

fn main() {
    emit_release_var().ok();
    emit_crate_list().unwrap();
}
