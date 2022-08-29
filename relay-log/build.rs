use std::io;
use std::process::{Command, Stdio};

fn emit_release_var() -> Result<(), io::Error> {
    let cmd = Command::new("git")
        .args(&["rev-parse", "HEAD"])
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

fn main() {
    emit_release_var().ok();
}
