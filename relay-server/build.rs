use std::env;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::process::Command;

fn git_revision_short() -> Option<String> {
    let output = Command::new("git")
        .args(["rev-parse", "--short=8", "HEAD"])
        .output()
        .ok()?;

    if !output.status.success() {
        return None;
    }

    let revision = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    (!revision.is_empty()).then_some(revision)
}

fn main() {
    let out_dir = env::var("OUT_DIR").unwrap();
    let dest_path = Path::new(&out_dir).join("constants.gen.rs");
    let mut f = File::create(dest_path).unwrap();
    let version = env::var("CARGO_PKG_VERSION").unwrap();

    writeln!(f, "pub const SERVER: &str = \"sentry-relay/{}\";", version).unwrap();
    writeln!(f, "pub const CLIENT: &str = \"sentry.relay/{}\";", version).unwrap();
    match git_revision_short() {
        Some(revision) => {
            writeln!(
                f,
                "pub const GIT_REVISION_SHORT: Option<&str> = Some(\"{revision}\");"
            )
            .unwrap();
        }
        None => {
            writeln!(f, "pub const GIT_REVISION_SHORT: Option<&str> = None;").unwrap();
        }
    }
    println!("cargo:rerun-if-changed=build.rs\n");
    println!("cargo:rerun-if-changed=Cargo.toml\n");
}
