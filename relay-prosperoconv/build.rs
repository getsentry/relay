use std::fs;
use std::process::Command;

use dircpy::CopyBuilder;
use tempfile::tempdir;

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed=RUSTFLAGS");
    println!("cargo:rerun-if-changed=prosperoconv.version");

    // Need to explicitly check the env var here since if the `--target` flag is used the build
    // script will not receive the cfgs:
    // https://doc.rust-lang.org/nightly/cargo/reference/config.html#buildrustflags
    if !cfg!(sentry)
        && !std::env::var("CARGO_ENCODED_RUSTFLAGS").is_ok_and(|v| v.contains("--cfg\u{1f}sentry"))
    {
        println!("cargo:warning=Sentry flag not set");
        return;
    }

    match fs::read_to_string("src/lib.rs") {
        Ok(content) if !content.trim().is_empty() => {
            println!("cargo:warning=PlayStation files already present, skipping setup");
            return;
        }
        _ => {
            println!("cargo:warning=Setting up PlayStation support");
        }
    }

    let temp_dir = tempdir().expect("Failed to make temp_dir");
    let temp_dir_path = temp_dir.path();
    let temp_dir_str = temp_dir_path.to_string_lossy().to_string();

    let version = fs::read_to_string("prosperoconv.version")
        .expect("Failed to read prosperoconv.version file")
        .trim()
        .to_string();

    if !Command::new("git")
        .args([
            "clone",
            "git@github.com:getsentry/tempest.git",
            &temp_dir_str,
        ])
        .status()
        .map(|x| x.success())
        .unwrap_or(false)
    {
        Command::new("git")
            .args([
                "clone",
                "https://github.com/getsentry/tempest.git",
                &temp_dir_str,
            ])
            .status()
            .expect("Failed to clone tempest repository");
    }

    Command::new("git")
        .args(["checkout", &version])
        .current_dir(temp_dir_path)
        .status()
        .expect("Failed to checkout branch");

    CopyBuilder::new(temp_dir_path.join("crates/prosperoconv/src"), "src")
        .overwrite(true)
        .run()
        .expect("Failed to copy files");

    Command::new("git")
        .args(["update-index", "--skip-worktree", "src/lib.rs"])
        .status()
        .expect("Failed to skip worktree");

    println!("cargo:warning=Finished setting up");
}
