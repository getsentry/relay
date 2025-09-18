use std::fs;
use std::io::Write;
use std::process::Command;

use anyhow::Context;
use dircpy::CopyBuilder;
use tempfile::tempdir;

fn main() -> anyhow::Result<()> {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed=RUSTFLAGS");
    println!("cargo:rerun-if-changed=prosperoconv.version");

    // Need to explicitly check the env var here since if the `--target` flag is used the build
    // script will not receive the cfgs:
    // https://doc.rust-lang.org/nightly/cargo/reference/config.html#buildrustflags
    if !cfg!(sentry)
        && !std::env::var("CARGO_ENCODED_RUSTFLAGS").is_ok_and(|v| v.contains("--cfg\u{1f}sentry"))
    {
        return Ok(());
    }

    let desired_version = fs::read_to_string("prosperoconv.version")
        .context("Failed to read prosperoconv.version file")?
        .trim()
        .to_owned();

    let current_version = fs::read_to_string("src/prosperoconv.version").ok();

    if let Some(current_version) = current_version
        && current_version.trim() == desired_version
    {
        return Ok(());
    }

    println!("cargo:warning=Setting up PlayStation support");
    let temp_dir = tempdir().context("Failed to make temp_dir")?;
    let temp_dir_path = temp_dir.path();
    let temp_dir_str = temp_dir_path.to_string_lossy().to_string();

    if !Command::new("git")
        .args([
            "clone",
            "git@github.com:getsentry/tempest.git",
            &temp_dir_str,
            &format!("--revision={desired_version}"),
            "--depth=1",
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
                &format!("--revision={desired_version}"),
                "--depth=1",
            ])
            .status()
            .context("Failed to clone tempest revision")?;
    }

    CopyBuilder::new(temp_dir_path.join("crates/prosperoconv/src"), "src")
        .overwrite(true)
        .run()
        .context("Failed to copy files")?;

    patch_lib_rs().context("patching lib.rs")?;

    Command::new("git")
        .args(["update-index", "--skip-worktree", "src/lib.rs"])
        .status()
        .context("Failed to skip worktree")?;

    std::fs::copy("prosperoconv.version", "src/prosperoconv.version")
        .context("Failed to copy prosperoconv.version")?;

    Ok(())
}

/// Patch lib.rs to allow lints, these are already validated in the source repo,
/// but regularly break on Rust updates.
///
/// Ideally we can do this via `Cargo.toml` but setting the level to `allow` for the
/// groups does not seem to work.
fn patch_lib_rs() -> anyhow::Result<()> {
    let content = fs::read_to_string("src/lib.rs")?;
    let mut f = fs::OpenOptions::new().write(true).open("src/lib.rs")?;

    f.write_all(b"#![allow(warnings)]\n")?;
    f.write_all(b"#![allow(clippy::all)]\n")?;
    f.write_all(content.as_bytes())?;

    Ok(())
}
