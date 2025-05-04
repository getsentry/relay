use std::fs;
use std::path::Path;
use std::process::Command;

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed=RUSTFLAGS");

    if !cfg!(playstation) {
        return;
    }

    if Path::new("src/event.rs").exists() {
        println!("cargo:warning=PlayStation files already present, skipping setup");
        return;
    }
    println!("cargo:warning=Setting up PlayStation support");

    let temp_dir = "/tmp/tempest";
    if !Command::new("git")
        .args(["clone", "git@github.com:getsentry/tempest.git", temp_dir])
        .status()
        .map(|x| x.success())
        .unwrap_or(false)
    {
        Command::new("git")
            .args([
                "clone",
                "https://github.com/getsentry/tempest.git",
                temp_dir,
            ])
            .status()
            .expect("Failed to clone tempest repository");
    }

    Command::new("git")
        .args(["checkout", "tobias-wilfert/feat/test"])
        .current_dir(temp_dir)
        .status()
        .expect("Failed to checkout branch");

    Command::new("cp")
        .args([
            "-rf",
            &format!("{}/crates/prosperoconv/src/.", temp_dir),
            "src/",
        ])
        .status()
        .expect("Failed to copy files");

    fs::remove_dir_all(temp_dir).expect("Failed to remove temp directory");
}
