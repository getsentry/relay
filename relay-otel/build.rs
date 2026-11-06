use std::path::Path;
use std::process::Command;
use std::{env, fs};

fn main() {
    let descriptors = opentelemetry_proto::DESCRIPTORS;
    let roots = [
        "opentelemetry/proto/logs/v1/logs.proto".to_owned(),
        "opentelemetry/proto/trace/v1/trace.proto".to_owned(),
    ];
    let uses = [
        "use opentelemetry_proto::tonic::logs::v1::*;",
        "use opentelemetry_proto::tonic::trace::v1::*;",
    ]
    .join("\n");

    let rendered = proto_descriptors_lib::render(&roots, &Some(uses), descriptors).unwrap();

    let out_dir = env::var("OUT_DIR").unwrap();
    let desc_path = Path::new(&out_dir).join("descriptors.gen.rs");
    fs::write(&desc_path, rendered).unwrap();
    format(&desc_path);
}

/// Runs `rustfmt` over the generated file.
fn format(out: &Path) {
    let _ = Command::new("rustfmt")
        .args(["--edition", "2024"])
        .arg(out)
        .status();
}
