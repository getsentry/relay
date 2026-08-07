//! Generates the `MessageDesc` tables which bound protobuf decoding.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result, bail};
use clap::Parser;
use convert_case::{Case, Casing};
use prost::Message;
use prost_types::field_descriptor_proto::Type;
use prost_types::{DescriptorProto, FileDescriptorSet};

#[derive(Debug, Parser)]
#[command(about = "Generates descriptor tables for bounded protobuf decoding")]
struct Cli {
    /// Root directory of a set of protos to compile.
    #[arg(long)]
    proto_root: PathBuf,

    /// Where to write the generated Rust.
    #[arg(long)]
    out: PathBuf,

    // Rust 'use'-statements to include in the generated file;
    #[arg(long)]
    use_stmts: Option<String>,

    /// Path to the specific proto file to process.
    file: PathBuf,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    let rendered = run(&cli.proto_root, &cli.out, &cli.use_stmts, &cli.file)?;

    fs::write(&cli.out, rendered).with_context(|| format!("cannot write {}", cli.out.display()))?;
    format(&cli.out)?;
    Ok(())
}

fn run(
    proto_root: &Path,
    out: &Path,
    use_stmts: &Option<String>,
    file: &Path,
) -> Result<String, anyhow::Error> {
    let descriptor_set = compile(proto_root, file)?;
    let descriptor_set =
        FileDescriptorSet::decode(descriptor_set.as_slice()).context("malformed descriptor set")?;
    let (root_messages, messages) = extract_roots_and_messages(file, &descriptor_set)?;
    let reachable = walk(messages)?;
    render_to_rust(proto_root, out, use_stmts, file, &reachable, &root_messages)
}

/// Runs `rustfmt` over the generated file.
fn format(out: &Path) -> Result<()> {
    let status = Command::new("rustfmt")
        .args(["--edition", "2024"])
        .arg(out)
        .status()
        .context("cannot run rustfmt; is it installed and on PATH?")?;

    if !status.success() {
        bail!("rustfmt failed with {status}");
    }

    Ok(())
}

/// Compiles the entry protos into a `FileDescriptorSet` using the installed `protoc`.
fn compile(proto_root: &Path, entry_proto: &Path) -> Result<Vec<u8>> {
    // Generate a unique-enough name; this can be run concurrently in tests.
    let out = std::env::temp_dir().join(format!(
        "proto-descriptor-set-{}-{}.pb",
        entry_proto
            .file_stem()
            .unwrap_or_default()
            .to_string_lossy(),
        std::process::id()
    ));

    let status = Command::new("protoc")
        .arg(format!("--descriptor_set_out={}", out.display()))
        .arg("--include_imports")
        .arg("-I")
        .arg(proto_root)
        .arg(entry_proto)
        .status()
        .context("cannot run protoc; is it installed and on PATH?")?;

    if !status.success() {
        bail!("protoc failed with {status}");
    }

    fs::read(&out).with_context(|| format!("cannot read {}", out.display()))
}

// Roots are the top-level messages in the specified file, but we'll still transitively pull
// in other messages that are nested.
fn extract_roots_and_messages<'a>(
    root_file: &Path,
    descriptor_set: &'a FileDescriptorSet,
) -> Result<(Vec<String>, BTreeMap<String, &'a DescriptorProto>)> {
    let mut messages = BTreeMap::new();
    let mut root_types = vec![];
    for file in &descriptor_set.file {
        let prefix = match file.package() {
            "" => String::new(),
            package => format!(".{package}"),
        };

        for message in &file.message_type {
            collect(&prefix, message, &mut messages)?;

            if file.name() == root_file {
                root_types.push(message.name().to_owned());
            }
        }
    }

    Ok((root_types, messages))
}

// Adds `message` and everything transitively nested inside it to `messages`.
fn collect<'a>(
    prefix: &str,
    message: &'a DescriptorProto,
    messages: &mut BTreeMap<String, &'a DescriptorProto>,
) -> Result<()> {
    let full_name = format!("{prefix}.{}", message.name());

    if messages.insert(full_name.clone(), message).is_some() {
        bail!("{full_name} is defined twice in the descriptor set");
    }

    for nested in &message.nested_type {
        collect(&full_name, nested, messages)?;
    }

    Ok(())
}

struct ProcessedMessage<'a> {
    // The fully qualified proto name
    full_name: String,
    // The short name of this message
    ident: String,
    // `(tag, fully qualified name)` for every field holding a nested message, ordered by tag.
    nested: Vec<(u32, &'a str)>,
}

fn walk<'a>(messages: BTreeMap<String, &'a DescriptorProto>) -> Result<Vec<ProcessedMessage<'a>>> {
    let mut processed = Vec::new();
    let mut queue: Vec<String> = messages.keys().map(|k| k.to_owned()).collect();

    while let Some(full_name) = queue.pop() {
        let (full_name, message) = messages
            .get_key_value(&full_name)
            .map(|(name, message)| (name.as_str(), *message))
            .with_context(|| format!("{full_name} is not in the descriptor set"))?;

        let mut nested = Vec::new();
        for field in &message.field {
            // Bail loudly on this for now; not supported in proto3.
            if field.r#type() == Type::Group {
                bail!(
                    "{full_name}.{} is a group, which this generator does not describe",
                    field.name()
                );
            }

            if field.r#type() != Type::Message {
                continue;
            }

            let type_name = field.type_name();

            let target = messages.get(type_name).with_context(|| {
                format!(
                    "{full_name}.{} refers to {type_name}, which is not in the descriptor set - \
                     was it built without --include_imports?",
                    field.name()
                )
            })?;

            // Maps not supported at the moment.
            if target.options.as_ref().is_some_and(|o| o.map_entry()) {
                bail!(
                    "{full_name}.{} is a map, which this generator does not describe",
                    field.name()
                );
            }

            let tag = u32::try_from(field.number())
                .with_context(|| format!("{full_name}.{} has a negative tag", field.name()))?;

            nested.push((tag, type_name));
        }

        nested.sort_unstable();

        processed.push(ProcessedMessage {
            full_name: full_name.trim_start_matches('.').to_owned(),
            ident: ident(full_name),
            nested,
        });
    }

    // Sorting by name keeps regeneration diffs empty when nothing has actually changed.
    processed.sort_unstable_by(|a, b| a.full_name.cmp(&b.full_name));

    let mut idents = BTreeMap::new();
    for message in &processed {
        if let Some(other) = idents.insert(message.ident.clone(), message.full_name.clone()) {
            bail!(
                "{} and {} both map to the static {}",
                other,
                message.full_name,
                message.ident
            );
        }
    }

    Ok(processed)
}

// Derives the name of a static from a fully qualified proto name. For example,
// `.opentelemetry.proto.trace.v1.Span.Event` becomes `SPAN_EVENT`.
fn ident(full_name: &str) -> String {
    // Message names start with an uppercase letter and package segments do not, so the first
    // uppercase segment is where the message path begins.
    full_name
        .split('.')
        .skip_while(|segment| !segment.starts_with(|c: char| c.is_ascii_uppercase()))
        .map(|s| s.to_case(Case::UpperSnake))
        .collect::<Vec<_>>()
        .join("_")
}

fn render_to_rust(
    proto_root: &Path,
    out_path: &Path,
    use_stmts: &Option<String>,
    file: &Path,
    reachable: &[ProcessedMessage<'_>],
    root_types: &Vec<String>,
) -> Result<String> {
    let mut out = String::new();

    writeln!(
        out,
        "// @generated by tools/proto-descriptors - DO NOT EDIT."
    )?;
    writeln!(out, "//")?;
    writeln!(out, "// To regenerate, invoke:")?;
    writeln!(out, "// cargo run -p proto-descriptors -- \\")?;
    writeln!(out, "//   --proto-root {} \\", proto_root.to_string_lossy())?;

    if let Some(use_stmts) = use_stmts {
        writeln!(out, "//   --use-stmts \"{}\" \\", use_stmts)?;
    }

    writeln!(out, "//   --out {} \\", out_path.to_string_lossy())?;
    writeln!(out, "//   {}", file.to_string_lossy())?;
    writeln!(out)?;
    writeln!(out, "use relay_serialization::prost::Error;")?;
    writeln!(out, "use relay_serialization::prost::MessageDesc;")?;
    writeln!(out, "use relay_serialization::prost::decode;")?;

    if let Some(stmts) = use_stmts {
        writeln!(out, "{}", stmts)?;
    }

    writeln!(
        out,
        "pub trait Decodable {{ fn decode_bounded(buf: &[u8], max_ops: usize) -> Result<Self, Error> where
        Self: Sized; }}"
    )?;

    for message in reachable {
        let nested = if !message.nested.is_empty() {
            let mut nested = "".to_owned();
            for (tag, type_name) in &message.nested {
                nested += &format!("        ({tag}, &{}),", ident(type_name));
            }
            nested
        } else {
            "".to_owned()
        };

        writeln!(out)?;
        writeln!(out, "/// `{}`", message.full_name)?;
        writeln!(
            out,
            "pub static {}: MessageDesc = MessageDesc {{",
            message.ident
        )?;
        writeln!(out, "    name: \"{}\",", message.full_name)?;
        writeln!(out, "    nested: &[{}],", nested)?;
        writeln!(out, "}};")?;
    }

    for typ in root_types {
        writeln!(
            out,
            "
            impl Decodable for {} {{\
                fn decode_bounded(buf: &[u8], max_ops: usize) -> Result<Self, Error> {{ decode(buf, &{}, max_ops) }}\
            }}",
            typ,
            typ.to_case(Case::UpperSnake)
        )?;
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    const PROTO_ROOT: &str = "tests/fixtures/protos";
    const TREE_PROTO: &str = "relay/test/v1/tree.proto";

    fn compile_fixture(file: &str, proto_root: &str) -> (FileDescriptorSet, PathBuf) {
        let file = PathBuf::from(file);
        let bytes = compile(Path::new(proto_root), &file).expect("cannot compile fixture");
        let set = FileDescriptorSet::decode(bytes.as_slice()).expect("malformed descriptor set");

        (set, file)
    }

    fn descriptors(file: &str, proto_root: &str) -> Result<(Vec<String>, Vec<String>)> {
        let (set, file) = compile_fixture(file, proto_root);
        let (root_messages, messages) = extract_roots_and_messages(&file, &set)?;
        let reachable = walk(messages)?;

        Ok((
            root_messages,
            reachable.iter().map(|m| m.full_name.clone()).collect(),
        ))
    }

    #[test]
    fn test_render_matches_fixture() {
        let cli = Cli {
            proto_root: PathBuf::from(PROTO_ROOT),
            out: PathBuf::from("tests/fixtures/descriptors.rs"),
            use_stmts: Some("use super::proto::relay::test::v1::*;".to_owned()),
            file: PathBuf::from(TREE_PROTO),
        };

        let rendered = run(&cli.proto_root, &cli.out, &cli.use_stmts, &cli.file).unwrap();
        let out_path =
            std::env::temp_dir().join(format!("proto-descriptors-{}.rs", std::process::id()));
        fs::write(&out_path, rendered).expect("cannot write rendered output");
        format(&out_path).unwrap();
        let rendered = std::fs::read_to_string(&out_path).unwrap();

        let fixture = fs::read_to_string(&cli.out).unwrap();

        assert_eq!(
            rendered,
            fixture,
            "regenerate with the command in the header of {}",
            cli.out.display()
        );
    }

    #[test]
    fn test_walk_rejects_maps() {
        let error = descriptors("relay/test/v1/maps.proto", PROTO_ROOT).unwrap_err();

        assert_eq!(
            error.to_string(),
            ".relay.test.v1.WithMap.labels is a map, which this generator does not describe"
        );
    }

    #[test]
    fn test_walk_rejects_groups() {
        let error = descriptors("relay/test/v1/groups.proto", PROTO_ROOT).unwrap_err();

        assert_eq!(
            error.to_string(),
            ".relay.test.v1.WithGroup.inner is a group, which this generator does not describe"
        );
    }

    #[test]
    fn test_walk_rejects_colliding_idents() {
        // `FooBar` and `Foo.Bar` both flatten to `FOO_BAR`, which would emit the static twice.
        let error = descriptors("relay/test/v1/collision.proto", PROTO_ROOT).unwrap_err();

        assert_eq!(
            error.to_string(),
            "relay.test.v1.Foo.Bar and relay.test.v1.FooBar both map to the static FOO_BAR"
        );
    }

    #[test]
    fn test_ident_drops_package_and_snake_cases() {
        assert_eq!(ident(".opentelemetry.proto.logs.v1.LogsData"), "LOGS_DATA");
        assert_eq!(
            ident(".opentelemetry.proto.common.v1.AnyValue"),
            "ANY_VALUE"
        );
        assert_eq!(
            ident(".opentelemetry.proto.common.v1.KeyValueList"),
            "KEY_VALUE_LIST"
        );
        assert_eq!(
            ident(".opentelemetry.proto.trace.v1.Span.Event"),
            "SPAN_EVENT"
        );
        assert_eq!(
            ident(".opentelemetry.proto.resource.v1.Resource"),
            "RESOURCE"
        );
    }
}
