use anyhow::{Context, Result, bail};
use convert_case::{Case, Casing};
use prost::Message;
use prost_types::field_descriptor_proto::Type;
use prost_types::{DescriptorProto, FileDescriptorSet};
use std::collections::BTreeMap;
use std::fmt::Write as _;

// Runs the rendering process, given the supplied descriptor set, and a set of root protos from
// which to start the traversal.  Returns the String of the rendered file, or an error.
pub fn render(
    root_files: &[String],
    use_stmts: &Option<String>,
    descriptor_set_bytes: &[u8],
) -> Result<String, anyhow::Error> {
    let descriptor_set =
        FileDescriptorSet::decode(descriptor_set_bytes).context("malformed descriptor set")?;

    let (root_messages, messages) = extract_roots_and_messages(root_files, &descriptor_set)?;
    let reachable = walk(messages)?;
    let rendered = render_to_rust(use_stmts, &reachable, &root_messages)?;

    Ok(rendered)
}

// Roots are the top-level messages in the specified file, but we'll still transitively pull
// in other messages that are nested.  Ultimately, we only want to implement Decodable for root
// types (rather than everything transitive, which is a lot.)
fn extract_roots_and_messages<'a>(
    root_files: &[String],
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

            if root_files.contains(&file.name().to_owned()) {
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

// The actual rendering function producing the generated file.
fn render_to_rust(
    use_stmts: &Option<String>,
    reachable: &[ProcessedMessage<'_>],
    root_types: &Vec<String>,
) -> Result<String> {
    let mut out = String::new();

    writeln!(
        out,
        "//! @generated by tools/proto-descriptors - DO NOT EDIT."
    )?;

    writeln!(out)?;
    writeln!(out, "use relay_serialization::prost::Error;")?;
    writeln!(out, "use relay_serialization::prost::MessageDesc;")?;
    writeln!(out, "use relay_serialization::prost::decode;")?;

    if let Some(stmts) = use_stmts {
        writeln!(out, "{}", stmts)?;
    }

    writeln!(
        out,
        "/// Provides a bounded decode function for any implementor."
    )?;
    writeln!(out, "pub trait Decodable {{")?;
    writeln!(
        out,
        "/// Attempts to decode the provided proto bytes within the specified number of ops."
    )?;
    writeln!(
        out,
        "fn decode_bounded(buf: &[u8], max_ops: usize) -> Result<Self, Error> where Self: Sized; }}"
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

    // We only implement Deocodable for root_types.
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

    use std::path::{Path, PathBuf};
    use std::process::Command;

    const TREE: &str = "relay/test/v1/tree.proto";
    const MAPS: &str = "relay/test/v1/maps.proto";
    const GROUPS: &str = "relay/test/v1/groups.proto";
    const COLLISION: &str = "relay/test/v1/collision.proto";

    fn proto_dir() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/protos")
    }

    /// Compiles `protos` with `protoc`, the way the descriptor sets fed to [`render`] in
    /// production are built. Requires `protoc` on `PATH`.
    fn descriptor_set(protos: &[&str], include_imports: bool) -> Vec<u8> {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("descriptors.binpb");

        let mut command = Command::new("protoc");
        command
            .arg("-I")
            .arg(proto_dir())
            .arg(format!("--descriptor_set_out={}", out.display()));

        if include_imports {
            command.arg("--include_imports");
        }

        let output = command.args(protos).output().unwrap_or_else(|err| {
            panic!("failed to run protoc - is it installed and on PATH? ({err})")
        });

        assert!(
            output.status.success(),
            "protoc failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );

        std::fs::read(&out).unwrap()
    }

    /// Renders `protos`, treating every one of them as a root file.
    fn render_fixture(protos: &[&str]) -> Result<String> {
        let roots = protos.iter().map(|p| (*p).to_owned()).collect::<Vec<_>>();
        render(&roots, &None, &descriptor_set(protos, true))
    }

    /// Drops all whitespace, so assertions do not depend on the generated file\'s (pre-rustfmt)
    /// layout.
    fn squeeze(s: &str) -> String {
        s.chars().filter(|c| !c.is_whitespace()).collect()
    }

    /// The `MessageDesc` static [`render_to_rust`] is expected to emit for one message.
    fn desc(ident: &str, name: &str, nested: &[(u32, &str)]) -> String {
        let nested: String = nested
            .iter()
            .map(|(tag, ident)| format!("({tag}, &{ident}),"))
            .collect();

        squeeze(&format!(
            "pub static {ident}: MessageDesc = MessageDesc {{ \
                 name: \"{name}\", \
                 nested: &[{nested}], \
             }};"
        ))
    }

    /// The `Decodable` impl [`render_to_rust`] is expected to emit for one root type.
    fn decodable_impl(typ: &str, ident: &str) -> String {
        squeeze(&format!(
            "impl Decodable for {typ} {{ \
                 fn decode_bounded(buf: &[u8], max_ops: usize) -> Result<Self, Error> {{ \
                     decode(buf, &{ident}, max_ops) \
                 }} \
             }}"
        ))
    }

    /// The idents of the statics, in the order they appear in the generated file.
    fn static_idents(rendered: &str) -> Vec<&str> {
        rendered
            .lines()
            .filter_map(|line| line.trim().strip_prefix("pub static "))
            .filter_map(|line| line.split(':').next())
            .collect()
    }

    #[test]
    fn test_ident_strips_package_and_flattens_nesting() {
        assert_eq!(
            ident(".opentelemetry.proto.trace.v1.Span.Event"),
            "SPAN_EVENT"
        );
        assert_eq!(ident(".relay.test.v1.Tree.Node"), "TREE_NODE");
        assert_eq!(ident(".relay.test.v1.Leaf"), "LEAF");
        // Word boundaries inside a single message name survive.
        assert_eq!(ident(".relay.test.v1.FooBar"), "FOO_BAR");
        // A message in the unnamed package still resolves, since the search stops at the first
        // uppercase segment either way.
        assert_eq!(ident(".Leaf"), "LEAF");
    }

    #[test]
    fn test_descriptors_describe_every_nested_message() {
        let rendered = squeeze(&render_fixture(&[TREE]).unwrap());

        // Only message-typed fields are described; `Leaf`\'s strings, scalars, packed
        // `repeated uint64`, `bytes` and enum all carry their own length or are fixed width, so
        // they cost a bounded number of ops and do not need a descriptor. Tag 9 is the `Leaf`
        // arm of the `oneof`, which recurses.
        assert!(rendered.contains(&desc("LEAF", "relay.test.v1.Leaf", &[(9, "LEAF")])));

        // Repeated and singular message fields are described identically - the bound is per
        // occurrence - so `root` and `forest` both point at `TREE_NODE`. Tag 3 resolves into the
        // imported file.
        assert!(rendered.contains(&desc(
            "TREE",
            "relay.test.v1.Tree",
            &[(1, "TREE_NODE"), (2, "TREE_NODE"), (3, "METADATA")],
        )));

        // A type nested inside another message is flattened into a top-level static, and
        // recursion through it is just a self-reference.
        assert!(rendered.contains(&desc(
            "TREE_NODE",
            "relay.test.v1.Tree.Node",
            &[(2, "LEAF"), (3, "TREE_NODE")],
        )));

        // Transitively reachable messages from the imported file are described too, even though
        // that file is not a root.
        assert!(rendered.contains(&desc(
            "METADATA",
            "relay.test.common.v1.Metadata",
            &[(2, "TAG")],
        )));
        assert!(rendered.contains(&desc("TAG", "relay.test.common.v1.Tag", &[])));
    }

    #[test]
    fn test_decodable_is_implemented_for_root_types_only() {
        let rendered = render_fixture(&[TREE]).unwrap();
        let squeezed = squeeze(&rendered);

        // `Tree` and `Leaf` are the top-level messages of the root file.
        assert!(squeezed.contains(&decodable_impl("Tree", "TREE")));
        assert!(squeezed.contains(&decodable_impl("Leaf", "LEAF")));

        // `Tree.Node` is nested rather than top-level, and `Metadata`/`Tag` live in an imported
        // file - all three get a descriptor but no impl.
        assert_eq!(rendered.matches("impl Decodable for").count(), 2);
    }

    #[test]
    fn test_no_roots_renders_descriptors_without_impls() {
        let rendered = render(&[], &None, &descriptor_set(&[TREE], true)).unwrap();

        assert!(rendered.contains("pub static TREE: MessageDesc"));
        assert!(!rendered.contains("impl Decodable for"));
    }

    #[test]
    fn test_statics_are_ordered_by_proto_name() {
        let rendered = render_fixture(&[TREE]).unwrap();

        // Sorting by fully qualified name is what keeps regeneration diffs empty, so it is part
        // of the contract rather than an accident of traversal order.
        assert_eq!(
            static_idents(&rendered),
            ["METADATA", "TAG", "LEAF", "TREE", "TREE_NODE"]
        );
    }

    #[test]
    fn test_preamble_and_use_statements() {
        let bytes = descriptor_set(&[TREE], true);
        let roots = vec![TREE.to_owned()];

        let rendered = render(&roots, &None, &bytes).unwrap();
        assert!(rendered.starts_with("//! @generated by tools/proto-descriptors - DO NOT EDIT.\n"));
        assert!(rendered.contains("use relay_serialization::prost::MessageDesc;"));
        assert!(!rendered.contains("use relay_test_protos"));

        // Callers supply the imports that bring the root types into scope.
        let uses = Some("use relay_test_protos::relay::test::v1::*;".to_owned());
        let rendered = render(&roots, &uses, &bytes).unwrap();
        assert!(rendered.contains("use relay_test_protos::relay::test::v1::*;"));
    }

    #[test]
    fn test_descriptor_set_without_include_imports_is_rejected() {
        // `Tree.metadata` points into `metadata.proto`, which is absent from the set unless
        // protoc was asked to include imports. Describing `Tree` without it would silently
        // under-count, so this has to fail loudly.
        let error = render(&[TREE.to_owned()], &None, &descriptor_set(&[TREE], false))
            .unwrap_err()
            .to_string();

        assert!(error.contains("relay.test.v1.Tree.metadata"), "{error}");
        assert!(error.contains(".relay.test.common.v1.Metadata"), "{error}");
        assert!(error.contains("--include_imports"), "{error}");
    }

    #[test]
    fn test_map_fields_are_rejected() {
        // A map is sugar for a repeated synthesized entry message; its cost is not described by
        // the entry descriptor alone.
        let error = render_fixture(&[MAPS]).unwrap_err().to_string();

        assert!(error.contains("relay.test.v1.WithMap.labels"), "{error}");
        assert!(error.contains("is a map"), "{error}");
    }

    #[test]
    fn test_group_fields_are_rejected() {
        // Groups use their own framing rather than a length prefix.
        let error = render_fixture(&[GROUPS]).unwrap_err().to_string();

        assert!(error.contains("relay.test.v1.WithGroup.inner"), "{error}");
        assert!(error.contains("is a group"), "{error}");
    }

    #[test]
    fn test_ident_collisions_are_rejected() {
        // `FooBar` and `Foo.Bar` both flatten to `FOO_BAR`; emitting the static twice would not
        // compile, and silently keeping one would describe the wrong message.
        let error = render_fixture(&[COLLISION]).unwrap_err().to_string();

        assert!(error.contains("relay.test.v1.FooBar"), "{error}");
        assert!(error.contains("relay.test.v1.Foo.Bar"), "{error}");
        assert!(error.contains("FOO_BAR"), "{error}");
    }
}
