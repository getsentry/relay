#![doc(
    html_logo_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png",
    html_favicon_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png"
)]

use std::collections::HashMap;
use std::fmt;
use std::fs::File;
use std::io::{self, Write};
use std::path::PathBuf;

use anyhow::{Context, Result};
use serde::Serialize;
use structopt::clap::AppSettings;
use structopt::StructOpt;

static FILE_CONTENTS: &str = include_str!("../../../relay-server/src/metrics.rs");

#[derive(Clone, Copy, Debug)]
enum SchemaFormat {
    Json,
    Yaml,
}

impl fmt::Display for SchemaFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Json => write!(f, "json"),
            Self::Yaml => write!(f, "yaml"),
        }
    }
}

#[derive(Debug)]
struct ParseSchemaFormatError;

impl fmt::Display for ParseSchemaFormatError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid schema format")
    }
}

impl std::error::Error for ParseSchemaFormatError {}

impl std::str::FromStr for SchemaFormat {
    type Err = ParseSchemaFormatError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "json" => Ok(Self::Json),
            "yaml" => Ok(Self::Yaml),
            _ => Err(ParseSchemaFormatError),
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize)]
enum MetricType {
    Timer,
    Counter,
    Histogram,
    Set,
    Gauge,
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct MetricPath(syn::Ident, syn::Ident);

#[derive(Debug, Serialize)]
struct Metric {
    #[serde(rename = "type")]
    ty: MetricType,
    name: String,
    description: String,
    features: Vec<String>,
}

/// Returns the value of a matching attribute in form `#[name = "value"]`.
fn get_attr(name: &str, nv: syn::MetaNameValue) -> Option<String> {
    match nv.lit {
        syn::Lit::Str(lit_string) if nv.path.is_ident(name) => Some(lit_string.value()),
        _ => None,
    }
}

/// Adds a line to the string if the attribute is a doc attribute.
fn add_doc_line(docs: &mut String, nv: syn::MetaNameValue) {
    if let Some(line) = get_attr("doc", nv) {
        if !docs.is_empty() {
            docs.push('\n');
        }
        docs.push_str(line.trim());
    }
}

/// Adds the name of the feature if the given attribute is a `cfg(feature)` attribute.
fn add_feature(features: &mut Vec<String>, l: syn::MetaList) {
    if l.path.is_ident("cfg") {
        for nested in l.nested {
            if let syn::NestedMeta::Meta(syn::Meta::NameValue(nv)) = nested {
                if let Some(feature) = get_attr("feature", nv) {
                    features.push(feature);
                }
            }
        }
    }
}

/// Returns metric information from metric enum variant attributes.
fn parse_variant_parts(attrs: &[syn::Attribute]) -> Result<(String, Vec<String>)> {
    let mut features = Vec::new();
    let mut docs = String::new();

    for attribute in attrs {
        match attribute.parse_meta()? {
            syn::Meta::NameValue(nv) => add_doc_line(&mut docs, nv),
            syn::Meta::List(l) => add_feature(&mut features, l),
            _ => (),
        }
    }

    Ok((docs, features))
}

/// Returns the final type name from a path in format `path::to::Type`.
fn get_path_type(mut path: syn::Path) -> Option<syn::Ident> {
    let last_segment = path.segments.pop()?;
    Some(last_segment.into_value().ident)
}

/// Returns the variant name and string value of a match arm in format `Type::Variant => "value"`.
fn get_match_pair(arm: syn::Arm) -> Option<(syn::Ident, String)> {
    let variant = match arm.pat {
        syn::Pat::Path(path) => get_path_type(path.path)?,
        _ => return None,
    };

    if let syn::Expr::Lit(lit) = *arm.body {
        if let syn::Lit::Str(s) = lit.lit {
            return Some((variant, s.value()));
        }
    }

    None
}

/// Returns the metric type of a trait implementation.
fn get_metric_type(imp: &mut syn::ItemImpl) -> Option<MetricType> {
    let (_, path, _) = imp.trait_.take()?;
    let trait_name = get_path_type(path)?;

    if trait_name == "TimerMetric" {
        Some(MetricType::Timer)
    } else if trait_name == "CounterMetric" {
        Some(MetricType::Counter)
    } else if trait_name == "HistogramMetric" {
        Some(MetricType::Histogram)
    } else if trait_name == "SetMetric" {
        Some(MetricType::Set)
    } else if trait_name == "GaugeMetric" {
        Some(MetricType::Gauge)
    } else {
        None
    }
}

/// Returns the type name of a type in format `path::to::Type`.
fn get_type_name(ty: syn::Type) -> Option<syn::Ident> {
    match ty {
        syn::Type::Path(path) => get_path_type(path.path),
        _ => None,
    }
}

/// Resolves match arms in the implementation of the `name` method.
fn find_name_arms(items: Vec<syn::ImplItem>) -> Option<Vec<syn::Arm>> {
    for item in items {
        let method = match item {
            syn::ImplItem::Method(method) if method.sig.ident == "name" => method,
            _ => continue,
        };

        for stmt in method.block.stmts {
            if let syn::Stmt::Expr(syn::Expr::Match(mat)) = stmt {
                return Some(mat.arms);
            }
        }
    }

    None
}

/// Parses metrics information from an impl block.
fn parse_impl_parts(mut imp: syn::ItemImpl) -> Option<(MetricType, syn::Ident, Vec<syn::Arm>)> {
    let ty = get_metric_type(&mut imp)?;
    let type_name = get_type_name(*imp.self_ty)?;
    let arms = find_name_arms(imp.items)?;
    Some((ty, type_name, arms))
}

/// Parses metrics from the given source code.
fn parse_metrics(source: &str) -> Result<Vec<Metric>> {
    let ast = syn::parse_file(source).with_context(|| "failed to parse metrics file")?;

    let mut variant_parts = HashMap::new();
    let mut impl_parts = HashMap::new();

    for item in ast.items {
        match item {
            syn::Item::Enum(enum_item) => {
                for variant in enum_item.variants {
                    let path = MetricPath(enum_item.ident.clone(), variant.ident);
                    let (description, features) = parse_variant_parts(&variant.attrs)?;
                    variant_parts.insert(path, (description, features));
                }
            }
            syn::Item::Impl(imp) => {
                if let Some((ty, type_name, arms)) = parse_impl_parts(imp) {
                    for (variant, name) in arms.into_iter().filter_map(get_match_pair) {
                        let path = MetricPath(type_name.clone(), variant);
                        impl_parts.insert(path, (name, ty));
                    }
                }
            }
            _ => (),
        }
    }

    let mut metrics = Vec::new();

    for (path, (name, ty)) in impl_parts {
        let (description, features) = variant_parts.remove(&path).unwrap();
        metrics.push(Metric {
            name,
            ty,
            description,
            features,
        });
    }

    metrics.sort_by(|a, b| a.name.cmp(&b.name));

    Ok(metrics)
}

/// Prints documentation for metrics.
#[derive(Debug, StructOpt)]
#[structopt(verbatim_doc_comment, setting = AppSettings::ColoredHelp)]
struct Cli {
    /// The format to output the documentation in.
    #[structopt(short, long, default_value = "json")]
    format: SchemaFormat,

    /// Optional output path. By default, documentation is printed on stdout.
    #[structopt(short, long, value_name = "PATH")]
    output: Option<PathBuf>,
}

impl Cli {
    fn write_metrics<W: Write>(&self, writer: W, metrics: &[Metric]) -> Result<()> {
        match self.format {
            SchemaFormat::Json => serde_json::to_writer_pretty(writer, metrics)?,
            SchemaFormat::Yaml => serde_yaml::to_writer(writer, metrics)?,
        };

        Ok(())
    }

    pub fn run(self) -> Result<()> {
        let metrics = parse_metrics(FILE_CONTENTS)?;

        match self.output {
            Some(ref path) => self.write_metrics(File::create(path)?, &metrics)?,
            None => self.write_metrics(io::stdout(), &metrics)?,
        }

        Ok(())
    }
}

fn print_error(error: &anyhow::Error) {
    eprintln!("Error: {}", error);

    let mut cause = error.source();
    while let Some(ref e) = cause {
        eprintln!("  caused by: {}", e);
        cause = e.source();
    }
}

#[paw::main]
fn main(cli: Cli) {
    match cli.run() {
        Ok(()) => (),
        Err(error) => {
            print_error(&error);
            std::process::exit(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_metrics() -> Result<()> {
        let source = r#"
            /// A metric collection used for testing.
            pub enum TestSets {
                /// The metric we test.
                UniqueSet,
                /// The metric we test.
                #[cfg(feature = "conditional")]
                ConditionalSet,
            }

            impl SetMetric for TestSets {
                fn name(&self) -> &'static str {
                    match self {
                        Self::UniqueSet => "test.unique",
                        #[cfg(feature = "conditional")]
                        Self::ConditionalSet => "test.conditional",
                    }
                }
            }
        "#;

        let metrics = parse_metrics(&source)?;
        insta::assert_debug_snapshot!(metrics, @r###"
        [
            Metric {
                ty: Set,
                name: "test.conditional",
                description: "The metric we test.",
                features: [
                    "conditional",
                ],
            },
            Metric {
                ty: Set,
                name: "test.unique",
                description: "The metric we test.",
                features: [],
            },
        ]
        "###);

        Ok(())
    }
}
