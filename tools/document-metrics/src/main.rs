#![doc(
    html_logo_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png",
    html_favicon_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png"
)]

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Parser, ValueEnum};
use serde::Serialize;
use syn::{Expr, Lit, LitStr};

#[derive(Clone, Copy, Debug, ValueEnum)]
enum SchemaFormat {
    Json,
    Yaml,
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
fn get_attr(name: &str, nv: &syn::MetaNameValue) -> Option<String> {
    let Expr::Lit(lit) = &nv.value else {
        return None;
    };
    match &lit.lit {
        syn::Lit::Str(s) if nv.path.is_ident(name) => Some(s.value()),
        _ => None,
    }
}

/// Adds a line to the string if the attribute is a doc attribute.
fn add_doc_line(docs: &mut String, nv: &syn::MetaNameValue) {
    if let Some(line) = get_attr("doc", nv) {
        if !docs.is_empty() {
            docs.push('\n');
        }
        docs.push_str(line.trim());
    }
}

/// Adds the name of the feature if the given attribute is a `cfg(feature)` attribute.
fn add_feature(features: &mut Vec<String>, l: &syn::MetaList) -> Result<()> {
    if l.path.is_ident("cfg") {
        l.parse_nested_meta(|meta| process_meta_item(features, &meta))?;
    }
    Ok(())
}

/// Recursively processes a meta item and its nested items.
fn process_meta_item(
    features: &mut Vec<String>,
    meta: &syn::meta::ParseNestedMeta,
) -> syn::Result<()> {
    if meta.path.is_ident("feature") {
        let s = meta.value()?.parse::<LitStr>()?;
        features.push(s.value());
    } else if meta.path.is_ident("all") {
        meta.parse_nested_meta(|nested_meta| process_meta_item(features, &nested_meta))?;
    } else if let Some(ident) = meta.path.get_ident() {
        features.push(ident.to_string());
    } else if !meta.input.peek(syn::Token![,]) {
        let _ = meta.value()?.parse::<Lit>()?;
    }
    Ok(())
}

/// Returns metric information from metric enum variant attributes.
fn parse_variant_parts(attrs: &[syn::Attribute]) -> Result<(String, Vec<String>)> {
    let mut features = Vec::new();
    let mut docs = String::new();

    for attribute in attrs {
        match &attribute.meta {
            syn::Meta::NameValue(nv) => add_doc_line(&mut docs, nv),
            syn::Meta::List(l) => add_feature(&mut features, l)?,
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
            syn::ImplItem::Fn(method) if method.sig.ident == "name" => method,
            _ => continue,
        };

        for stmt in method.block.stmts {
            if let syn::Stmt::Expr(syn::Expr::Match(mat), _) = stmt {
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

fn sort_metrics(metrics: &mut [Metric]) {
    metrics.sort_by(|a, b| a.name.cmp(&b.name));
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

    let mut metrics = Vec::with_capacity(impl_parts.len());

    for (path, (name, ty)) in impl_parts {
        let (description, features) = variant_parts.remove(&path).unwrap();
        metrics.push(Metric {
            ty,
            name,
            description,
            features,
        });
    }

    sort_metrics(&mut metrics);
    Ok(metrics)
}

/// Prints documentation for metrics.
#[derive(Debug, Parser)]
#[command(verbatim_doc_comment)]
struct Cli {
    /// The format to output the documentation in.
    #[arg(value_enum, short, long, default_value = "json")]
    format: SchemaFormat,

    /// Optional output path. By default, documentation is printed on stdout.
    #[arg(short, long)]
    output: Option<PathBuf>,

    /// Paths to source files declaring metrics.
    #[arg(required = true)]
    paths: Vec<PathBuf>,
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
        let mut metrics = Vec::new();
        for path in &self.paths {
            metrics.extend(parse_metrics(&fs::read_to_string(path)?)?);
        }
        sort_metrics(&mut metrics);

        match self.output {
            Some(ref path) => self.write_metrics(File::create(path)?, &metrics)?,
            None => self.write_metrics(io::stdout(), &metrics)?,
        }

        Ok(())
    }
}

fn print_error(error: &anyhow::Error) {
    eprintln!("Error: {error}");

    let mut cause = error.source();
    while let Some(ref e) = cause {
        eprintln!("  caused by: {e}");
        cause = e.source();
    }
}

fn main() {
    let cli = Cli::parse();

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
                /// Another metric we test.
                #[cfg(cfg_flag)]
                ConditionalCompileSet,
                /// Yet another metric we test.
                #[cfg(all(cfg_flag, feature = "conditional"))]
                MultiConditionalCompileSet,
            }

            impl SetMetric for TestSets {
                fn name(&self) -> &'static str {
                    match self {
                        Self::UniqueSet => "test.unique",
                        #[cfg(feature = "conditional")]
                        Self::ConditionalSet => "test.conditional",
                        #[cfg(cfg_flag)]
                        Self::ConditionalCompileSet => "test.conditional_compile",
                        #[cfg(all(cfg_flag, feature = "conditional"))]
                        Self::MultiConditionalCompileSet => "test.multi_conditional_compile"
                    }
                }
            }
        "#;

        let metrics = parse_metrics(source)?;
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
                name: "test.conditional_compile",
                description: "Another metric we test.",
                features: [
                    "cfg_flag",
                ],
            },
            Metric {
                ty: Set,
                name: "test.multi_conditional_compile",
                description: "Yet another metric we test.",
                features: [
                    "cfg_flag",
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
