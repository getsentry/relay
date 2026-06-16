use pest::Parser;
use proc_macro2::{Ident, TokenStream};
use quote::{format_ident, quote};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Operation {
    pub ops: Vec<String>,
    pub templates: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Description {
    pub operations: Vec<Operation>,
}

/// Returns a `TokenStream` representing Rust code defining a `description_for_op_and_attributes` method.
/// This method constructs a description for the given op and attributes based on the span description
/// conventions defined in the `sentry-conventions` repository.
///
/// Some clippy lints are explicitly allowed, when fixing them would complicate the codegen.
pub fn description_file_output(descriptions: impl Iterator<Item = Description>) -> TokenStream {
    let operations = descriptions.flat_map(|description| description.operations);

    let match_arms = operations.map(|Operation { ops, templates }| {
        let (templates_with_attributes, literal_template)  = match templates.split_last() {
            Some((last, init)) if !last.contains("{{")=> (init, Some(last.as_str())),
            _ => (&templates[..], None),
        };

        let conditional_attribute_blocks = templates_with_attributes.iter().map(|template| {
            let parts = parse_template_into_parts(template);
            if !parts.iter().any(|part| matches!(part, TemplatePart::Attribute(_, _))) {
                panic!("templates before the final template must contain attributes (bad template: {})", template);
            }

            // First, each attribute becomes a let clause for our if block.
            let if_clauses = parts.iter().flat_map(|part| {
                if let TemplatePart::Attribute(name, ident) = part {
                    Some(quote! {
                        let Some(#ident @ (Val::String(_) | Val::Bool(_) | Val::U64(_) | Val::I64(_) | Val::F64(_))) = attributes.get_value(#name)
                    })
                } else {
                    None
                }
            });

            // Then, construct the format string and argument list for a `format!` call to produce the name.
            let format_string = parts.iter().map(|part| match part {
                TemplatePart::Literal(l) => *l,
                TemplatePart::Attribute(_, _) => "{}",
            }).collect::<String>();
            let format_args = parts.iter().flat_map(|part| {
                if let TemplatePart::Attribute(_, ident) = part {
                    Some(quote! { DisplayVal(#ident) })
                } else {
                    None
                }
            });

            Some(quote! {
                if #(#if_clauses)&&* {
                    return Some(format!(#format_string, #(#format_args),*));
                };
            })
        });

        let literal_name_fallback = match literal_template {
            Some(literal_template) => quote! { Some(#literal_template.to_owned()) },
            None => quote! { None },
        };

        // Assemble the match arm, with `ops` forming the match clause and the match body checking
        // each template in turn before falling back to a literal (zero-attribute) template.
        quote! {
            #(#ops)|* => {
                #(#conditional_attribute_blocks)*
                #literal_name_fallback
            }
        }
    });

    quote! {
        use relay_protocol::{Getter, Val};
        use std::fmt;
        use std::fmt::Display;

        pub fn description_for_op_and_attributes(op: &str, attributes: &impl Getter) -> Option<String> {
            match op {
                #(#match_arms)*
                _ => None
            }
        }

        struct DisplayVal<'a>(Val<'a>);

        impl Display for DisplayVal<'_> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                match self.0 {
                    Val::Bool(b) => write!(f, "{b}"),
                    Val::I64(i) => write!(f, "{i}"),
                    Val::U64(u) => write!(f, "{u}"),
                    Val::F64(fl) => write!(f, "{fl}"),
                    Val::String(s) => f.write_str(s),
                    Val::HexId(_) | Val::Array(_) | Val::Object(_) => Ok(()),
                }
            }
        }
    }
}

enum TemplatePart<'a> {
    Literal(&'a str),
    Attribute(&'a str, Ident),
}

fn parse_template_into_parts(template: &'_ str) -> Vec<TemplatePart<'_>> {
    let Ok(mut parsed) = TemplateParser::parse(Rule::root, template) else {
        // This panic (at build time) will make it obvious if the sentry-conventions submodule ever
        // contains an invalid template.
        panic!(
            "sentry_conventions contained unparseable template \"{}\"",
            template
        );
    };
    let root = parsed.next().unwrap();
    root.into_inner()
        .enumerate()
        .filter_map(|(i, part)| {
            Some(match part.as_rule() {
                Rule::text => TemplatePart::Literal(part.as_str()),
                Rule::attribute_name => {
                    TemplatePart::Attribute(part.as_str(), format_ident!("attribute_{}", i))
                }
                Rule::EOI => return None,
                Rule::root | Rule::attribute => unreachable!(),
            })
        })
        .collect()
}

mod parser {
    #[derive(pest_derive::Parser)]
    #[grammar = "../build/name_description_template.pest"]
    pub struct TemplateParser;
}

use self::parser::{Rule, TemplateParser};
