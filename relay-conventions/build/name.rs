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
pub struct Name {
    pub operations: Vec<Operation>,
}

/// Returns a `TokenStream` representing Rust code defining a `name_for_op_and_attributes` method.
/// This method constructs a name for the given op and attributes based on the span naming
/// conventions defined in the `sentry-conventions` repository.
///
/// This function depends on the invariants that:
/// - each list of name templates contains a single template without attributes, and
/// - the template without attributes is the final template in the list
///
/// In other words, each template list must have a fallback name for the case
/// where all names are missing. These invariants are enforced by a test in `sentry-conventions`.
///
/// Some clippy lints are explicitly allowed, when fixing them would complicate the codegen.
pub fn name_file_output(names: impl Iterator<Item = Name>) -> TokenStream {
    let operations = names.flat_map(|name| name.operations);

    let match_arms = operations.map(|Operation { ops, templates }| {
        let Some((literal_template, templates_with_attributes)) = templates.split_last() else {
            panic!("name definition had empty template list");
        };
        if literal_template.contains("{{") {
            panic!("final template must not contain attributes (bad template: {})", literal_template);
        }

        let conditional_attribute_blocks = templates_with_attributes.iter().map(|template| {
            let parts = parse_template_into_parts(template);
            if !parts.iter().any(|part| matches!(part, TemplatePart::Attribute(_, _))) {
                panic!("templates before the final template must contain attributes (bad template: {})", template);
            }

            // First, each attribute becomes a let clause for our if block.
            let if_clauses = parts.iter().flat_map(|part| {
                if let TemplatePart::Attribute(name, ident) = part {
                    Some(quote! {
                        let Some(#ident) = attributes.get_value(#name).and_then(val_to_string)
                    })
                } else {
                    None
                }
            });

            // Then, we allocate a string with the appropriate size to hold the name.
            let declare_name_string = {
                let literal_length = parts.iter().fold(0, |acc, part|{
                    if let TemplatePart::Literal(s) = part {
                        acc + s.len()
                    } else {
                        acc
                    }
                });
                let attribute_lengths = parts.iter().flat_map(|part|
                    if let TemplatePart::Attribute(_, ident) = part {
                        Some(quote! { #ident.len() })
                    } else {
                        None
                    });
                quote! {
                    #[allow(clippy::identity_op)]
                    let mut name = String::with_capacity(#literal_length + #(#attribute_lengths)+*);
                }
            };

            // Finally, append each template part (literal or dynamic) to the string.
            let append_parts_to_name = parts.iter().map(|part| match part {
                TemplatePart::Literal(s) => {
                    quote! {
                        #[allow(clippy::single_char_add_str)]
                        name.push_str(#s);
                    }
                }
                TemplatePart::Attribute(_, ident) => quote! {
                    name.push_str(&#ident);
                },
            });

            Some(quote! {
                if #(#if_clauses)&&* {
                    #declare_name_string
                    #(#append_parts_to_name)*
                    return name;
                };
            })
        });

        let literal_name_fallback = quote! {
            #literal_template.to_owned()
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
        use std::borrow::Cow;
        use relay_protocol::{Getter, Val};

        pub fn name_for_op_and_attributes(op: &str, attributes: &impl Getter) -> String {
            match op {
                #(#match_arms)*
                _ => op.to_owned()
            }
        }

        fn val_to_string(val: Val<'_>) -> Option<Cow<'_, str>> {
            Some(match val {
                Val::String(s) => Cow::Borrowed(s),
                Val::I64(i) => Cow::Owned(i.to_string()),
                Val::U64(u) => Cow::Owned(u.to_string()),
                Val::F64(f) => Cow::Owned(f.to_string()),
                Val::Bool(b) => Cow::Borrowed(if b { "true" } else { "false" }),
                Val::HexId(_) | Val::Array(_) | Val::Object(_) => return None,
            })
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
    #[grammar = "../build/name_template.pest"]
    pub struct TemplateParser;
}

use self::parser::{Rule, TemplateParser};
