use pest::Parser;
use pest::iterators::Pair;
use proc_macro2::TokenStream;
use quote::quote;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Operation {
    ops: Vec<String>,
    templates: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Name {
    operations: Vec<Operation>,
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
pub fn name_file_output(names: impl Iterator<Item = Name>) -> TokenStream {
    let operations = names.flat_map(|name| name.operations);

    let match_arms = TokenStream::from_iter(operations.map(|Operation { ops, templates }| {
        // Each template stanza handles a single template: if all referenced attributes are present,
        // the name is constructed via that template and returned.
        let template_stanzas = templates.into_iter().flat_map(|template| {
            let Some(parts) = parse_template_into_parts(&template) else {
                println!(
                    "sentry_conventions contained unparseable template \"{}\"",
                    template
                );
                return None;
            };

            let first_part = parts.first()?;
            let referenced_attribute_names: Vec<&str> = parts
                .iter()
                .filter_map(|part| match part {
                    TemplatePart::Literal(_) => None,
                    TemplatePart::Attribute(attr) => Some(*attr),
                })
                .collect();

            if referenced_attribute_names.is_empty() {
                // There are no dynamic parts to this name - it consists of a single literal.
                let static_name = match first_part {
                    TemplatePart::Literal(s) => *s,
                    _ => unreachable!(),
                };
                // Because of the enforced invariant that the single literal template is always
                // last, no explicit return is needed. (Otherwise clippy complains.)
                return Some(quote! {
                    #static_name.to_owned()
                });
            }

            // Ensure that all referenced attributes are present in `attributes` before constructing
            // a name from them.
            let if_clauses = referenced_attribute_names.iter().map(|attribute_name| {
                quote! {
                    attributes.get_value(#attribute_name).and_then(|value| value.as_str()).is_some()
                }
            });

            // Append each part to the String variable `name`.
            let append_parts_to_string =
                TokenStream::from_iter(parts.iter().map(|part| match part {
                    TemplatePart::Literal(s) => {
                        // This feels like a micro-optimization, but clippy complains without it.
                        if s.chars().count() == 1 {
                            let c = s.chars().next();
                            quote! { name.push(#c); }
                        } else {
                            quote! { name.push_str(#s); }
                        }
                    }
                    TemplatePart::Attribute(attribute_name) => quote! {
                        name.push_str(attributes.get_value(#attribute_name).unwrap().as_str().unwrap());
                    },
                }));

            // Assemble the pieces together into a single `if` block.
            Some(quote! {
                if #(#if_clauses)&&* {
                    let mut name = String::new();
                    #append_parts_to_string
                    return name;
                };
            })
        });

        // Assemble the match arm, with `ops` forming the match clause and the list of
        // `template_stanzas` (one per template) forming the match body.
        quote! {
            #(#ops)|* => {
                #(#template_stanzas)*
            }
        }
    }));

    quote! {
        pub fn name_for_op_and_attributes(op: &str, attributes: &impl relay_protocol::Getter) -> String {
            match op {
                #match_arms
                _ => op.to_owned()
            }
        }
    }
}

enum TemplatePart<'a> {
    Literal(&'a str),
    Attribute(&'a str),
}

fn parse_template_into_parts(template: &'_ str) -> Option<Vec<TemplatePart<'_>>> {
    let Ok(mut parsed) = TemplateParser::parse(Rule::root, template) else {
        println!(
            "sentry_conventions contained unparseable template \"{}\"",
            template
        );
        return None;
    };
    let root = parsed.next().unwrap();

    let mut parts: Vec<TemplatePart> = Vec::new();
    for part in root.into_inner() {
        match part.as_rule() {
            Rule::text => parts.push(TemplatePart::Literal(part.as_str())),
            Rule::attribute_name => parts.push(TemplatePart::Attribute(part.as_str())),
            Rule::EOI => {}
            Rule::root | Rule::attribute => unreachable!(),
        }
    }
    Some(parts)
}

mod parser {
    #[derive(pest_derive::Parser)]
    #[grammar = "../build/name_template.pest"]
    pub struct TemplateParser;
}

use self::parser::{Rule, TemplateParser};
