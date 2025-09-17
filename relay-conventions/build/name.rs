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

pub fn name_file_output(names: impl Iterator<Item = Name>) -> TokenStream {
    let operations = names.flat_map(|name| name.operations);

    let match_arms = TokenStream::from_iter(operations.map(|Operation { ops, templates }| {
        let template_stanzas = templates.into_iter().flat_map(|template| {
            let Ok(mut parsed) = TemplateParser::parse(Rule::root, &template) else {
                println!(
                    "sentry_conventions contained unparseable template \"{}\"",
                    template
                );
                return None;
            };
            let root = parsed.next().unwrap();
            let parts = template_parts_from_root(root);
            let first_part = parts.first()?;
            let attributes: Vec<&str> = parts
                .iter()
                .filter_map(|part| match part {
                    TemplatePart::Literal(_) => None,
                    TemplatePart::Attribute(attr) => Some(*attr),
                })
                .collect();

            if attributes.is_empty() {
                // There are no dynamic parts to this name - it consists of a single literal.
                let static_name = match first_part {
                    TemplatePart::Literal(s) => *s,
                    _ => unreachable!(),
                };
                return Some(quote! {
                    return #static_name.to_owned();
                });
            }

            // Ensure that all referenced attributes are present before constructing a name from
            // them.
            let if_clauses = attributes.iter().map(|attribute| {
                quote! {
                    attributes.get_value(#attribute).and_then(|val| val.as_str()).is_some()
                }
            });
            let string_appends = TokenStream::from_iter(parts.iter().map(|part| match part {
                TemplatePart::Literal(s) => quote! { name.push_str(#s); },
                TemplatePart::Attribute(attribute) => quote! {
                    name.push_str(attributes.get_value(#attribute).unwrap().as_str().unwrap());
                },
            }));
            Some(quote! {
                if #(#if_clauses)&&* {
                    let mut name = String::new();
                    #string_appends
                    return name;
                };
            })
        });

        quote! {
            #(#ops)|* => {
                #(#template_stanzas)*
            }
        }
    }));

    quote! {
        #[allow(clippy::style)]
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

fn template_parts_from_root(root: Pair<Rule>) -> Vec<TemplatePart> {
    let mut parts: Vec<TemplatePart> = Vec::new();
    for part in root.into_inner() {
        match part.as_rule() {
            Rule::text => parts.push(TemplatePart::Literal(part.as_str())),
            Rule::attribute_name => parts.push(TemplatePart::Attribute(part.as_str())),
            Rule::EOI => {}
            Rule::root | Rule::attribute => unreachable!(),
        }
    }
    parts
}

mod parser {
    #[derive(pest_derive::Parser)]
    #[grammar = "../build/name_template.pest"]
    pub struct TemplateParser;
}

use self::parser::{Rule, TemplateParser};
