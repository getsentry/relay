use proc_macro2::TokenStream;
use quote::quote;
use serde::Deserialize;

use crate::template::{TemplatePart, parse_template_into_parts};

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
                        let Some(#ident @ (Value::String(_) | Value::Bool(_) | Value::U64(_) | Value::I64(_) | Value::F64(_) | Value::Array(_))) = get_attribute_value(#name)
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
                    Some(quote! { DisplayVal(&#ident) })
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

        let literal_name_fallback = quote! {
            Some(#literal_template.to_owned())
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
        use relay_protocol::Value;
        use std::fmt;
        use std::fmt::Display;

        pub fn name_for_op_and_attributes<'a>(op: &str, get_attribute_value: impl Fn(&'_ str) -> Option<&'a Value>) -> Option<String> {
            match op {
                #(#match_arms)*
                _ => None
            }
        }

        struct DisplayVal<'a>(&'a Value);

        impl Display for DisplayVal<'_> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                match self.0 {
                    Value::Bool(b) => write!(f, "{b}"),
                    Value::I64(i) => write!(f, "{i}"),
                    Value::U64(u) => write!(f, "{u}"),
                    Value::F64(fl) => write!(f, "{fl}"),
                    Value::String(s) => f.write_str(s),
                    Value::Array(vals) => {
                        let mut first = true;

                        for v in vals {
                            let Some(v) = v.value() else {
                                continue;
                            };

                            if !first {
                                f.write_str(", ")?;
                            }

                            write!(f, "{}", DisplayVal(v))?;

                            first = false;
                        }
                        Ok(())
                    }
                    Value::Object(_) => Ok(()),
                }
            }
        }
    }
}
