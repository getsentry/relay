use pest::Parser;
use proc_macro2::Ident;
use quote::format_ident;

pub enum TemplatePart<'a> {
    Literal(&'a str),
    Attribute(&'a str, Ident),
}

pub fn parse_template_into_parts(template: &'_ str) -> Vec<TemplatePart<'_>> {
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
