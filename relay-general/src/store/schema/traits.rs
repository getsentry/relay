use regex::Regex;

use crate::processor::PathItem;

#[derive(Clone, Copy, Debug)]
pub struct SchemaAttrs {
    pub required: &'static [PathItem<'static>],
    pub nonempty: &'static [PathItem<'static>],
    pub trim_whitespace: &'static [PathItem<'static>],
    pub match_regex: &'static [(PathItem<'static>, Regex)],
}

impl Default for SchemaAttrs {
    fn default() -> SchemaAttrs {
        SchemaAttrs {
            required: &[],
            nonempty: &[],
            trim_whitespace: &[],
            match_regex: &[],
        }
    }
}

impl SchemaAttrs {
    pub fn required(&self, field: &PathItem<'_>) -> bool {
        self.required.contains(&field)
    }

    pub fn nonempty(&self, field: &PathItem<'_>) -> bool {
        self.nonempty.contains(&field)
    }

    pub fn trim_whitespace(&self, field: &PathItem<'_>) -> bool {
        self.trim_whitespace.contains(&field)
    }

    pub fn match_regex(&self, field: &PathItem<'_>) -> Option<&Regex> {
        for (k, v) in self.match_regex {
            if k == field {
                return Some(v);
            }
        }

        None
    }
}
