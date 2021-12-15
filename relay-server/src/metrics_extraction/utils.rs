use std::collections::BTreeMap;
use std::fmt;

pub fn with_tag(
    tags: &BTreeMap<String, String>,
    name: &str,
    value: impl fmt::Display,
) -> BTreeMap<String, String> {
    let mut tags = tags.clone();
    tags.insert(name.to_owned(), value.to_string());
    tags
}
