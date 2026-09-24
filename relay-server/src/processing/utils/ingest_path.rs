use std::sync::OnceLock;

use relay_auth::RelayVersion;
use relay_config::Config;
use relay_conventions::attributes::SENTRY__RELAY__INGEST_PATH;
use relay_event_schema::protocol::{AttributeType, AttributeValue, Attributes, RelayInfo};
use relay_protocol::{Annotated, Array, Value};

use crate::constants::GIT_REVISION_SHORT;

fn format_relay_ingest_path_prefix(version: &str, git_revision: Option<&str>) -> String {
    match git_revision {
        Some(revision) if !revision.is_empty() => format!("{version}@[{revision}]"),
        _ => version.to_owned(),
    }
}

fn format_relay_ingest_path_entry(
    version: &str,
    git_revision: Option<&str>,
    public_key: Option<&str>,
) -> String {
    let prefix = format_relay_ingest_path_prefix(version, git_revision);
    format!("{prefix}:{}", public_key.unwrap_or_default())
}

fn relay_info_path_entry(relay_info: &RelayInfo) -> Option<String> {
    Some(format_relay_ingest_path_entry(
        relay_info.version.as_str()?,
        None,
        relay_info.public_key.as_str(),
    ))
}

fn current_relay_ingest_path_entry(config: &Config) -> String {
    static PREFIX: OnceLock<String> = OnceLock::new();

    let prefix = PREFIX.get_or_init(|| {
        format_relay_ingest_path_prefix(&RelayVersion::current().to_string(), GIT_REVISION_SHORT)
    });

    format!(
        "{prefix}:{}",
        config
            .public_key()
            .map(|key| key.to_string())
            .unwrap_or_default()
    )
}

fn read_ingest_path_attribute(attributes: &Attributes) -> Option<Vec<String>> {
    match attributes.get_value(SENTRY__RELAY__INGEST_PATH)? {
        Value::Array(values) => values
            .iter()
            .map(|value| match value.value()? {
                Value::String(value) => Some(value.clone()),
                _ => None,
            })
            .collect(),
        _ => None,
    }
}

fn write_ingest_path_attribute(attributes: &mut Attributes, entries: Vec<String>) {
    let values = entries
        .into_iter()
        .map(|entry| Annotated::new(Value::String(entry)))
        .collect();

    attributes.insert(
        SENTRY__RELAY__INGEST_PATH,
        AttributeValue {
            ty: Annotated::new(AttributeType::Array),
            value: Annotated::new(Value::Array(values)),
        },
    );
}

fn normalize_relay_ingest_path_with_base(
    attributes: &mut Annotated<Attributes>,
    base_path: Vec<String>,
    current_entry: &str,
) {
    let mut ingest_path = attributes
        .value()
        .and_then(read_ingest_path_attribute)
        .unwrap_or(base_path);

    if ingest_path
        .last()
        .is_none_or(|entry| entry != current_entry)
    {
        ingest_path.push(current_entry.to_owned());
    }

    let attributes = attributes.get_or_insert_with(Default::default);
    write_ingest_path_attribute(attributes, ingest_path);
}

pub fn normalize_relay_ingest_path(attributes: &mut Annotated<Attributes>, config: &Config) {
    normalize_relay_ingest_path_with_base(
        attributes,
        Vec::new(),
        &current_relay_ingest_path_entry(config),
    );
}

pub fn normalize_relay_ingest_path_from_event(
    attributes: &mut Annotated<Attributes>,
    event_ingest_path: Option<&Array<RelayInfo>>,
    config: &Config,
) {
    let base_path = event_ingest_path
        .into_iter()
        .flatten()
        .filter_map(Annotated::value)
        .filter_map(relay_info_path_entry)
        .collect();

    normalize_relay_ingest_path_with_base(
        attributes,
        base_path,
        &current_relay_ingest_path_entry(config),
    );
}

#[cfg(test)]
mod tests {
    use relay_conventions::attributes::SENTRY__RELAY__INGEST_PATH;

    use super::*;

    fn ingest_path_values(attributes: &Annotated<Attributes>) -> Vec<String> {
        read_ingest_path_attribute(attributes.value().unwrap()).unwrap()
    }

    fn invalid_ingest_path_attribute() -> Annotated<Attributes> {
        let mut attributes = Attributes::default();
        attributes.insert(SENTRY__RELAY__INGEST_PATH, "not-an-array");
        Annotated::new(attributes)
    }

    #[test]
    fn test_format_relay_ingest_path_entry_with_hash() {
        assert_eq!(
            format_relay_ingest_path_entry("26.8.0", Some("deadbeef"), Some("public-key")),
            "26.8.0@[deadbeef]:public-key"
        );
    }

    #[test]
    fn test_format_relay_ingest_path_entry_without_hash() {
        assert_eq!(
            format_relay_ingest_path_entry("26.8.0", None, Some("public-key")),
            "26.8.0:public-key"
        );
    }

    #[test]
    fn test_normalize_relay_ingest_path_uses_base_path_when_missing() {
        let mut attributes = Annotated::empty();

        normalize_relay_ingest_path_with_base(
            &mut attributes,
            vec!["25.7.0:upstream".to_owned()],
            "26.8.0@[deadbeef]:current",
        );

        assert_eq!(
            ingest_path_values(&attributes),
            vec![
                "25.7.0:upstream".to_owned(),
                "26.8.0@[deadbeef]:current".to_owned(),
            ]
        );
    }

    #[test]
    fn test_normalize_relay_ingest_path_appends_to_existing_attribute() {
        let mut attributes = Annotated::empty();

        normalize_relay_ingest_path_with_base(
            &mut attributes,
            vec!["25.7.0:upstream".to_owned()],
            "26.8.0@[deadbeef]:current",
        );
        normalize_relay_ingest_path_with_base(
            &mut attributes,
            vec!["ignored".to_owned()],
            "26.8.0@[cafebabe]:next",
        );

        assert_eq!(
            ingest_path_values(&attributes),
            vec![
                "25.7.0:upstream".to_owned(),
                "26.8.0@[deadbeef]:current".to_owned(),
                "26.8.0@[cafebabe]:next".to_owned(),
            ]
        );
    }

    #[test]
    fn test_normalize_relay_ingest_path_replaces_invalid_attribute() {
        let mut attributes = invalid_ingest_path_attribute();

        normalize_relay_ingest_path_with_base(
            &mut attributes,
            vec!["25.7.0:upstream".to_owned()],
            "26.8.0@[deadbeef]:current",
        );

        assert_eq!(
            ingest_path_values(&attributes),
            vec![
                "25.7.0:upstream".to_owned(),
                "26.8.0@[deadbeef]:current".to_owned(),
            ]
        );
    }
}
