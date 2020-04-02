//! Functionality to override the configuration with Environment variables and command line arguments

use std::env;
use std::hash::Hash;

use clap::ArgMatches;
use serde_yaml::{self, Mapping, Number, Sequence, Value};

#[derive(Debug, Default)]
pub struct OverridableConfig {
    upstream: Option<String>,
    host: Option<String>,
    port: Option<i32>,
    processing: Option<bool>,
    kafka_url: Option<String>,
    redis_url: Option<String>,
    id: Option<String>,
    secret_key: Option<String>,
    public_key: Option<String>,
}

impl OverridableConfig {
    fn has_config(&self) -> bool {
        self.upstream.is_some()
            || self.host.is_some()
            || self.port.is_some()
            || self.processing.is_some()
            || self.kafka_url.is_some()
            || self.redis_url.is_some()
    }
    fn has_credentials(&self) -> bool {
        self.id.is_some() || self.public_key.is_some() || self.secret_key.is_some()
    }
}

/// Extract config arguments from a parsed command line arguments object
pub fn extract_config_args(matches: &ArgMatches) -> OverridableConfig {
    let port = matches
        .value_of("port")
        .and_then(|p| i32::from_str_radix(p, 10).ok());
    let processing_enabled = if matches.is_present("processing-enabled") {
        Some(true)
    } else if matches.is_present("processing-disabled") {
        Some(false)
    } else {
        None
    };

    OverridableConfig {
        secret_key: matches.value_of("secret_key").map(str::to_owned),
        public_key: matches.value_of("public_key").map(str::to_owned),
        id: matches.value_of("id").map(str::to_owned),
        upstream: matches.value_of("upstream-url").map(str::to_owned),
        host: matches.value_of("host").map(str::to_owned),
        kafka_url: matches.value_of("kafka-broker-url").map(str::to_owned),
        redis_url: matches.value_of("redis-url").map(str::to_owned),
        processing: processing_enabled,
        port,
    }
}

/// Extract config arguments from environment variables
pub fn extract_config_env_vars() -> OverridableConfig {
    let processing = if let Ok(processing) = env::var("RELAY_PROCESSING_ENABLED") {
        match processing.to_lowercase().as_str() {
            "true" => Some(true),
            "false" => Some(false),
            _ => {
                panic!("Invalid configuration parameter 'processing' expected a true/false value ")
            }
        }
    } else {
        None
    };

    let port = if let Ok(port) = env::var("RELAY_PORT") {
        if let Ok(port) = i32::from_str_radix(port.as_str(), 10) {
            Some(port)
        } else {
            panic!("Invalid configuraton parameter 'port' expected an integer value")
        }
    } else {
        None
    };

    OverridableConfig {
        upstream: env::var("RELAY_UPSTREAM_URL").ok(),
        host: env::var("RELAY_HOST").ok(),
        port,
        processing,
        kafka_url: env::var("RELAY_KAFKA_BROKER_URL").ok(),
        redis_url: env::var("RELAY_REDIS_URL").ok(),
        id: env::var("RELAY_ID").ok(),
        public_key: env::var("RELAY_PUBLIC_KEY").ok(),
        secret_key: env::var("RELAY_SECRET_KEY").ok(),
    }
}

/// Override configuration with a override object
fn override_config(config: &mut Mapping, overrides: OverridableConfig) {
    let relay = get_or_create_sub_obj(config, "relay").unwrap();

    if let Some(upstream) = overrides.upstream {
        set_string(relay, "upstream", upstream);
    }

    if let Some(host) = overrides.host {
        set_string(relay, "host", host);
    }
    if let Some(port) = overrides.port {
        set_number(relay, "port", port);
    }

    let processing = get_or_create_sub_obj(config, "processing").unwrap();

    if let Some(procesing) = overrides.processing {
        set_bool(processing, "enabled", procesing);
    }

    if let Some(redis_server_url) = overrides.redis_url {
        set_string(processing, "redis", redis_server_url);
    }

    if let Some(kafka_servers_url) = overrides.kafka_url {
        let kafka_config = get_or_create_sub_array(processing, "kafka_config").unwrap();
        set_bootstrap_servers(kafka_config, kafka_servers_url);
    }
}

fn get_or_create_sub_obj<'a>(hash: &'a mut Mapping, key: &str) -> Result<&'a mut Mapping, String> {
    let val_key = Value::String(key.into());

    if !hash.contains_key(&val_key) {
        hash.insert(val_key.clone(), Value::Mapping(Mapping::new()));
    }
    match hash.get_mut(&val_key) {
        Some(Value::Mapping(child)) => Ok(child),
        Some(_) => Err("trying to navigate through a filed that is not an Object".to_owned()),
        _ => Err(format!("not an object at key:{}", key)),
    }
}

fn get_or_create_sub_array<'a>(
    hash: &'a mut Mapping,
    key: &str,
) -> Result<&'a mut Sequence, String> {
    let val_key = Value::String(key.into());

    if !hash.contains_key(&val_key) {
        hash.insert(val_key.clone(), Value::Sequence(Sequence::new()));
    }
    match hash.get_mut(&val_key) {
        Some(Value::Sequence(child)) => Ok(child),
        Some(_) => Err("trying to navigate through a filed that is not an Object".to_owned()),
        _ => Err(format!("not an array at key:{}", key)),
    }
}

fn set_string<Key, Val>(obj: &mut Mapping, k: Key, v: Val)
where
    Key: Into<String>,
    Val: Into<String>,
{
    obj.insert(Value::String(k.into()), Value::String(v.into()));
}

fn set_number<Key, Val>(obj: &mut Mapping, k: Key, v: Val)
where
    Key: Into<String>,
    Val: Into<Number>,
{
    obj.insert(Value::String(k.into()), Value::Number(v.into()));
}

fn set_bool<Key>(obj: &mut Mapping, k: Key, v: bool)
where
    Key: Into<String>,
{
    obj.insert(Value::String(k.into()), Value::Bool(v));
}

fn set_bootstrap_servers<Val>(kafka_config: &mut Sequence, val: Val) -> ()
where
    Val: Into<String>,
{
    let name_key = Value::String("name".to_owned());
    let servers_key = "bootstrap.servers";

    for obj in kafka_config.iter_mut() {
        if let Value::Mapping(map) = obj {
            if let Some(Value::String(name)) = map.get(&name_key) {
                if name == servers_key {
                    set_string(map, "value", val);
                    return;
                }
            }
        }
    }

    let mut bootstrap_servers = Mapping::new();
    set_string(&mut bootstrap_servers, "name", "bootstrap.servers");
    set_string(&mut bootstrap_servers, "value", val);
    kafka_config.push(Value::Mapping(bootstrap_servers));
}

#[cfg(test)]
mod test {
    use super::*;
    use insta::assert_yaml_snapshot;
    use serde_yaml::{from_str, Number};

    #[test]
    fn get_existing_sub_obj() {
        let obj_src = r###"{ "a": {"sub-obj": 123 }}"###;

        let mut initial_val: Value = from_str(obj_src).unwrap();

        assert!(initial_val.is_mapping());
        let config = initial_val.as_mapping_mut().unwrap();

        let sub_obj = get_or_create_sub_obj(config, "a");

        assert!(sub_obj.is_ok());
        let sub_obj = sub_obj.unwrap();

        let expected_key = Value::String("sub-obj".to_owned());
        // assert!(sub_obj.contains_key(&expected_key));
        let expected_value = sub_obj.get(&expected_key);
        assert!(expected_value.is_some());
        assert_eq!(expected_value.unwrap(), &Value::Number(123.into()));
    }
    #[test]
    fn create_missing_sub_obj() {
        let obj_src = r###"{ "b":  123 }"###;

        let mut initial_val: Value = from_str(obj_src).unwrap();

        assert!(initial_val.is_mapping());
        let config = initial_val.as_mapping_mut().unwrap();

        let sub_obj = get_or_create_sub_obj(config, "a");

        assert!(sub_obj.is_ok());
        let sub_obj = sub_obj.unwrap();
        sub_obj.insert(
            Value::String("key".to_owned()),
            Value::String("val".to_owned()),
        );

        assert_yaml_snapshot!(initial_val, @ r###"
        ⋮---
        ⋮b: 123
        ⋮a:
        ⋮  key: val
         "###);
    }
    #[test]
    fn get_existing_sub_array() {
        let obj_src = r###"{ "a": [123]}"###;

        let mut initial_val: Value = from_str(obj_src).unwrap();

        assert!(initial_val.is_mapping());
        let config = initial_val.as_mapping_mut().unwrap();

        let array = get_or_create_sub_array(config, "a");

        assert!(array.is_ok());
        let array = array.unwrap();

        assert_eq!(array.len(), 1);
        assert_eq!(array[0], Value::Number(123.into()));
    }
    #[test]
    fn create_missing_sub_array() {
        let obj_src = r###"{ "b":  123 }"###;

        let mut initial_val: Value = from_str(obj_src).unwrap();

        assert!(initial_val.is_mapping());
        let config = initial_val.as_mapping_mut().unwrap();

        let sub_obj = get_or_create_sub_array(config, "a");

        assert!(sub_obj.is_ok());
        let sub_obj = sub_obj.unwrap();
        sub_obj.push(Value::String("val".to_owned()));

        assert_yaml_snapshot!(initial_val, @ r###"
        ⋮---
        ⋮b: 123
        ⋮a:
        ⋮  - val
         "###);
    }

    fn process_kafka_config(config: &str) -> Value {
        let mut initial_val: Value = from_str(config).unwrap();
        assert!(initial_val.is_sequence());
        let kafka_config = initial_val.as_sequence_mut().unwrap();

        let servers = "my-kafka:3333";

        set_bootstrap_servers(kafka_config, servers);
        initial_val
    }

    #[test]
    fn test_set_bootstrap_servers_empty() {
        let result = process_kafka_config("[]");
        assert_yaml_snapshot!( result, @r###"
       ⋮---
       ⋮- name: bootstrap.servers
       ⋮  value: "my-kafka:3333"
        "###);
    }
    #[test]
    fn test_set_bootstrap_servers_some_content() {
        let existing_kafka_config = r##"
             - {name: "some key", value: "some val"}
            "##;

        let result = process_kafka_config(existing_kafka_config);
        assert_yaml_snapshot!( result, @r###"
       ⋮---
       ⋮- name: some key
       ⋮  value: some val
       ⋮- name: bootstrap.servers
       ⋮  value: "my-kafka:3333"
        "###);
    }
    #[test]
    fn test_set_bootstrap_servers_existing_config() {
        let exisinting_servers = r##"
            - {name: "some key", value: "some val"}
            - {name: "some other key", value: "some other val"}
            - {name: "bootstrap.servers", value: "kafka:9092"}
        "##;

        let result = process_kafka_config(exisinting_servers);
        assert_yaml_snapshot!( result, @r###"
       ⋮---
       ⋮- name: some key
       ⋮  value: some val
       ⋮- name: some other key
       ⋮  value: some other val
       ⋮- name: bootstrap.servers
       ⋮  value: "my-kafka:3333"
        "###);
    }

    fn get_config() -> OverridableConfig {
        OverridableConfig {
            upstream: Some("new_upstream".to_owned()),
            host: Some("new_host".to_owned()),
            port: Some(8888),
            processing: Some(true),
            kafka_url: Some("new_kafka_url".to_owned()),
            redis_url: Some("new_redis_url".to_owned()),
            id: Some("new_id".to_owned()),
            public_key: Some("new_public_key".to_owned()),
            secret_key: Some("new_secret_key".to_owned()),
        }
    }

    #[test]
    fn test_override_existing_config() {
        let source_doc = r##"
            relay:
              upstream: "http://web:9000/"
              host: 0.0.0.0
              port: 3000
            processing:
              enabled: false
              kafka_config:
                - {name: "some key", value: "some val"}
                - {name: "some other key", value: "some other val"}
                - {name: "bootstrap.servers", value: "kafka:9092"}
              redis: redis://redis:6379
            "##;

        let mut initial_val: Value = from_str(source_doc).unwrap();

        assert!(initial_val.is_mapping());

        let config = initial_val.as_mapping_mut().unwrap();

        override_config(config, get_config());

        assert_yaml_snapshot!( config, @ r###"
       ⋮---
       ⋮relay:
       ⋮  upstream: new_upstream
       ⋮  host: new_host
       ⋮  port: 8888
       ⋮processing:
       ⋮  kafka_config:
       ⋮    - name: some key
       ⋮      value: some val
       ⋮    - name: some other key
       ⋮      value: some other val
       ⋮    - name: bootstrap.servers
       ⋮      value: new_kafka_url
       ⋮  enabled: true
       ⋮  redis: new_redis_url
        "###);
    }

    #[test]
    fn test_override_empty_config() {
        let mut initial_val: Value = Value::Mapping(Mapping::new());

        assert!(initial_val.is_mapping());

        let config = initial_val.as_mapping_mut().unwrap();

        override_config(config, get_config());

        assert_yaml_snapshot!( config, @ r###"
       ⋮---
       ⋮relay:
       ⋮  upstream: new_upstream
       ⋮  host: new_host
       ⋮  port: 8888
       ⋮processing:
       ⋮  enabled: true
       ⋮  redis: new_redis_url
       ⋮  kafka_config:
       ⋮    - name: bootstrap.servers
       ⋮      value: new_kafka_url
        "###);
    }

    #[test]
    fn test_empty_override_existing_config() {
        let source_doc = r##"
            relay:
              upstream: "http://web:9000/"
              host: 0.0.0.0
              port: 3000
            processing:
              enabled: true
              kafka_config:
                - {name: "some key", value: "some val"}
                - {name: "some other key", value: "some other val"}
                - {name: "bootstrap.servers", value: "kafka:9092"}
              redis: redis://redis:6379
            "##;

        let mut initial_val: Value = from_str(source_doc).unwrap();

        assert!(initial_val.is_mapping());

        let config = initial_val.as_mapping_mut().unwrap();

        override_config(config, OverridableConfig::default());

        assert_yaml_snapshot!( config, @ r###"
       ⋮---
       ⋮relay:
       ⋮  upstream: "http://web:9000/"
       ⋮  host: 0.0.0.0
       ⋮  port: 3000
       ⋮processing:
       ⋮  enabled: false
       ⋮  kafka_config:
       ⋮    - name: some key
       ⋮      value: some val
       ⋮    - name: some other key
       ⋮      value: some other val
       ⋮    - name: bootstrap.servers
       ⋮      value: "kafka:9092"
       ⋮  redis: "redis://redis:6379"
        "###);
    }
}
