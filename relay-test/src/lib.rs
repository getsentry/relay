//! Helpers for testing the web server and services.
//!
//! When writing tests, keep the following points in mind:
//!
//!  - In every test, call [`setup`]. This will set up the logger so that all console output is
//!    captured by the test runner. All logs emitted with [`relay_log`] will show up for test
//!    failures or when run with `--nocapture`.
//!
//! # Example
//!
//! ```
//! #[test]
//! pub fn my_test() {
//!     relay_test::setup();
//!
//!     relay_log::debug!("hello, world!");
//! }
//! ```

#![doc(
    html_logo_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png",
    html_favicon_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png"
)]
#![allow(clippy::derive_partial_eq_without_eq)]

use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddrV4, TcpListener};
use std::path::PathBuf;
use std::process::Child;

use chrono::Utc;
use relay_auth::PublicKey;
use relay_base_schema::project::{ProjectId, ProjectKey};
use relay_dynamic_config::TransactionMetricsConfig;
use relay_event_schema::protocol::EventId;
use relay_sampling::config::{RuleType, SamplingRule};
use relay_sampling::SamplingConfig;
use relay_system::{channel, Addr, Interface};
use serde_json::{json, Map, Value};
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::mini_sentry::ProjectState;

pub mod mini_sentry;
pub mod relay;

/// Setup the test environment.
///
///  - Initializes logs: The logger only captures logs from this crate and mutes all other logs.
pub fn setup() {
    relay_log::init_test!();
}

/// Spawns a mock service that handles messages through a closure.
pub fn mock_service<S, I, F>(name: &'static str, mut state: S, mut f: F) -> (Addr<I>, JoinHandle<S>)
where
    S: Send + 'static,
    I: Interface,
    F: FnMut(&mut S, I) + Send + 'static,
{
    let (addr, mut rx) = channel(name);

    let handle = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            f(&mut state, msg);
        }

        state
    });

    (addr, handle)
}

pub trait Upstream {
    fn url(&self) -> String;
    fn internal_error_dsn(&self) -> String;
    fn insert_known_relay(&self, relay_id: Uuid, public_key: PublicKey);
    fn public_dsn_key(&self, id: ProjectId) -> ProjectKey;
}

/// All information needed to send an envelope request to relay.
#[derive(Debug, Clone)]
pub struct Envelope {
    pub http_headers: HashMap<String, String>,
    pub project_id: ProjectId,
    pub envelope_headers: HashMap<String, Value>,
    pub items: Vec<RawItem>,
}

impl Default for Envelope {
    fn default() -> Self {
        Self {
            project_id: ProjectId::new(42),
            http_headers: Default::default(),
            envelope_headers: Default::default(),
            items: Default::default(),
        }
    }
}

impl Envelope {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn project_id_from_dsn(dsn: &str) -> ProjectId {
        let trimmed_dsn = dsn.trim_matches('"').trim_start_matches("http://");

        let parts: Vec<&str> = trimmed_dsn.split([':', '@', '/']).collect();

        let project_id_str = parts[4];
        let project_id = project_id_str.parse::<u64>().unwrap();

        ProjectId::new(project_id)
    }

    pub fn from_utf8(vec: Vec<u8>) -> Self {
        let serialized_envelope: String = String::from_utf8(vec).unwrap();

        let parts: Vec<&str> = serialized_envelope.split('\n').collect();
        let envelope_headers: Value = serde_json::from_str(parts[0]).unwrap();
        let envelope_headers: HashMap<String, Value> = envelope_headers
            .as_object()
            .unwrap()
            .clone()
            .into_iter()
            .collect();

        let project_id =
            Self::project_id_from_dsn(&envelope_headers.get("dsn").unwrap().to_string());

        let item_headers: Value = serde_json::from_str(parts[1]).unwrap();
        let item_headers: HashMap<String, Value> = item_headers
            .as_object()
            .unwrap()
            .clone()
            .into_iter()
            .collect();

        let payload: Value = serde_json::from_str(parts[2]).unwrap();

        let item = RawItem::from_parts(item_headers, PayLoad::Json(payload));

        Self {
            http_headers: Default::default(),
            project_id,
            envelope_headers,
            items: vec![item],
        }
    }

    /// Returns the event in the envelope headers
    pub fn event_id(&self) -> Option<EventId> {
        let as_str = self.envelope_headers.get("event_id")?.to_string();
        let as_str = as_str.trim_matches('\"');
        let id = Uuid::parse_str(as_str).unwrap();

        Some(EventId(id))
    }

    pub fn add_basic_trace_info(mut self, public_key: ProjectKey) -> Self {
        let trace_id = self.items.last().unwrap().trace_id().unwrap();

        let trace_info = json!({
            "trace_id": trace_id.simple().to_string(),
            "public_key": public_key,
        });

        self.envelope_headers.insert("trace".into(), trace_info);
        self
    }

    pub fn set_client_sample_rate(mut self, sample_rate: f32) -> Self {
        let trace_object = self
            .envelope_headers
            .get_mut("trace")
            .unwrap()
            .as_object_mut()
            .unwrap();

        let sample_rate = format!("{:.5}", sample_rate);
        trace_object.insert(
            "sample_rate".to_string(),
            serde_json::Value::String(sample_rate),
        );

        self
    }

    pub fn add_basic_transaction(self, transaction: Option<&str>) -> Self {
        self.add_transaction(transaction, None, None)
    }

    pub fn add_transaction(
        self,
        transaction: Option<&str>,
        event_id: Option<Uuid>,
        trace_id: Option<Uuid>,
    ) -> Self {
        let item = create_transaction_item(transaction, trace_id, event_id);
        self.add_item(item)
    }

    pub fn add_error_event_with_trace_info(self, public_key: ProjectKey) -> Self {
        let (item, trace_id, event_id) = create_error_item();

        self.add_item(item).set_event_id(event_id).add_trace_info(
            trace_id,
            public_key,
            Some(1.0),
            Some(true),
            Some(1.0),
            Some("/transaction"),
        )
    }

    pub fn add_trace_info(
        mut self,
        trace_id: Uuid,
        public_key: ProjectKey,
        release: Option<f32>,
        sampled: Option<bool>,
        client_sample_rate: Option<f32>,
        transaction: Option<&str>,
    ) -> Self {
        let mut x = json!({
            "trace_id": trace_id,
            "public_key": public_key,
        });

        let trace_info = x.as_object_mut().unwrap();

        if let Some(release) = release {
            let release = format!("{:.1}", release);
            trace_info.insert("release".to_string(), release.into());
        }

        if let Some(sample_rate) = client_sample_rate {
            let sample_rate = format!("{:.5}", sample_rate);
            trace_info.insert("sample_rate".to_string(), sample_rate.into());
        }

        if let Some(transaction) = transaction {
            trace_info.insert("transaction".to_string(), transaction.into());
        }

        if let Some(sampled) = sampled {
            trace_info.insert("sampled".to_string(), sampled.into());
        }

        self.envelope_headers.insert(
            "trace".into(),
            serde_json::Value::Object(trace_info.to_owned()),
        );
        self
    }

    pub fn set_project_id(mut self, id: ProjectId) -> Self {
        self.project_id = id;
        self
    }

    pub fn set_event_id(self, id: Uuid) -> Self {
        self.add_header("event_id", &id.to_string())
    }

    /// Sets the event id of the envelope from the last inserted item.
    pub fn fill_event_id(self) -> Self {
        let last_item = self.items.last().unwrap();
        let id = last_item.event_id().unwrap();
        self.add_header("event_id", &id.to_string())
    }

    pub fn add_header(mut self, key: &str, val: &str) -> Self {
        self.envelope_headers.insert(key.into(), val.into());
        self
    }

    pub fn add_http_header(mut self, key: &str, val: &str) -> Self {
        self.http_headers.insert(key.into(), val.into());
        self
    }

    pub fn add_item(mut self, item: RawItem) -> Self {
        self.items.push(item);
        self
    }

    pub fn add_item_from_json(mut self, payload: Value, ty: &str) -> Self {
        let item = RawItem::from_json(payload).set_type(ty);
        self.items.push(item);
        self
    }

    pub fn serialize(&self) -> String {
        let mut serialized = String::new();

        // Serialize envelope-level headers as JSON
        let headers_json = serde_json::to_string(&self.envelope_headers).unwrap();
        serialized.push_str(&format!("{}\n", headers_json));

        // Serialize items, which are already adjusted to include JSON headers
        for item in &self.items {
            serialized.push_str(item.serialize().as_str());
        }

        serialized
    }
}

#[derive(Debug, Clone)]
pub enum PayLoad {
    Json(Value),
    Bytes(Vec<u8>),
}

#[derive(Debug, Clone)]
pub struct RawItem {
    headers: HashMap<String, Value>,
    payload: PayLoad,
}

impl RawItem {
    pub fn sampled(&self) -> Option<bool> {
        self.payload()
            .as_object()?
            .get("contexts")?
            .as_object()?
            .get("trace")?
            .as_object()?
            .get("sampled")?
            .as_bool()
    }

    pub fn trace_id(&self) -> Option<Uuid> {
        let binding = self.payload();
        let as_str = binding
            .as_object()
            .unwrap()
            .get("contexts")?
            .as_object()
            .unwrap()
            .get("trace")?
            .as_object()
            .unwrap()
            .get("trace_id")?
            .as_str()?
            .trim_matches('\"');

        let trace_id: Uuid = Uuid::parse_str(as_str).unwrap();

        Some(trace_id)
    }

    pub fn event_id(&self) -> Option<EventId> {
        let as_str = self
            .payload()
            .as_object()
            .unwrap()
            .get("event_id")?
            .to_string();
        let as_str = as_str.trim_matches('\"');
        let id = Uuid::parse_str(as_str).unwrap();

        Some(EventId(id))
    }

    pub fn from_parts(headers: HashMap<String, Value>, payload: PayLoad) -> Self {
        Self { headers, payload }
    }

    pub fn ty(&self) -> &str {
        let as_str = self.headers.get("type").unwrap().as_str().unwrap();
        let as_str = as_str.trim_matches('\"');
        as_str
    }

    pub fn from_json(payload: Value) -> Self {
        let mut headers = HashMap::default();

        for (key, value) in payload.as_object().unwrap().iter() {
            headers.insert(key.clone(), value.clone());
        }

        Self {
            headers,
            payload: PayLoad::Json(payload),
        }
    }

    pub fn payload_string(&self) -> String {
        match &self.payload {
            PayLoad::Json(json) => json.to_string(),
            PayLoad::Bytes(bytes) => String::from_utf8(bytes.clone()).unwrap(),
        }
    }

    pub fn payload(&self) -> Value {
        serde_json::from_str(&self.payload_string()).unwrap()
    }

    pub fn inferred_content_type(&self) -> &'static str {
        match self.payload {
            PayLoad::Json(_) => "application/json",
            PayLoad::Bytes(_) => "application/octet-stream",
        }
    }

    pub fn add_header_from_json(mut self, key: &str, val: Value) -> Self {
        self.headers.insert(key.into(), val);
        self
    }

    pub fn add_header(mut self, key: &str, val: &str) -> Self {
        self.headers.insert(key.into(), val.into());
        self
    }

    pub fn set_type(mut self, ty: &str) -> Self {
        let ty: String = ty.to_string();
        self.headers.insert("type".into(), ty.into());
        self
    }

    // Serialize the RawItem for inclusion in RawEnvelope's serialization
    pub fn serialize(&self) -> String {
        let mut headers = self.headers.clone();
        // Assuming payload length is desired in bytes, considering UTF-8 encoding
        let payload = self.payload_string();
        headers.insert(
            "length".to_owned(),
            serde_json::Value::Number(payload.len().into()),
        );

        headers.insert(
            "content_type".to_owned(),
            self.inferred_content_type().into(),
        );

        // Serialize headers to JSON string
        dbg!(&headers);
        let headers_json = serde_json::to_string(&headers).unwrap();
        let serialized = format!("{}\n{}\n", headers_json, payload);

        dbg!(serialized)
    }
}

pub fn create_error_item() -> (RawItem, Uuid, Uuid) {
    let trace_id = Uuid::new_v4();
    let event_id = Uuid::new_v4();
    let error_event = json!({
        "event_id": event_id.simple(),
        "message": "This is an error.",
        "extra": {"msg_text": "This is an error", "id": event_id.simple()},
        "type": "error",
        "environment": "production",
        "release": "foo@1.2.3",
    });

    let item = RawItem::from_json(error_event).set_type("event");
    (item, trace_id, event_id)
}

pub fn random_port() -> u16 {
    let loopback = Ipv4Addr::new(127, 0, 0, 1);
    let socket = SocketAddrV4::new(loopback, 0);
    let listener = TcpListener::bind(socket).expect("Failed to bind to address");
    listener
        .local_addr()
        .expect("Failed to get local address")
        .port()
}

pub const DEFAULT_DSN_PUBLIC_KEY: &str = "31a5a894b4524f74a9a8d0e27e21ba91";

pub struct BackgroundProcess {
    pub child: Option<Child>,
}

impl BackgroundProcess {
    pub fn new(command: &str, args: &[&str]) -> Self {
        let child = std::process::Command::new(command)
            .args(args)
            .spawn()
            .expect("Failed to start process");

        BackgroundProcess { child: Some(child) }
    }
}

impl Drop for BackgroundProcess {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

pub struct TempDir {
    base_dir: PathBuf,
}

impl Default for TempDir {
    fn default() -> Self {
        TempDir {
            base_dir: tempfile::tempdir()
                .expect("Failed to create temp dir")
                .into_path(),
        }
    }
}

impl TempDir {
    pub fn create(&mut self, name: &str) -> PathBuf {
        let dir_path = self.base_dir.join(name);
        if dir_path.exists() {
            panic!("IT ALREADY EXISTS!!");
        }
        std::fs::create_dir(&dir_path).expect("Failed to create config dir");

        dir_path
    }
}

#[derive(Clone)]
pub struct StateBuilder {
    project_id: ProjectId,
    trusted_relays: Vec<PublicKey>,
    dsn_public_key: ProjectKey,
    sampling_rules: Vec<SamplingRule>,
    transaction_metrics_version: Option<u32>,
    outcomes: Option<Value>,
}

impl From<StateBuilder> for ProjectState {
    fn from(value: StateBuilder) -> Self {
        value.build()
    }
}

impl StateBuilder {
    pub fn new() -> Self {
        Self {
            project_id: ProjectId::new(42),
            trusted_relays: vec![],
            dsn_public_key: Uuid::new_v4().simple().to_string().parse().unwrap(),
            sampling_rules: vec![],
            transaction_metrics_version: None,
            outcomes: None,
        }
    }

    pub fn public_key(&self) -> ProjectKey {
        self.dsn_public_key
    }

    pub fn enable_outcomes(mut self) -> Self {
        self.outcomes = Some(json!({
            "outcomes": {
            "emit_outcomes": true,
            "batch_size": 1,
            "batch_interval": 1,
            "source": "relay"
        }}));
        self
    }

    pub fn set_sampling_rule(self, sample_rate: f32, rule_type: RuleType) -> Self {
        let rule = new_sampling_rule(sample_rate, rule_type.into(), vec![], None, None);
        self.add_sampling_rule(rule)
    }

    pub fn set_transaction_metrics_version(mut self, version: u32) -> Self {
        self.transaction_metrics_version = Some(version);
        self
    }

    pub fn set_project_id(mut self, id: ProjectId) -> Self {
        self.project_id = id;
        self
    }

    pub fn add_trusted_relays(mut self, relays: Vec<PublicKey>) -> Self {
        self.trusted_relays.extend(relays);
        self
    }

    pub fn add_sampling_rules(mut self, rules: Vec<SamplingRule>) -> Self {
        self.sampling_rules.extend(rules);
        self
    }

    pub fn add_sampling_rule(mut self, rule: SamplingRule) -> Self {
        self.sampling_rules.push(rule);
        self
    }

    pub fn add_basic_sampling_rule(mut self, rule_type: RuleType, sample_rate: f32) -> Self {
        let rule = new_sampling_rule(sample_rate, Some(rule_type), vec![], None, None);
        self.sampling_rules.push(rule);
        self
    }

    pub fn build(self) -> ProjectState {
        let last_fetch = Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        let last_change = Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);

        let mut sampling_config = SamplingConfig::new();
        sampling_config.rules = self.sampling_rules.clone();

        let transaction_metrics = self.transaction_metrics_version.map(|version| {
            let mut tmc = TransactionMetricsConfig::new();
            tmc.version = version as u16;
            tmc
        });

        let json = json!({
        "projectId": self.project_id,
        "slug": "python",
        "publicKeys": [{
            "publicKey": self.public_key(),
        }],
        "rev": "5ceaea8c919811e8ae7daae9fe877901",
        "disabled": false,
        "lastFetch": last_fetch,
        "lastChange": last_change,
        "config": {
            "allowedDomains": ["*"],
            "trustedRelays": self.trusted_relays,
            "transactionMetrics": transaction_metrics,
            "sampling": sampling_config,
            "piiConfig": {
                "rules": {},
                "applications": {
                    "$string": ["@email", "@mac", "@creditcard", "@userpath"],
                    "$object": ["@password"],
                    },
                },
            }
        });

        ProjectState::new(serde_json::from_value(json).unwrap())
    }
}

impl Default for StateBuilder {
    fn default() -> Self {
        Self::new()
    }
}

pub fn new_sampling_rule(
    sample_rate: f32,
    rule_type: Option<RuleType>,
    releases: Vec<f32>,
    user_segments: Option<()>,
    environments: Option<()>,
) -> SamplingRule {
    let releases: Vec<String> = releases.iter().map(|x| format!("{:.1}", x)).collect();

    let rule_type = rule_type.unwrap_or(RuleType::Trace);

    let mut conditions: Vec<Value> = vec![];

    let field_prefix = match rule_type {
        RuleType::Trace => "trace.",
        RuleType::Transaction => "event.",
        RuleType::Unsupported => panic!(),
    };

    if !releases.is_empty() {
        conditions.push(json!({
            "op": "glob",
            "name": format!("{}{}", field_prefix, "release",),
            "value": releases,
        }));
    }

    if user_segments.is_some() {
        conditions.push(json!(
        {
            "op": "eq",
            "name": format!("{}{}", field_prefix, "user"),
            "value": user_segments,
            "options": {
                "ignoreCase": true,
            },
        }));
    }

    if environments.is_some() {
        conditions.push(json!(
            {
                "op": "eq",
                "name": format!("{}{}", field_prefix , "environment"),
                "value": environments,
                "options": {
                    "ignoreCase": true,
                },
            }
        ))
    }

    serde_json::from_value(json! ({
        "samplingValue": {"type": "sampleRate", "value": sample_rate},
        "type": rule_type,
        "condition": {"op": "and", "inner": conditions},
        "id": 1,
    }))
    .unwrap()
}

pub fn merge(mut base: Value, merge_value: Value, keys: Vec<&str>) -> Value {
    //    dbg!(&base, &merge_value, &keys);
    let mut base_map = base.as_object_mut().expect("base should be an object");

    // Navigate down the nested structure to the final map where the merge should occur
    for key in keys {
        if !base_map.contains_key(key) {
            // If the key doesn't exist at this level, create a new object for it
            base_map.insert(key.to_string(), Value::Object(Map::new()));
        }
        // Now we're sure the key exists, navigate into it
        base_map = base_map.get_mut(key).unwrap().as_object_mut().unwrap();
    }

    let merge_map = merge_value
        .as_object()
        .expect("merge_value should be an object");

    for (key, merge_val) in merge_map {
        if let Some(existing_val) = base_map.get_mut(key) {
            // If the key exists, and both existing and merging values are objects, merge recursively
            if let (Some(existing_obj), Some(merge_obj)) =
                (existing_val.as_object_mut(), merge_val.as_object())
            {
                for (merge_key, val) in merge_obj {
                    existing_obj.insert(merge_key.clone(), val.clone());
                }
                continue;
            }
        }
        // If the key doesn't exist, or the existing value is not an object, insert/replace directly
        base_map.insert(key.clone(), merge_val.clone());
    }

    base
}

pub fn outcomes_enabled_config() -> Value {
    json!({
        "outcomes": {
            "emit_outcomes": true,
            "batch_size": 1,
            "batch_interval": 1,
            "source": "relay"
    }})
}

pub fn create_transaction_item(
    transaction: Option<&str>,
    trace_id: Option<Uuid>,
    event_id: Option<Uuid>,
) -> RawItem {
    let trace_id = trace_id.unwrap_or_else(Uuid::new_v4);
    let event_id = event_id.unwrap_or_else(Uuid::new_v4);

    let item = json!({
        "event_id": event_id,
        "transaction": transaction.unwrap_or( "tr1"),
        "start_timestamp": 1597976392.6542819,
        "timestamp": 1597976400.6189718,
        "contexts": {
            "trace": {
                "trace_id": trace_id.simple(),
                "span_id": "FA90FDEAD5F74052",
                "type": "trace",
            }
        },
        "spans": [],
        "extra": {"id": event_id},
    });

    RawItem::from_json(item).set_type("transaction")
}

pub fn get_topic_name(topic: &str) -> String {
    let random = Uuid::new_v4().simple().to_string();
    format!("relay-test-{}-{}", topic, random)
}

pub fn processing_config() -> Value {
    let bootstrap_servers =
        std::env::var("KAFKA_BOOTSTRAP_SERVER").unwrap_or_else(|_| "127.0.0.1:49092".to_string());

    json!({
        "processing": {
            "enabled": true,
            "kafka_config": [
                {
                    "name": "bootstrap.servers",
                    "value": bootstrap_servers
                }
            ],
            "topics": {
                "events": get_topic_name("events"),
                "attachments": get_topic_name("attachments"),
                "transactions": get_topic_name("transactions"),
                "outcomes": get_topic_name("outcomes"),
                "sessions": get_topic_name("sessions"),
                "metrics": get_topic_name("metrics"),
                "metrics_generic": get_topic_name("metrics"),
                "replay_events": get_topic_name("replay_events"),
                "replay_recordings": get_topic_name("replay_recordings"),
                "monitors": get_topic_name("monitors"),
                "spans": get_topic_name("spans")
            },
            "redis": "redis://127.0.0.1",
            "projectconfig_cache_prefix": format!("relay-test-relayconfig-{}", uuid::Uuid::new_v4())
        }
    })
}
