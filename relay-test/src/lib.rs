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
//! fn my_test() {
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

use hyper::Error;
use std::collections::{BTreeMap, HashMap};
use std::net::{Ipv4Addr, SocketAddrV4, TcpListener};
use std::path::PathBuf;
use std::process::Child;
use std::str::FromStr;
//use std::sync::Mutex;
use no_deadlocks::Mutex;

use chrono::Utc;
use lazy_static::lazy_static;
use relay_auth::PublicKey;
use relay_base_schema::events::EventType;
use relay_base_schema::project::{ProjectId, ProjectKey};
use relay_config::HttpEncoding;
use relay_config::RelayInfo;
use relay_dynamic_config::{ErrorBoundary, GlobalConfig, TransactionMetricsConfig};
use relay_event_schema::protocol::{Event, LogEntry};
use relay_event_schema::protocol::{EventId, LenientString};
use relay_protocol::IntoValue;
use relay_protocol::Value as RelayValue;
use relay_quotas::Scoping;
use relay_sampling::config::{RuleType, SamplingRule};
use relay_sampling::dsc::TraceUserContext;
use relay_sampling::SamplingConfig;
use relay_server::actors::envelopes::SendEnvelope;
use relay_server::actors::project::ProjectState;
use relay_server::actors::project::PublicKeyConfig;
use relay_server::actors::upstream::UpstreamRequest;
use relay_server::envelope::ContentType;
use relay_server::envelope::Envelope;
use relay_server::envelope::EnvelopeHeaders;
use relay_server::envelope::Item;
use relay_server::envelope::ItemType;
use relay_server::envelope::Items;
use relay_server::extractors::PartialDsn;
use relay_server::extractors::RequestMeta;
use relay_system::{channel, Addr, Interface};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use uuid::fmt::Simple; // It's better to use Tokio's Mutex in async contexts.
use uuid::Uuid;

use crate::mini_sentry::MiniSentry;
use crate::relay::Relay;
use crate::test_envelopy::RawItem;

pub struct System {
    relay: Relay,
    sentry: MiniSentry,
}

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

/*
    def internal_error_dsn(self):
        """DSN whose events make the test fail."""
        return "http://{}@{}:{}/666".format(
            self.default_dsn_public_key, *self.server_address
        )
*/

lazy_static! {
    static ref KNOWN_RELAYS: Mutex<HashMap<String, RelayInfo>> = Mutex::new(HashMap::new());
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ProjectResponse {
    configs: HashMap<ProjectKey, ErrorBoundary<Option<ProjectState>>>,
    pending: Vec<ProjectKey>,
    global: Option<GlobalConfig>,
}

fn dsn(public_key: ProjectKey) -> PartialDsn {
    format!("https://{}:@sentry.io/42", public_key)
        .parse()
        .unwrap()
}

fn request_meta(public_key: ProjectKey) -> RequestMeta {
    RequestMeta::newer(dsn(public_key))
}

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

pub struct ConfigDir {
    base_dir: PathBuf,
    counters: HashMap<String, usize>,
}

impl ConfigDir {
    pub fn new() -> Self {
        ConfigDir {
            base_dir: tempfile::tempdir()
                .expect("Failed to create temp dir")
                .into_path(),
            counters: HashMap::new(),
        }
    }

    pub fn create(&mut self, name: &str) -> PathBuf {
        let count = self.counters.entry(name.to_string()).or_insert(0);
        *count += 1;

        let dir_name = format!("{}-{}", name, count);
        let dir_path = self.base_dir.join(dir_name);
        std::fs::create_dir(&dir_path).expect("Failed to create config dir");

        dir_path
    }
}

mod mini_sentry;
mod relay;
//mod consumers;
//mod test_attachments;
//mod test_aws_extension;
//mod test_client_report;
mod test_dynamic_sampling;
//mod test_envelopy;
//mod test_session;

/*
def _add_sampling_config(
    config,
    sample_rate,
    rule_type,
    releases=None,
    user_segments=None,
    environments=None,
):
    """
    Adds a sampling configuration rule to a project configuration
    """
    ds = config["config"].setdefault("sampling", {})
    ds.setdefault("version", 2)
    # We set the rules as empty array, and we add rules to it.
    rules = ds.setdefault("rules", [])

    if rule_type is None:
        rule_type = "trace"
    conditions = []
    field_prefix = "trace." if rule_type == "trace" else "event."
    if releases is not None:
        conditions.append(
            {
                "op": "glob",
                "name": field_prefix + "release",
                "value": releases,
            }
        )
    if user_segments is not None:
        conditions.append(
            {
                "op": "eq",
                "name": field_prefix + "user",
                "value": user_segments,
                "options": {
                    "ignoreCase": True,
                },
            }
        )
    if environments is not None:
        conditions.append(
            {
                "op": "eq",
                "name": field_prefix + "environment",
                "value": environments,
                "options": {
                    "ignoreCase": True,
                },
            }
        )

    rule = {
        "samplingValue": {"type": "sampleRate", "value": sample_rate},
        "type": rule_type,
        "condition": {"op": "and", "inner": conditions},
        "id": len(rules) + 1,
    }
    rules.append(rule)
    return rules
 */

#[derive(Clone)]
struct StateBuilder {
    project_id: ProjectId,
    trusted_relays: Vec<PublicKey>,
    dsn_public_key_config: PublicKeyConfig,
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
    fn new() -> Self {
        Self {
            project_id: ProjectId(42),
            trusted_relays: vec![],
            dsn_public_key_config: {
                let public_key: ProjectKey = Uuid::new_v4().simple().to_string().parse().unwrap();
                let numeric_id = Some(123);

                PublicKeyConfig {
                    public_key,
                    numeric_id,
                }
            },
            sampling_rules: vec![],
            transaction_metrics_version: None,
            outcomes: None,
        }
    }

    fn public_key(&self) -> ProjectKey {
        self.dsn_public_key_config.public_key
    }

    fn enable_outcomes(mut self) -> Self {
        self.outcomes = Some(json!({
            "outcomes": {
            "emit_outcomes": true,
            "batch_size": 1,
            "batch_interval": 1,
            "source": "relay"
        }}));
        self
    }

    fn set_sampling_rule(self, sample_rate: f32, rule_type: RuleType) -> Self {
        let rule = new_sampling_rule(sample_rate, rule_type.into(), vec![], None, None);
        self.add_sampling_rule(rule)
    }

    fn set_transaction_metrics_version(mut self, version: u32) -> Self {
        self.transaction_metrics_version = Some(version);
        self
    }

    fn set_project_id(mut self, id: ProjectId) -> Self {
        self.project_id = id;
        self
    }

    fn add_trusted_relays(mut self, relays: Vec<PublicKey>) -> Self {
        self.trusted_relays.extend(relays);
        self
    }

    fn add_sampling_rules(mut self, rules: Vec<SamplingRule>) -> Self {
        self.sampling_rules.extend(rules);
        self
    }

    fn add_sampling_rule(mut self, rule: SamplingRule) -> Self {
        self.sampling_rules.push(rule);
        self
    }

    fn add_basic_sampling_rule(mut self, rule_type: RuleType, sample_rate: f32) -> Self {
        let rule = new_sampling_rule(sample_rate, Some(rule_type), vec![], None, None);
        self.sampling_rules.push(rule);
        self
    }

    fn build(self) -> ProjectState {
        let last_fetch = Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        let last_change = Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);

        let json = json!({
        "projectId": self.project_id,
        "slug": "python",
        "publicKeys": [self.dsn_public_key_config],
        "rev": "5ceaea8c919811e8ae7daae9fe877901",
        "disabled": false,
        "lastFetch": last_fetch,
        "lastChange": last_change,
        "config": {
            "allowedDomains": ["*"],
            "trustedRelays": self.trusted_relays,
            "transaction_metrics": {
                "version": self.transaction_metrics_version,
            },
            "piiConfig": {
                "rules": {},
                "applications": {
                    "$string": ["@email", "@mac", "@creditcard", "@userpath"],
                    "$object": ["@password"],
                    },
                },
            }
        });

        let mut project_state: ProjectState = serde_json::from_value(json).unwrap();

        let mut sampling_config = SamplingConfig::new();
        sampling_config.rules = self.sampling_rules.clone();
        project_state.config.sampling = Some(ErrorBoundary::Ok(sampling_config));

        if let Some(tv) = self.transaction_metrics_version {
            let mut x = TransactionMetricsConfig::new();
            x.version = tv as u16;
            project_state.config.transaction_metrics = Some(ErrorBoundary::Ok(x));
        }

        project_state
    }
}

impl Default for StateBuilder {
    fn default() -> Self {
        Self::new()
    }
}

fn new_sampling_rule(
    sample_rate: f32,
    rule_type: Option<RuleType>,
    releases: Vec<f32>,
    user_segments: Option<()>,
    environments: Option<()>,
) -> SamplingRule {
    // let releases: Vec<String> = releases.iter().map(|x| x.to_string()).collect();
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

fn get_trace_info(
    trace_id: Uuid,
    public_key: ProjectKey,
    release: Option<String>,
    user_segment: Option<String>,
    client_sample_rate: Option<f32>,
    transaction: Option<String>,
    sampled: Option<bool>,
) -> Value {
    let mut trace_info = Map::new();
    trace_info.insert("trace_id".to_string(), json!(trace_id.to_string()));
    trace_info.insert("public_key".to_string(), json!(public_key));

    if let Some(release) = release {
        trace_info.insert("release".to_string(), json!(release));
    }

    if let Some(user_segment) = user_segment {
        trace_info.insert("user_segment".to_string(), json!(user_segment));
    }

    if let Some(client_sample_rate) = client_sample_rate {
        trace_info.insert("client_sample_rate".to_string(), json!(client_sample_rate));
    }

    if let Some(transaction) = transaction {
        trace_info.insert("transaction".to_string(), json!(transaction));
    }

    if let Some(sampled) = sampled {
        trace_info.insert("sampled".to_string(), json!(sampled));
    }

    Value::Object(trace_info)
}

fn create_transaction_item(
    trace_id: Option<Uuid>,
    event_id: Option<EventId>,
    transaction: Option<&str>,
) -> (Item, Simple, EventId) {
    let trace_id = trace_id.unwrap_or(Uuid::new_v4()).simple();
    let event_id = event_id.unwrap_or(EventId::new());
    let payload = json!(
    {
    "event_id": event_id,
    "type": "transaction",
    "transaction": transaction.unwrap_or("tr1"),
    "start_timestamp": 1597976392.6542819,
    "timestamp": 1597976400.6189718,
    "contexts": {
        "trace": {
            "trace_id": trace_id,
            "span_id": "FA90FDEAD5F74052",
            "type": "trace",
        }
    },
    "spans": [],
    "extra": {"id": event_id},
    });
    let mut item = Item::new(ItemType::Transaction);
    item.set_payload(ContentType::Json, payload.to_string());
    (item, trace_id, event_id)
}

fn merge(mut base: Value, merge_value: Value, keys: Vec<&str>) -> Value {
    dbg!(&base, &merge_value, &keys);
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

///
///
/// we wanna merge two maps of Value
/// we should use get_or_insert_with or smth
///
///
///

fn get_nested_value(val: &Value, path: &str) -> Value {
    let keys: Vec<&str> = path
        .split('/')
        .map(|s| s.trim()) // Trimming whitespace from each key
        .collect();
    keys.iter()
        .fold(val, |acc, key| acc.as_object().unwrap().get(*key).unwrap())
        .clone() // Cloning the final result to get an owned Value
}

fn get_nested_bool(val: &Value, path: &str) -> bool {
    get_nested_value(val, path).as_bool().unwrap()
}

fn get_nested_float(val: &Value, path: &str) -> f64 {
    get_nested_value(val, path).as_f64().unwrap()
}

fn get_nested_int(val: &Value, path: &str) -> i64 {
    get_nested_value(val, path).as_i64().unwrap()
}

fn get_nested_string(val: &Value, path: &str) -> String {
    get_nested_value(val, path).to_string()
}

fn merge_maps(base: &mut Map<String, Value>, mut merge: Map<String, Value>) {
    let key: Vec<&String> = merge.keys().take(1).collect();
    let key = key[0].to_owned();

    dbg!(&key, &base);

    let base_inner = base.get_mut(&key).unwrap().as_object_mut().unwrap();
    let merge_inner = merge.get_mut(&key).unwrap().as_object().unwrap();

    for (key, val) in merge_inner.iter() {
        base_inner.insert(key.to_owned(), val.clone());
    }

    dbg!(&base);
}

fn outcomes_enabled_config() -> Value {
    json!({
        "outcomes": {
            "emit_outcomes": true,
            "batch_size": 1,
            "batch_interval": 1,
            "source": "relay"
    }})
}

fn intovalue_str(x: impl IntoValue) -> String {
    let x: RelayValue = x.into_value();
    let x: Value = x.into();
    serde_json::to_string(&x).unwrap()
}

fn into_item(item_type: ItemType, value: impl IntoValue) -> Item {
    let x: RelayValue = value.into_value();
    let x: Value = x.into();
    let x: String = serde_json::to_string(&x).unwrap();
    let mut item = Item::new(ItemType::Event);
    item.set_payload(ContentType::Json, x);
    item
}

fn opt_create_transaction_item(
    transaction: Option<&str>,
    trace_id: Option<Uuid>,
    event_id: Option<Uuid>,
) -> (RawItem, Uuid, Uuid) {
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

    let item = RawItem::from_json(item).set_type(ItemType::Transaction);
    (item, trace_id, event_id)
}

fn x_create_transaction_item(transaction: Option<&str>) -> (RawItem, Uuid, Uuid) {
    let trace_id = Uuid::new_v4();
    let event_id = Uuid::new_v4();

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

    let item = RawItem::from_json(item).set_type(ItemType::Transaction);
    (item, trace_id, event_id)
}

fn create_transaction_envelope(public_key: ProjectKey) -> EnvelopeBuilder {
    let (item, trace_id, event_id) = create_transaction_item(None, None, None);

    EnvelopeBuilder::new(event_id, public_key)
        .set_item(item)
        .add_trace_info(
            trace_id,
            public_key,
            Some("1.0".to_string()),
            None,
            Some(1.0),
            Some("/transaction".into()),
            Some(true),
        )
}

fn x_create_transaction_envelope(
    public_key: ProjectKey,
    trace_id: Option<Uuid>,
    event_id: Option<Uuid>,
) -> EnvelopeBuilder {
    let event_id = event_id.map(|id| EventId(id));
    let (item, trace_id, event_id) = create_transaction_item(trace_id, event_id, None);

    EnvelopeBuilder::new(event_id, public_key)
        .set_item(item)
        .add_trace_info(
            trace_id,
            public_key,
            Some("1.0".to_string()),
            None,
            Some(1.0),
            Some("/transaction".into()),
            Some(true),
        )
}

fn create_error_envelope(public_key: ProjectKey) -> EnvelopeBuilder {
    let event_id = EventId::new();
    let trace_id = Uuid::new_v4().simple();

    let event = Event {
        ty: EventType::Error.into(),
        environment: "production".to_string().into(),
        id: event_id.into(),
        release: LenientString::from("foo@1.2.3".to_string()).into(),
        ..Default::default()
    };

    let item = into_item(ItemType::Event, event);

    EnvelopeBuilder::new(event_id, public_key)
        .set_item(item)
        .add_trace_info(
            trace_id,
            public_key,
            Some("1.0".to_string()),
            None,
            Some(1.0),
            Some("/transaction".into()),
            Some(true),
        )
}

pub struct EnvelopeBuilder {
    pub headers: EnvelopeHeaders,
    pub items: Vec<Item>,
}

impl From<EnvelopeBuilder> for Envelope {
    fn from(value: EnvelopeBuilder) -> Self {
        value.into_envelope()
    }
}

impl EnvelopeBuilder {
    fn new(event_id: EventId, public_key: ProjectKey) -> Self {
        Self {
            headers: Self::default_headers(event_id, public_key),
            items: vec![],
        }
    }

    fn create(public_key: ProjectKey) -> Self {
        let event_id = EventId::default();
        Self::new(event_id, public_key)
    }

    pub fn add_event(self, payload: &str) -> Self {
        let event = Event {
            logentry: LogEntry::from(payload.to_string()).into(),
            ty: EventType::Error.into(),
            ..Default::default()
        };

        let item = into_item(ItemType::Event, event);

        self.set_item(item)
    }

    fn event_id(&self) -> EventId {
        self.headers.event_id.unwrap()
    }

    fn set_item(mut self, item: Item) -> Self {
        self.items.push(item);
        self
    }
    fn set_headers(&mut self, headers: EnvelopeHeaders) {
        self.headers = headers;
    }

    fn set_event_id(mut self, event_id: EventId) -> Self {
        self.headers.event_id = Some(event_id);
        self
    }

    fn set_item_from_payload(mut self, payload: Value, ty: ItemType) -> Self {
        let mut item = Item::new(ty);
        item.set_payload(ContentType::Json, serde_json::to_vec(&payload).unwrap());
        self
    }

    #[allow(clippy::too_many_arguments)]
    fn add_trace_info(
        mut self,
        trace_id: Simple,
        public_key: ProjectKey,
        release: Option<String>,
        user_segment: Option<String>,
        client_sample_rate: Option<f64>,
        transaction: Option<String>,
        sampled: Option<bool>,
    ) -> Self {
        let mut trace = match self.headers.trace.clone().unwrap_or_default() {
            ErrorBoundary::Ok(t) => t,
            _ => panic!(),
        };

        trace.trace_id = trace_id.into();
        trace.public_key = public_key;

        if release.is_some() {
            trace.release = release;
        }

        if let Some(segment) = user_segment {
            trace.user = TraceUserContext {
                user_segment: segment,
                user_id: String::new(),
            };
        }

        if client_sample_rate.is_some() {
            trace.sample_rate = client_sample_rate;
        }

        if transaction.is_some() {
            trace.transaction = transaction;
        }

        if sampled.is_some() {
            trace.sampled = sampled;
        }

        self.headers.trace = Some(ErrorBoundary::Ok(trace));
        self
    }

    fn default_headers(
        event_id: impl Into<Option<EventId>>,
        public_key: ProjectKey,
    ) -> EnvelopeHeaders {
        EnvelopeHeaders {
            event_id: event_id.into(),
            meta: request_meta(public_key),
            retention: None,
            sent_at: None,
            other: BTreeMap::new(),
            trace: None,
        }
    }

    fn into_envelope(self) -> Envelope {
        let mut x = Items::new();
        for item in self.items {
            x.push(item);
        }
        Envelope {
            headers: self.headers,
            items: x,
        }
    }
}

fn envelope_to_request(envelope: Envelope, scoping: Scoping) -> impl UpstreamRequest {
    let envelope_body = envelope.to_vec().unwrap();
    dbg!(String::from_utf8(envelope_body.clone()).unwrap());
    let (tx, _) = oneshot::channel();

    SendEnvelope {
        envelope_body,
        envelope_meta: envelope.meta().clone(),
        project_cache: Addr::dummy(),
        scoping,
        http_encoding: HttpEncoding::Identity,
        response_sender: tx,
        project_key: scoping.project_key,
        partition_key: None,
    }
}

fn is_envelope_sampled(envelope: &Envelope) -> bool {
    let bytes = &envelope.items().next().unwrap().payload;
    let x: serde_json::Value = serde_json::from_slice(bytes).unwrap();
    dbg!(&x);
    x.get("contexts")
        .unwrap()
        .get("trace")
        .unwrap()
        .get("sampled")
        .unwrap()
        .as_bool()
        .unwrap()
}

fn event_id_from_item(item: &Item) -> EventId {
    let bytes = &item.payload;
    let x: serde_json::Value = serde_json::from_slice(bytes).unwrap();
    let s = x.get("event_id").unwrap().as_str().unwrap();
    EventId::from_str(s).unwrap()
}
