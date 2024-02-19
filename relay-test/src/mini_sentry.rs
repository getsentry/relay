use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::io::Read;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use axum::body::Bytes;
use axum::response::Json;
use axum::routing::{get, post};
use axum::Router;
use flate2::read::GzDecoder;
use serde_json::{json, Value};
use tokio::runtime::Runtime;
use uuid::Uuid;

use relay_auth::{PublicKey, RelayId, RelayVersion};
use relay_base_schema::project::{ProjectId, ProjectKey};
use relay_config::RelayInfo;
use relay_dynamic_config::GlobalConfig;
use relay_event_schema::protocol::EventId;

use crate::{random_port, Envelope, Outcome, ProjectState, RawItem, Upstream};

pub const DEFAULT_DSN_PUBLIC_KEY: &str = "31a5a894b4524f74a9a8d0e27e21ba91";

#[derive(Default, Clone)]
pub struct CapturedOutcomes {
    inner: Arc<Mutex<Vec<Outcome>>>,
}

impl CapturedOutcomes {
    pub fn is_empty(&self) -> bool {
        self.inner.lock().unwrap().is_empty()
    }

    pub fn clear(&self) -> &Self {
        self.inner.lock().unwrap().clear();
        self
    }

    pub fn pop(&self) -> Option<Outcome> {
        self.inner.lock().unwrap().pop()
    }

    pub fn extend(&self, outcomes: Vec<Outcome>) {
        self.inner.lock().unwrap().extend(outcomes);
    }

    pub fn push(&self, outcome: Outcome) {
        self.inner.lock().unwrap().push(outcome);
    }

    pub fn take_index(&self, idx: usize) -> Outcome {
        self.inner.lock().unwrap().remove(idx)
    }

    pub fn assert_outcome_qty(&self, qty: usize) -> &Self {
        assert_eq!(self.inner.lock().unwrap().len(), qty);
        self
    }

    pub fn assert_all_outcome_reasons(&self, reason: &str) -> &Self {
        let guard = self.inner.lock().unwrap();

        for outcome in guard.iter() {
            assert_eq!(outcome.reason(), reason);
        }
        self
    }

    pub fn assert_all_outcome_id(&self, outcome_id: u64) -> &Self {
        let guard = self.inner.lock().unwrap();

        for outcome in guard.iter() {
            assert_eq!(outcome.outcome(), outcome_id);
        }
        self
    }

    pub fn wait_for_outcome(&self, timeout: u64) -> &Self {
        for _ in 0..timeout {
            if !self.is_empty() {
                return self;
            }
            std::thread::sleep(Duration::from_secs(1));
        }

        panic!("timed out while waiting for outcome");
    }

    pub fn wait(&self, secs: u64) -> &Self {
        std::thread::sleep(Duration::from_secs(secs));
        self
    }

    pub fn assert_empty(&self) -> &Self {
        self.assert_outcome_qty(0);
        self
    }

    pub fn assert_not_empty(&self) -> &Self {
        assert!(!self.inner.lock().unwrap().is_empty());
        self
    }
}

#[derive(Default, Clone)]
pub struct CapturedEnvelopes {
    inner: Arc<Mutex<Vec<Envelope>>>,
}

impl CapturedEnvelopes {
    pub fn is_empty(&self) -> bool {
        self.inner.lock().unwrap().is_empty()
    }

    pub fn clear(&self) -> &Self {
        self.inner.lock().unwrap().clear();
        self
    }

    pub fn pop(&self) -> Option<Envelope> {
        self.inner.lock().unwrap().pop()
    }

    pub fn push(&self, envelope: Envelope) {
        self.inner.lock().unwrap().push(envelope);
    }

    pub fn get_index(&self, idx: usize) -> Envelope {
        self.inner.lock().unwrap().remove(idx)
    }

    pub fn assert_all_sampled_status(&self, sampled_status: bool) -> &Self {
        for item in self.get_items() {
            assert_eq!(item.sampled().unwrap(), sampled_status);
        }

        self
    }

    pub fn assert_envelope_qty(&self, qty: usize) -> &Self {
        assert_eq!(self.inner.lock().unwrap().len(), qty);
        self
    }

    pub fn assert_item_qty(&self, qty: usize) -> &Self {
        assert_eq!(self.get_items().len(), qty);
        self
    }

    /// Fails if any itemtype is different than the given item type.
    pub fn assert_all_item_types(&self, ty: &str) -> &Self {
        for item in self.get_items() {
            assert_eq!(item.ty(), ty);
        }

        self
    }

    pub fn get_envelopes(&self) -> Vec<Envelope> {
        let guard = self.inner.lock().unwrap();

        let mut events = vec![];
        for envelope in guard.iter() {
            events.push(envelope.clone());
        }
        events
    }

    pub fn wait_for_n_envelope(&self, n: usize, timeout: u64) -> &Self {
        for _ in 0..timeout {
            if self.inner.lock().unwrap().len() >= n {
                return self;
            }
            std::thread::sleep(Duration::from_secs(1));
        }

        panic!("timed out while waiting for envelope");
    }

    fn get_items(&self) -> Vec<RawItem> {
        let mut items = vec![];

        for envelope in self.get_envelopes() {
            for item in envelope.items {
                items.push(item);
            }
        }

        items
    }

    pub fn assert_n_item_types(&self, ty: &str, n: usize) -> &Self {
        let mut matches = 0;

        for item in self.get_items() {
            if item.ty() == ty {
                matches += 1;
            }
        }

        assert_eq!(matches, n);
        self
    }

    pub fn wait_for_envelope(&self, timeout: u64) -> &Self {
        self.wait_for_n_envelope(1, timeout)
    }

    pub fn debug(&self) -> &Self {
        println!("{:?}", self.get_envelopes());
        self
    }

    pub fn wait(&self, secs: u64) -> &Self {
        std::thread::sleep(Duration::from_secs(secs));
        self
    }

    pub fn assert_empty(&self) -> &Self {
        self.assert_envelope_qty(0)
    }

    /// Checks if any item corresponds to the given event id.
    pub fn assert_contains_event_id(&self, event_id: EventId) -> &Self {
        for item in self.get_items() {
            if item.event_id() == Some(event_id) {
                return self;
            }
        }

        panic!("No items with event id: {}", event_id);
    }
}

pub struct MiniSentry {
    pub inner: Arc<Mutex<MiniSentryInner>>,
}

pub struct MiniSentryInner {
    server_address: SocketAddr,
    captured_envelopes: CapturedEnvelopes,
    captured_outcomes: CapturedOutcomes,
    known_relays: HashMap<RelayId, RelayInfo>,
    server_handle: Option<tokio::task::JoinHandle<()>>,
    runtime: Runtime,
    project_configs: HashMap<ProjectId, ProjectState>,
    global_config: GlobalConfig,
}

impl MiniSentryInner {
    pub fn internal_error_dsn(&self) -> String {
        format!(
            "http://{}@{}:{}/666",
            DEFAULT_DSN_PUBLIC_KEY,
            self.server_address.ip(),
            self.server_address.port()
        )
    }

    fn add_project_state(&mut self, project_state: ProjectState) {
        let project_id = project_state.project_id();
        self.project_configs.insert(project_id, project_state);
    }

    pub fn url(&self) -> String {
        format!("http://127.0.0.1:{}", self.server_address.port())
    }
}

impl Default for MiniSentry {
    fn default() -> Self {
        Self::new()
    }
}

impl Upstream for MiniSentry {
    fn url(&self) -> String {
        self.inner.lock().unwrap().url()
    }

    fn internal_error_dsn(&self) -> String {
        self.inner
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .internal_error_dsn()
    }

    fn insert_known_relay(&self, relay_id: Uuid, public_key: PublicKey) {
        self.inner.lock().unwrap().known_relays.insert(
            relay_id,
            RelayInfo {
                public_key,
                internal: true,
            },
        );
    }

    fn public_dsn_key(&self, id: ProjectId) -> ProjectKey {
        self.inner
            .lock()
            .unwrap()
            .project_configs
            .get(&id)
            .unwrap()
            .public_key()
    }
}

impl MiniSentry {
    pub fn captured_envelopes(&self) -> CapturedEnvelopes {
        self.inner.lock().unwrap().captured_envelopes.clone()
    }

    pub fn captured_outcomes(&self) -> CapturedOutcomes {
        self.inner.lock().unwrap().captured_outcomes.clone()
    }

    pub fn add_basic_project_state(self) -> Self {
        self.add_project_state(ProjectState::default())
    }

    pub fn add_project_state(self, project_state: ProjectState) -> Self {
        self.inner.lock().unwrap().add_project_state(project_state);
        self
    }

    pub fn new() -> Self {
        let port = random_port();
        let addr = SocketAddr::from(([127, 0, 0, 1], port));

        let mini_sentry = Arc::new(Mutex::new(MiniSentryInner {
            server_address: addr,
            captured_envelopes: Default::default(),
            captured_outcomes: Default::default(),
            known_relays: HashMap::new(),
            server_handle: None,
            runtime: Runtime::new().unwrap(),
            project_configs: HashMap::new(),
            global_config: Default::default(),
        }));

        let mini_sentry = Self { inner: mini_sentry };

        let envelope_handler = make_handle_envelope(mini_sentry.inner.clone());
        let config_handler = make_handle_project_config(mini_sentry.inner.clone());
        let outcome_handler = make_handle_outcomes(mini_sentry.inner.clone());
        let public_key_handler = make_handle_public_keys(mini_sentry.inner.clone());
        let challenge_handler = make_handle_register_challenge(mini_sentry.inner.clone());

        let router = Router::new()
            .route("/", get("hello minisentry"))
            .route("/api/0/relays/live/", get(is_live))
            .route("/api/42/envelope/", post(envelope_handler))
            .route("/api/0/relays/register/challenge/", post(challenge_handler))
            .route(
                "/api/0/relays/register/response/",
                post(|| async { Json(register_response()) }),
            )
            .route("/api/0/relays/outcomes/", post(outcome_handler))
            .route("/api/0/relays/publickeys/", post(public_key_handler))
            .route("/api/0/relays/projectconfigs/", post(config_handler));

        println!("MiniSentry listening on {}", addr);

        let server_handle = mini_sentry.inner.lock().unwrap().runtime.spawn(async move {
            axum::Server::bind(&addr)
                .serve(router.into_make_service())
                .await
                .unwrap();
        });

        mini_sentry.inner.lock().unwrap().server_handle = Some(server_handle);

        mini_sentry
    }
}

fn decompress(data: &[u8]) -> Result<Vec<u8>, std::io::Error> {
    let mut decoder = GzDecoder::new(data);
    let mut decompressed_data = Vec::new();
    decoder.read_to_end(&mut decompressed_data)?;
    Ok(decompressed_data)
}

fn make_handle_outcomes(
    mini_sentry: Arc<Mutex<MiniSentryInner>>,
) -> impl Fn(Bytes) -> Pin<Box<dyn Future<Output = Json<&'static str>> + Send>> + Clone {
    move |bytes| {
        let mini_sentry = mini_sentry.clone();

        Box::pin(async move {
            let outcomes: Vec<Outcome> = serde_json::from_slice::<Value>(&bytes)
                .unwrap()
                .get("outcomes")
                .unwrap()
                .as_array()
                .unwrap()
                .iter()
                .map(|val| Outcome::new(val.clone()))
                .collect();

            mini_sentry
                .lock()
                .unwrap()
                .captured_outcomes
                .extend(outcomes);

            Json("ok")
        })
    }
}

fn make_handle_public_keys(
    mini_sentry: Arc<Mutex<MiniSentryInner>>,
) -> impl Fn(Bytes) -> Pin<Box<dyn Future<Output = Json<Value>> + Send>> + Clone {
    move |bytes| {
        let mini_sentry = mini_sentry.clone();

        Box::pin(async move {
            let get_relays = serde_json::from_slice::<Value>(&bytes).unwrap();

            let mut keys = HashMap::new();
            let mut relays = HashMap::new();

            for id in get_relays
                .as_object()
                .unwrap()
                .get("relay_ids")
                .unwrap()
                .as_array()
                .unwrap()
            {
                let relay_id: RelayId = id.as_str().unwrap().parse().unwrap();
                let guard = mini_sentry.lock().unwrap();
                if let Some(relay) = guard.known_relays.get(&relay_id).cloned() {
                    keys.insert(relay_id, Some(relay.public_key.clone()));
                    relays.insert(relay_id, Some(relay));
                }
            }

            Json(json!( { "relays": relays }))
        })
    }
}

fn make_handle_project_config(
    mini_sentry: Arc<Mutex<MiniSentryInner>>,
) -> impl Fn(Bytes) -> Pin<Box<dyn Future<Output = Json<Value>> + Send>> + Clone {
    move |bytes| {
        let mini_sentry = mini_sentry.clone();

        Box::pin(async move {
            let mut configs = BTreeMap::new();

            let binding = serde_json::from_slice::<Value>(&bytes).unwrap();
            let get_configs = binding.as_object().unwrap();
            let guard = mini_sentry.lock().unwrap();

            let global = if get_configs.get("global").is_some() {
                Some(guard.global_config.clone())
            } else {
                None
            };

            for project_key in get_configs.get("publicKeys").unwrap().as_array().unwrap() {
                let key = ProjectKey::parse(project_key.to_string().trim_matches('"')).unwrap();

                for project_state in guard.project_configs.values() {
                    let state_key = project_state.public_key();

                    if state_key == key {
                        configs.insert(key, project_state.clone());
                    }
                }
            }

            let response = json!({
                "configs": configs,
                "global": global,
            });

            Json(response)
        })
    }
}

fn make_handle_envelope(
    mini_sentry: Arc<Mutex<MiniSentryInner>>,
) -> impl Fn(Bytes) -> Pin<Box<dyn Future<Output = Json<&'static str>> + Send>> + Clone {
    move |bytes| {
        let mini_sentry = mini_sentry.clone();
        Box::pin(async move {
            let decompressed = decompress(&bytes).unwrap();
            let envelope = Envelope::from_utf8(decompressed);
            mini_sentry
                .lock()
                .unwrap()
                .captured_envelopes
                .push(envelope);

            Json("ok")
        })
    }
}

fn register_response() -> Value {
    let relay_id = Uuid::new_v4();
    let version = RelayVersion::current();

    json!({
        "relay_id": relay_id,
        "token": "abc",
        "version": version,
    })
}

fn make_handle_register_challenge(
    mini_sentry: Arc<Mutex<MiniSentryInner>>,
) -> impl Fn(Bytes) -> Pin<Box<dyn Future<Output = Json<Value>> + Send>> + Clone {
    move |bytes| {
        let mini_sentry = mini_sentry.clone();

        Box::pin(async move {
            let val = serde_json::from_slice::<Value>(&bytes).unwrap();

            let relay_id: RelayId = val
                .as_object()
                .unwrap()
                .get("relay_id")
                .unwrap()
                .as_str()
                .unwrap()
                .parse()
                .unwrap();

            let public_key: PublicKey = val
                .as_object()
                .unwrap()
                .get("public_key")
                .unwrap()
                .as_str()
                .unwrap()
                .parse()
                .unwrap();

            let relay_info = RelayInfo {
                public_key,
                internal: true,
            };
            mini_sentry
                .lock()
                .unwrap()
                .known_relays
                .insert(relay_id, relay_info);

            Json(json! ({
                "relay_id": relay_id,
                "token": "123 foobar",
            }))
        })
    }
}

async fn is_live() -> &'static str {
    "is_healthy: true"
}
