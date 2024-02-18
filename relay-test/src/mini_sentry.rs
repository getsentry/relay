use no_deadlocks::Mutex;
use relay_event_schema::protocol::EventId;
use std::collections::HashMap;
use std::io::Read;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use crate::{
    random_port, ProjectResponse, RawEnvelope, RawItem, StateBuilder, DEFAULT_DSN_PUBLIC_KEY,
};
use axum::body::Bytes;
use axum::response::Json;
use axum::routing::{get, post};
use axum::Router;
use flate2::read::GzDecoder;
use relay_auth::{PublicKey, RelayId, RelayVersion, SignedRegisterState};
use relay_base_schema::project::{ProjectId, ProjectKey};
use relay_config::RelayInfo;
use relay_dynamic_config::{ErrorBoundary, GlobalConfig, Options};
use relay_sampling::config::SamplingRule;
use relay_sampling::SamplingConfig;
use relay_server::envelope::Envelope as RelayEnvelope;
use relay_server::envelope::ItemType;
use relay_server::services::outcome::OutcomeId;
use relay_server::services::outcome::TrackRawOutcome;
use relay_server::services::project::ProjectState;
use relay_server::services::project::PublicKeyConfig;
use serde_json::{json, Value};
use std::future::Future;
use tokio::runtime::Runtime;
use uuid::Uuid;

#[derive(Default, Clone)]
pub struct CapturedOutcomes {
    inner: Arc<Mutex<Vec<TrackRawOutcome>>>,
}

impl CapturedOutcomes {
    pub fn is_empty(&self) -> bool {
        self.inner.lock().unwrap().is_empty()
    }

    pub fn clear(&self) -> &Self {
        self.inner.lock().unwrap().clear();
        self
    }

    pub fn pop(&self) -> Option<TrackRawOutcome> {
        self.inner.lock().unwrap().pop()
    }

    pub fn extend(&self, outcomes: Vec<TrackRawOutcome>) {
        self.inner.lock().unwrap().extend(outcomes);
    }

    pub fn push(&self, outcome: TrackRawOutcome) {
        self.inner.lock().unwrap().push(outcome);
    }

    pub fn take_index(&self, idx: usize) -> TrackRawOutcome {
        self.inner.lock().unwrap().remove(idx)
    }

    pub fn assert_outcome_qty(&self, qty: usize) -> &Self {
        assert_eq!(self.inner.lock().unwrap().len(), qty);
        self
    }

    pub fn assert_all_outcome_reasons(&self, reason: &str) -> &Self {
        let guard = self.inner.lock().unwrap();

        for outcome in guard.iter() {
            assert_eq!(outcome.reason.clone().unwrap().as_str(), reason);
        }
        self
    }

    pub fn assert_all_outcome_id(&self, ty: OutcomeId) -> &Self {
        let guard = self.inner.lock().unwrap();

        for outcome in guard.iter() {
            assert_eq!(outcome.outcome, ty);
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
    inner: Arc<Mutex<Vec<RawEnvelope>>>,
}

impl CapturedEnvelopes {
    pub fn is_empty(&self) -> bool {
        self.inner.lock().unwrap().is_empty()
    }

    pub fn clear(&self) -> &Self {
        self.inner.lock().unwrap().clear();
        self
    }

    pub fn pop(&self) -> Option<RawEnvelope> {
        self.inner.lock().unwrap().pop()
    }

    pub fn push(&self, envelope: RawEnvelope) {
        self.inner.lock().unwrap().push(envelope);
    }

    pub fn get_index(&self, idx: usize) -> RawEnvelope {
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
    pub fn assert_all_item_types(&self, ty: ItemType) -> &Self {
        for item in self.get_items() {
            assert_eq!(item.ty(), ty);
        }

        self
    }

    pub fn get_envelopes(&self) -> Vec<RawEnvelope> {
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

    pub fn assert_n_item_types(&self, ty: ItemType, n: usize) -> &Self {
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
        dbg!(self.get_envelopes());
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
            if let Some(id) = item.event_id() {
                if id == event_id {
                    return self;
                }
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
    pub known_relays: HashMap<RelayId, RelayInfo>,
    server_handle: Option<tokio::task::JoinHandle<()>>,
    runtime: Runtime,
    project_configs: HashMap<ProjectId, ProjectState>,
    pub global_config: GlobalConfig,
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
        let project_id = project_state.project_id.unwrap();
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

impl MiniSentry {
    pub fn insert_known_relay(&self, relay_id: Uuid, public_key: PublicKey) {
        self.inner.lock().unwrap().known_relays.insert(
            relay_id,
            RelayInfo {
                public_key,
                internal: true,
            },
        );
    }

    pub fn get_captured_envelopes(&self, timeout: u64) -> Option<CapturedEnvelopes> {
        let mut i = 0;
        loop {
            let envelopes = self.inner.lock().unwrap().captured_envelopes.clone();
            if !envelopes.is_empty() {
                return Some(envelopes);
            }
            std::thread::sleep(Duration::from_secs(1));
            i += 1;

            if i == timeout {
                return None;
            }
        }
    }

    pub fn set_global_options(self, options: Options) -> Self {
        self.inner.lock().unwrap().global_config.options = options;
        self
    }

    pub fn captured_envelopes(&self) -> CapturedEnvelopes {
        self.inner.lock().unwrap().captured_envelopes.clone()
    }

    pub fn captured_outcomes(&self) -> CapturedOutcomes {
        self.inner.lock().unwrap().captured_outcomes.clone()
    }

    fn _take_n_envelopes<const N: usize>(&self) -> [RawEnvelope; N] {
        let envelopes = self.captured_envelopes().get_envelopes();
        assert_eq!(envelopes.len(), N);

        envelopes.try_into().unwrap()
    }

    pub fn add_sampling_rule(self, rule: SamplingRule) -> Self {
        let mut inner = self.inner.lock().unwrap();
        assert_eq!(inner.project_configs.len(), 1);
        let project_config = inner.project_configs.values_mut().next().unwrap();

        if let Some(ErrorBoundary::Ok(sam)) = project_config.config.sampling.as_mut() {
            sam.rules.push(rule);
        } else {
            // Directly modifying the original Option in project_config.config.sampling
            project_config.config.sampling = Some(ErrorBoundary::Ok({
                let mut new_sampling_config = SamplingConfig::new();
                new_sampling_config.rules.push(rule);
                new_sampling_config
            }));
        }
        drop(inner);

        self
    }

    pub fn add_basic_project_state(self) -> Self {
        self.inner
            .lock()
            .unwrap()
            .add_project_state(StateBuilder::new().build());
        self
    }

    pub fn add_project_state(self, project_state: impl Into<ProjectState>) -> Self {
        self.inner
            .lock()
            .unwrap()
            .add_project_state(project_state.into());
        self
    }

    /// Returns the public key of the '42' project.
    pub fn public_key(&self) -> ProjectKey {
        self.inner
            .lock()
            .unwrap()
            .project_configs
            .get(&ProjectId::new(42))
            .as_ref()
            .unwrap()
            .public_keys[0]
            .clone()
            .public_key
    }

    pub fn get_dsn_public_key_configs(&self, project_id: ProjectId) -> Option<PublicKeyConfig> {
        let binding = self.inner.lock().unwrap();
        let x = binding.project_configs.get(&project_id)?;
        x.public_keys[0].clone().into()
    }

    pub fn new() -> Self {
        let port = random_port();
        let addr = SocketAddr::from(([127, 0, 0, 1], port));

        // Initialize your mini_sentry state here
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
            .route("/", get(handler))
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

        println!("Listening on {}", addr);

        // Use the runtime inside MiniSentry to spawn the server task
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
            let outcomes: Vec<TrackRawOutcome> = serde_json::from_slice::<Value>(&bytes)
                .unwrap()
                .get("outcomes")
                .unwrap()
                .as_array()
                .unwrap()
                .iter()
                .map(|val| serde_json::from_value(val.clone()).unwrap())
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
) -> impl Fn(Bytes) -> Pin<Box<dyn Future<Output = Json<GetRelaysResponse>> + Send>> + Clone {
    move |bytes| {
        let mini_sentry = mini_sentry.clone();

        Box::pin(async move {
            let x = serde_json::from_slice::<Value>(&bytes).unwrap();

            let mut keys = HashMap::new();
            let mut relays = HashMap::new();

            for id in x
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

            let x = GetRelaysResponse { relays };

            Json(x)
        })
    }
}

use relay_server::services::relays::GetRelaysResponse;

fn make_handle_project_config(
    mini_sentry: Arc<Mutex<MiniSentryInner>>,
) -> impl Fn(Bytes) -> Pin<Box<dyn Future<Output = Json<ProjectResponse>> + Send>> + Clone {
    move |_bytes| {
        let mini_sentry = mini_sentry.clone();

        Box::pin(async move {
            let mut configs = HashMap::new();

            for project_state in mini_sentry.lock().unwrap().project_configs.values() {
                let key = project_state.public_keys[0].public_key;
                configs.insert(key, ErrorBoundary::Ok(Some(project_state.clone())));
            }

            let global = Some(mini_sentry.lock().unwrap().global_config.clone());

            let response = ProjectResponse {
                configs,
                pending: vec![],
                global,
            };
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
            let decompressed = decompress(&bytes).unwrap_or(bytes.to_vec());
            let envelope: RelayEnvelope = *RelayEnvelope::parse_bytes(decompressed.into()).unwrap();
            let envelope = RawEnvelope::from_envelope(&envelope);
            mini_sentry
                .lock()
                .unwrap()
                .captured_envelopes
                .push(envelope);

            Json("ok")
        })
    }
}

async fn handler() -> &'static str {
    "Hello, mini_sentry!"
}

fn register_response() -> Value {
    let relay_id = Uuid::new_v4();
    let token = SignedRegisterState("abc".into());
    let version = RelayVersion::current();

    json!({
        "relay_id": relay_id,
        "token": token,
        "version": version,
    })
}

fn make_handle_register_challenge(
    mini_sentry: Arc<Mutex<MiniSentryInner>>,
) -> impl Fn(Bytes) -> Pin<Box<dyn Future<Output = Json<Value>> + Send>> + Clone {
    move |bytes| {
        let mini_sentry = mini_sentry.clone();

        Box::pin(async move {
            let x = serde_json::from_slice::<Value>(&bytes).unwrap();

            let relay_id: RelayId = x
                .as_object()
                .unwrap()
                .get("relay_id")
                .unwrap()
                .as_str()
                .unwrap()
                .parse()
                .unwrap();

            let public_key: PublicKey = x
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
                "token": SignedRegisterState("123 foobar".into()),
            }))
        })
    }
}

async fn is_live() -> &'static str {
    "is_healthy: true"
}
