use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use relay_auth::{PublicKey, RelayId};
use relay_config::{Config, RelayInfo};
use relay_system::{
    Addr, BroadcastChannel, BroadcastResponse, BroadcastSender, FromMessage, Interface, Service,
};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::actors::upstream::{Method, RequestPriority, SendQuery, UpstreamQuery, UpstreamRelay};
use crate::utils::{RetryBackoff, SleepHandle};

/// Resolves [`RelayInfo`] by it's [identifier](RelayId).
///
/// This message may fail if the upstream is not reachable repeatedly and Relay information cannot
/// be resolved.
#[derive(Debug)]
pub struct GetRelay {
    /// The unique identifier of the Relay deployment.
    ///
    /// This is part of the Relay credentials file and determined during setup.
    pub relay_id: RelayId,
}

/// Response of a [`GetRelay`] message.
///
/// This is `Some` if the Relay is known by the upstream or `None` the Relay is unknown.
pub type GetRelayResult = Option<RelayInfo>;

/// Manages authentication information for downstream Relays.
#[derive(Debug)]
pub struct RelayCache(GetRelay, BroadcastSender<GetRelayResult>);

impl Interface for RelayCache {}

impl FromMessage<GetRelay> for RelayCache {
    type Response = BroadcastResponse<GetRelayResult>;

    fn from_message(message: GetRelay, sender: BroadcastSender<GetRelayResult>) -> Self {
        Self(message, sender)
    }
}

/// Compatibility format for deserializing [`GetRelaysResponse`] from the legacy endpoint.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PublicKeysResultCompatibility {
    /// DEPRECATED. Legacy format only public key info.
    #[serde(default, rename = "public_keys")]
    pub public_keys: HashMap<RelayId, Option<PublicKey>>,

    /// A map from Relay's identifier to its information.
    ///
    /// Missing entries or explicit `None` both indicate that a Relay with this ID is not known by
    /// the upstream and should not be authenticated.
    #[serde(default)]
    pub relays: HashMap<RelayId, Option<RelayInfo>>,
}

/// Response of the [`GetRelays`] upstream query.
///
/// Former versions of the endpoint returned a different response containing only public keys,
/// defined by [`PublicKeysResultCompatibility`]. Relay's own endpoint is allowed to skip this field
/// and return just the new information.
#[derive(Debug, Serialize, Deserialize)]
pub struct GetRelaysResponse {
    /// A map from Relay's identifier to its information.
    ///
    /// Missing entries or explicit `None` both indicate that a Relay with this ID is not known by
    /// the upstream and should not be authenticated.
    pub relays: HashMap<RelayId, Option<RelayInfo>>,
}

impl From<PublicKeysResultCompatibility> for GetRelaysResponse {
    fn from(relays_info: PublicKeysResultCompatibility) -> Self {
        let relays = if relays_info.relays.is_empty() && !relays_info.public_keys.is_empty() {
            relays_info
                .public_keys
                .into_iter()
                .map(|(id, pk)| (id, pk.map(RelayInfo::new)))
                .collect()
        } else {
            relays_info.relays
        };
        Self { relays }
    }
}

/// Upstream batch query to resolve information for Relays by ID.
#[derive(Debug, Deserialize, Serialize)]
pub struct GetRelays {
    /// A list of Relay deployment identifiers to fetch.
    pub relay_ids: Vec<RelayId>,
}

impl UpstreamQuery for GetRelays {
    type Response = PublicKeysResultCompatibility;

    fn method(&self) -> Method {
        Method::POST
    }

    fn path(&self) -> Cow<'static, str> {
        Cow::Borrowed("/api/0/relays/publickeys/")
    }

    fn priority() -> RequestPriority {
        RequestPriority::High
    }

    fn retry() -> bool {
        false
    }

    fn route(&self) -> &'static str {
        "public_keys"
    }
}

/// Cache entry with metadata.
#[derive(Debug)]
enum RelayState {
    Exists {
        relay: RelayInfo,
        checked_at: Instant,
    },
    DoesNotExist {
        checked_at: Instant,
    },
}

impl RelayState {
    /// Returns `true` if this cache entry is still valid.
    fn is_valid_cache(&self, config: &Config) -> bool {
        match *self {
            RelayState::Exists { checked_at, .. } => {
                checked_at.elapsed() < config.relay_cache_expiry()
            }
            RelayState::DoesNotExist { checked_at } => {
                checked_at.elapsed() < config.cache_miss_expiry()
            }
        }
    }

    /// Returns `Some` if there is an existing entry.
    ///
    /// This entry may be expired; use `is_valid_cache` to verify this.
    fn as_option(&self) -> Option<&RelayInfo> {
        match *self {
            RelayState::Exists { ref relay, .. } => Some(relay),
            _ => None,
        }
    }

    /// Constructs a cache entry from an upstream response.
    fn from_option(option: Option<RelayInfo>) -> Self {
        match option {
            Some(relay) => RelayState::Exists {
                relay,
                checked_at: Instant::now(),
            },
            None => RelayState::DoesNotExist {
                checked_at: Instant::now(),
            },
        }
    }
}

/// Result type of the background fetch task.
///
///  - `Ok`: The task succeeded and information from the response should be inserted into the cache.
///  - `Err`: The task failed and the channels should be placed back for the next fetch.
type FetchResult = Result<GetRelaysResponse, HashMap<RelayId, BroadcastChannel<GetRelayResult>>>;

/// Service implementing the [`RelayCache`] interface.
#[derive(Debug)]
pub struct RelayCacheService {
    static_relays: HashMap<RelayId, RelayInfo>,
    relays: HashMap<RelayId, RelayState>,
    channels: HashMap<RelayId, BroadcastChannel<GetRelayResult>>,
    fetch_channel: (mpsc::Sender<FetchResult>, mpsc::Receiver<FetchResult>),
    backoff: RetryBackoff,
    delay: SleepHandle,
    config: Arc<Config>,
    upstream_relay: Addr<UpstreamRelay>,
}

impl RelayCacheService {
    /// Creates a new [`RelayCache`] service.
    pub fn new(config: Arc<Config>, upstream_relay: Addr<UpstreamRelay>) -> Self {
        Self {
            static_relays: config.static_relays().clone(),
            relays: HashMap::new(),
            channels: HashMap::new(),
            fetch_channel: mpsc::channel(1),
            backoff: RetryBackoff::new(config.http_max_retry_interval()),
            delay: SleepHandle::idle(),
            config,
            upstream_relay,
        }
    }

    /// Returns a clone of the sender for the background fetch task.
    fn fetch_tx(&self) -> mpsc::Sender<FetchResult> {
        let (ref tx, _) = self.fetch_channel;
        tx.clone()
    }

    /// Returns the backoff timeout for a batched upstream query.
    ///
    /// If previous queries succeeded, this will be the general batch interval. Additionally, an
    /// exponentially increasing backoff is used for retrying the upstream request.
    fn next_backoff(&mut self) -> Duration {
        self.config.query_batch_interval() + self.backoff.next_backoff()
    }

    /// Schedules a batched upstream query with exponential backoff.
    fn schedule_fetch(&mut self) {
        let backoff = self.next_backoff();
        self.delay.set(backoff);
    }

    /// Executes an upstream request to fetch information on downstream Relays.
    ///
    /// This assumes that currently no request is running. If the upstream request fails or new
    /// channels are pushed in the meanwhile, this will reschedule automatically.
    fn fetch_relays(&mut self) {
        let channels = std::mem::take(&mut self.channels);
        relay_log::debug!(
            "updating public keys for {} relays (attempt {})",
            channels.len(),
            self.backoff.attempt(),
        );

        let fetch_tx = self.fetch_tx();
        let upstream_relay = self.upstream_relay.clone();
        tokio::spawn(async move {
            let request = GetRelays {
                relay_ids: channels.keys().cloned().collect(),
            };

            let query_result = match upstream_relay.send(SendQuery(request)).await {
                Ok(inner) => inner,
                // Drop the channels to propagate the `SendError` up.
                Err(_send_error) => return,
            };

            let fetch_result = match query_result {
                Ok(response) => {
                    let response = GetRelaysResponse::from(response);

                    for (id, channel) in channels {
                        relay_log::debug!("relay {id} public key updated");
                        let info = response.relays.get(&id).unwrap_or(&None);
                        channel.send(info.clone());
                    }

                    Ok(response)
                }
                Err(error) => {
                    relay_log::error!(
                        error = &error as &dyn std::error::Error,
                        "error fetching public keys"
                    );
                    Err(channels)
                }
            };

            fetch_tx.send(fetch_result).await.ok();
        });
    }

    /// Handles results from the background fetch task.
    fn handle_fetch_result(&mut self, result: FetchResult) {
        match result {
            Ok(response) => {
                self.backoff.reset();

                for (id, info) in response.relays {
                    self.relays.insert(id, RelayState::from_option(info));
                }
            }
            Err(channels) => {
                self.channels.extend(channels);
            }
        }

        if !self.channels.is_empty() {
            self.schedule_fetch();
        }
    }

    /// Resolves information for a Relay and passes it to the sender.
    ///
    /// Sends information immediately if it is available in the cache. Otherwise, this schedules a
    /// delayed background fetch and attaches the sender to a broadcast channel.
    fn get_or_fetch(&mut self, message: GetRelay, sender: BroadcastSender<GetRelayResult>) {
        let relay_id = message.relay_id;

        // First check the statically configured relays
        if let Some(key) = self.static_relays.get(&relay_id) {
            sender.send(Some(key.clone()));
            return;
        }

        if let Some(key) = self.relays.get(&relay_id) {
            if key.is_valid_cache(&self.config) {
                sender.send(key.as_option().cloned());
                return;
            }
        }

        if self.config.credentials().is_none() {
            relay_log::error!(
                "no credentials configured. relay {relay_id} cannot send requests to this relay",
            );
            sender.send(None);
            return;
        }

        relay_log::debug!("relay {relay_id} public key requested");
        self.channels
            .entry(relay_id)
            .or_insert_with(BroadcastChannel::new)
            .attach(sender);

        if !self.backoff.started() {
            self.schedule_fetch();
        }
    }
}

impl Service for RelayCacheService {
    type Interface = RelayCache;

    fn spawn_handler(mut self, mut rx: relay_system::Receiver<Self::Interface>) {
        tokio::spawn(async move {
            relay_log::info!("key cache started");

            loop {
                tokio::select! {
                    // Prioritize flush over receiving messages to prevent starving.
                    biased;

                    Some(result) = self.fetch_channel.1.recv() => self.handle_fetch_result(result),
                    () = &mut self.delay => self.fetch_relays(),
                    Some(message) = rx.recv() => self.get_or_fetch(message.0, message.1),
                    else => break,
                }
            }

            relay_log::info!("key cache stopped");
        });
    }
}
