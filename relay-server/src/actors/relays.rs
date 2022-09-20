use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use actix::SystemService;
use actix_web::http::Method;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use relay_auth::{PublicKey, RelayId};
use relay_common::RetryBackoff;
use relay_config::{Config, RelayInfo};
use relay_log::LogError;
use relay_system::{compat, Addr, AsyncResponse, FromMessage, Interface, Sender, Service};

use crate::actors::upstream::{RequestPriority, SendQuery, UpstreamQuery, UpstreamRelay};
use crate::service::REGISTRY;
use crate::utils::SleepHandle;

#[derive(Debug)]
pub struct GetRelay {
    pub relay_id: RelayId,
}

pub type GetRelayResult = Option<RelayInfo>;

#[derive(Debug)]
pub struct RelayCache(GetRelay, Sender<GetRelayResult>);

impl RelayCache {
    pub fn from_registry() -> Addr<Self> {
        REGISTRY.get().unwrap().relay_cache.clone()
    }
}

impl Interface for RelayCache {}

impl FromMessage<GetRelay> for RelayCache {
    type Response = AsyncResponse<GetRelayResult>;

    fn from_message(message: GetRelay, sender: Sender<GetRelayResult>) -> Self {
        Self(message, sender)
    }
}

/// Defines a compatibility format for deserializing relays info that supports
/// both the old and the new format for relay info
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PublicKeysResultCompatibility {
    /// DEPRECATED. Legacy format only public key info.
    #[serde(default, rename = "public_keys")]
    pub public_keys: HashMap<RelayId, Option<PublicKey>>,
    /// A map from Relay's identifier to its information.
    #[serde(default)]
    pub relays: HashMap<RelayId, Option<RelayInfo>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GetRelaysResult {
    /// new format public key plus additional parameters
    pub relays: HashMap<RelayId, Option<RelayInfo>>,
}

impl From<PublicKeysResultCompatibility> for GetRelaysResult {
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

#[derive(Debug, Deserialize, Serialize)]
pub struct GetRelays {
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
}

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

    fn as_option(&self) -> Option<&RelayInfo> {
        match *self {
            RelayState::Exists { ref relay, .. } => Some(relay),
            _ => None,
        }
    }

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

type FetchResult = Result<GetRelaysResult, HashMap<RelayId, Vec<Sender<GetRelayResult>>>>;

#[derive(Debug)]
pub struct RelayCacheService {
    static_relays: HashMap<RelayId, RelayInfo>,
    relays: HashMap<RelayId, RelayState>,
    senders: HashMap<RelayId, Vec<Sender<GetRelayResult>>>,
    fetch_channel: (mpsc::Sender<FetchResult>, mpsc::Receiver<FetchResult>),
    backoff: RetryBackoff,
    delay: SleepHandle,
    config: Arc<Config>,
}

impl RelayCacheService {
    pub fn new(config: Arc<Config>) -> Self {
        Self {
            static_relays: config.static_relays().clone(),
            relays: HashMap::new(),
            senders: HashMap::new(),
            fetch_channel: mpsc::channel(1),
            backoff: RetryBackoff::new(config.http_max_retry_interval()),
            delay: SleepHandle::idle(),
            config,
        }
    }

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
        let channels = std::mem::take(&mut self.senders);
        relay_log::debug!(
            "updating public keys for {} relays (attempt {})",
            channels.len(),
            self.backoff.attempt(),
        );

        let fetch_tx = self.fetch_tx();
        tokio::spawn(async move {
            let request = GetRelays {
                relay_ids: channels.keys().cloned().collect(),
            };

            let upstream = UpstreamRelay::from_registry();
            let query_result = match compat::send(upstream, SendQuery(request)).await {
                Ok(inner) => inner,
                // Drop the senders to propagate the SendError up.
                Err(_send_error) => return,
            };

            let fetch_result = match query_result {
                Ok(response) => {
                    let response = GetRelaysResult::from(response);

                    for (id, channels) in channels {
                        relay_log::debug!("relay {} public key updated", id);
                        let info = response.relays.get(&id).unwrap_or(&None);
                        for channel in channels {
                            channel.send(info.clone());
                        }
                    }

                    Ok(response)
                }
                Err(error) => {
                    relay_log::error!("error fetching public keys: {}", LogError(&error));
                    Err(channels)
                }
            };

            fetch_tx.send(fetch_result).await.ok();
        });
    }

    fn handle_fetch_result(&mut self, result: FetchResult) {
        match result {
            Ok(response) => {
                self.backoff.reset();

                for (id, info) in response.relays {
                    self.relays.insert(id, RelayState::from_option(info));
                }
            }
            Err(channels) => {
                self.senders.extend(channels);
            }
        }

        if !self.senders.is_empty() {
            self.schedule_fetch();
        }
    }

    fn get_or_fetch(&mut self, message: GetRelay, sender: Sender<GetRelayResult>) {
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
                "No credentials configured. Relay {} cannot send requests to this relay.",
                relay_id
            );
            sender.send(None);
            return;
        }

        relay_log::debug!("relay {} public key requested", relay_id);
        self.senders
            .entry(relay_id)
            .or_insert_with(Vec::new)
            .push(sender);

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
                    biased;

                    Some(result) = self.fetch_channel.1.recv() => self.handle_fetch_result(result),
                    Some(message) = rx.recv() => self.get_or_fetch(message.0, message.1),
                    () = &mut self.delay => self.fetch_relays(),
                    else => break,
                }
            }

            relay_log::info!("key cache stopped");
        });
    }
}
