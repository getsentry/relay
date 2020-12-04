//! This actor caches known public keys.
use std::borrow::Cow;
use std::collections::HashMap;
use std::mem;
use std::sync::Arc;
use std::time::{Duration, Instant};

use ::actix::fut;
use ::actix::prelude::*;
use actix_web::{http::Method, HttpResponse, ResponseError};
use failure::Fail;
use futures::{future, future::Shared, sync::oneshot, Future};
use serde::{Deserialize, Serialize};

use relay_auth::{PublicKey, RelayId};
use relay_common::RetryBackoff;
use relay_config::Config;
use relay_log::LogError;

use crate::actors::upstream::{RequestPriority, SendQuery, UpstreamQuery, UpstreamRelay};
use crate::utils::{self, ApiErrorResponse, Response};

#[derive(Fail, Debug)]
#[fail(display = "failed to fetch keys")]
pub enum KeyError {
    #[fail(display = "failed to fetch relay key from upstream")]
    FetchFailed,

    #[fail(display = "could not schedule key fetching")]
    ScheduleFailed(#[cause] MailboxError),
}

impl ResponseError for KeyError {
    fn error_response(&self) -> HttpResponse {
        HttpResponse::BadGateway().json(&ApiErrorResponse::from_fail(self))
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

#[derive(Debug)]
struct RelayInfoChannel {
    sender: oneshot::Sender<Option<RelayInfo>>,
    receiver: Shared<oneshot::Receiver<Option<RelayInfo>>>,
}

impl RelayInfoChannel {
    pub fn new() -> Self {
        let (sender, receiver) = oneshot::channel();
        RelayInfoChannel {
            sender,
            receiver: receiver.shared(),
        }
    }

    pub fn send(self, value: Option<RelayInfo>) -> Result<(), Option<RelayInfo>> {
        self.sender.send(value)
    }

    pub fn receiver(&self) -> Shared<oneshot::Receiver<Option<RelayInfo>>> {
        self.receiver.clone()
    }
}

pub struct RelayCache {
    backoff: RetryBackoff,
    config: Arc<Config>,
    upstream: Addr<UpstreamRelay>,
    relays: HashMap<RelayId, RelayState>,
    relay_channels: HashMap<RelayId, RelayInfoChannel>,
}

impl RelayCache {
    pub fn new(config: Arc<Config>, upstream: Addr<UpstreamRelay>) -> Self {
        RelayCache {
            backoff: RetryBackoff::new(config.http_max_retry_interval()),
            config,
            upstream,
            relays: HashMap::new(),
            relay_channels: HashMap::new(),
        }
    }

    /// Returns the backoff timeout for a batched upstream query.
    ///
    /// If previous queries succeeded, this will be the general batch interval. Additionally, an
    /// exponentially increasing backoff is used for retrying the upstream request.
    fn next_backoff(&mut self) -> Duration {
        self.config.query_batch_interval() + self.backoff.next_backoff()
    }

    /// Schedules a batched upstream query with exponential backoff.
    fn schedule_fetch(&mut self, context: &mut Context<Self>) {
        utils::run_later(self.next_backoff(), Self::fetch_relays).spawn(context)
    }

    /// Executes an upstream request to fetch information on downstream Relays.
    ///
    /// This assumes that currently no request is running. If the upstream request fails or new
    /// channels are pushed in the meanwhile, this will reschedule automatically.
    fn fetch_relays(&mut self, context: &mut Context<Self>) {
        let channels = mem::replace(&mut self.relay_channels, HashMap::new());
        relay_log::debug!(
            "updating public keys for {} relays (attempt {})",
            channels.len(),
            self.backoff.attempt(),
        );

        let request = GetRelays {
            relay_ids: channels.keys().cloned().collect(),
        };

        self.upstream
            .send(SendQuery(request))
            .map_err(KeyError::ScheduleFailed)
            .into_actor(self)
            .and_then(|response, slf, ctx| {
                match response {
                    Ok(response) => {
                        let mut response = GetRelaysResult::from(response);
                        slf.backoff.reset();

                        for (id, channel) in channels {
                            let info = response.relays.remove(&id).unwrap_or(None);
                            slf.relays.insert(id, RelayState::from_option(info.clone()));
                            relay_log::debug!("relay {} public key updated", id);
                            channel.send(info).ok();
                        }
                    }
                    Err(error) => {
                        relay_log::error!("error fetching public keys: {}", LogError(&error));

                        // Put the channels back into the queue, in addition to channels that have
                        // been pushed in the meanwhile. We will retry again shortly.
                        slf.relay_channels.extend(channels);
                    }
                }

                if !slf.relay_channels.is_empty() {
                    slf.schedule_fetch(ctx);
                }

                fut::ok(())
            })
            .drop_err()
            .spawn(context);
    }

    fn get_or_fetch_info(
        &mut self,
        relay_id: RelayId,
        context: &mut Context<Self>,
    ) -> Response<(RelayId, Option<RelayInfo>), KeyError> {
        if let Some(key) = self.relays.get(&relay_id) {
            if key.is_valid_cache(&self.config) {
                return Response::ok((relay_id, key.as_option().cloned()));
            }
        }

        if self.config.credentials().is_none() {
            relay_log::error!(
                "No credentials configured. Relay {} cannot send requests to this relay.",
                relay_id
            );
            return Response::ok((relay_id, None));
        }

        relay_log::debug!("relay {} public key requested", relay_id);
        if !self.backoff.started() {
            self.backoff.reset();
            self.schedule_fetch(context);
        }

        let receiver = self
            .relay_channels
            .entry(relay_id)
            .or_insert_with(RelayInfoChannel::new)
            .receiver()
            .map(move |key| (relay_id, (*key).clone()))
            .map_err(|_| KeyError::FetchFailed);

        Response::future(receiver)
    }
}

impl Actor for RelayCache {
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Self::Context) {
        relay_log::info!("key cache started");
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        relay_log::info!("key cache stopped");
    }
}

#[derive(Debug)]
pub struct GetRelay {
    pub relay_id: RelayId,
}

#[derive(Debug)]
pub struct GetRelayResult {
    pub relay: Option<RelayInfo>,
}

impl Message for GetRelay {
    type Result = Result<GetRelayResult, KeyError>;
}

impl Handler<GetRelay> for RelayCache {
    type Result = Response<GetRelayResult, KeyError>;

    fn handle(&mut self, message: GetRelay, context: &mut Self::Context) -> Self::Result {
        self.get_or_fetch_info(message.relay_id, context)
            .map(|(_id, relay)| GetRelayResult { relay })
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct GetRelays {
    pub relay_ids: Vec<RelayId>,
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

/// Information on a downstream Relay.
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct RelayInfo {
    /// The public key that this Relay uses to authenticate and sign requests.
    pub public_key: PublicKey,

    /// Marks an internal relay that has privileged access to more project configuration.
    #[serde(default)]
    pub internal: bool,
}

impl RelayInfo {
    pub fn new(public_key: PublicKey) -> Self {
        Self {
            public_key,
            internal: false,
        }
    }
}

impl Message for GetRelays {
    type Result = Result<GetRelaysResult, KeyError>;
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

impl Handler<GetRelays> for RelayCache {
    type Result = Response<GetRelaysResult, KeyError>;

    fn handle(&mut self, message: GetRelays, context: &mut Self::Context) -> Self::Result {
        let mut relays = HashMap::new();
        let mut futures = Vec::new();

        for id in message.relay_ids {
            match self.get_or_fetch_info(id, context) {
                Response::Future(fut) => {
                    futures.push(fut);
                }
                Response::Reply(Ok((id, key))) => {
                    relays.insert(id, key);
                }
                Response::Reply(Err(_)) => {
                    // Cannot happen
                }
            }
        }

        if futures.is_empty() {
            return Response::reply(Ok(GetRelaysResult { relays }));
        }

        let future = future::join_all(futures).map(move |responses| {
            relays.extend(responses);
            GetRelaysResult { relays }
        });

        Response::future(future)
    }
}
