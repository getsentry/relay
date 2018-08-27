//! This actor caches known public keys.
use std::borrow::Cow;
use std::collections::HashMap;
use std::mem;
use std::sync::Arc;
use std::time::{Duration, Instant};

use actix::prelude::*;
use actix_web::{http::Method, HttpResponse, ResponseError};
use futures::{future, future::Shared, sync::oneshot, Future};

use semaphore_common::{Config, PublicKey, RelayId};

use actors::upstream::{SendQuery, UpstreamQuery, UpstreamRelay};
use utils::{ApiErrorResponse, Response};

#[derive(Fail, Debug)]
#[fail(display = "failed to fetch keys")]
pub struct KeyError;

impl ResponseError for KeyError {
    fn error_response(&self) -> HttpResponse {
        HttpResponse::BadGateway().json(&ApiErrorResponse::from_fail(self))
    }
}

#[derive(Debug)]
enum KeyState {
    Exists {
        public_key: PublicKey,
        checked_at: Instant,
    },
    DoesNotExist {
        checked_at: Instant,
    },
}

impl KeyState {
    fn is_valid_cache(&self, config: &Config) -> bool {
        match *self {
            KeyState::Exists { checked_at, .. } => {
                checked_at.elapsed() < config.relay_cache_expiry()
            }
            KeyState::DoesNotExist { checked_at } => {
                checked_at.elapsed() < config.cache_miss_expiry()
            }
        }
    }

    fn as_option(&self) -> Option<&PublicKey> {
        match *self {
            KeyState::Exists { ref public_key, .. } => Some(public_key),
            _ => None,
        }
    }

    fn from_option(option: Option<PublicKey>) -> Self {
        match option {
            Some(public_key) => KeyState::Exists {
                public_key,
                checked_at: Instant::now(),
            },
            None => KeyState::DoesNotExist {
                checked_at: Instant::now(),
            },
        }
    }
}

#[derive(Debug)]
struct KeyChannel {
    sender: oneshot::Sender<Option<PublicKey>>,
    receiver: Shared<oneshot::Receiver<Option<PublicKey>>>,
}

impl KeyChannel {
    pub fn new() -> Self {
        let (sender, receiver) = oneshot::channel();
        KeyChannel {
            sender,
            receiver: receiver.shared(),
        }
    }

    pub fn send(self, value: Option<PublicKey>) -> Result<(), Option<PublicKey>> {
        self.sender.send(value)
    }

    pub fn receiver(&self) -> Shared<oneshot::Receiver<Option<PublicKey>>> {
        self.receiver.clone()
    }
}

pub struct KeyCache {
    config: Arc<Config>,
    upstream: Addr<UpstreamRelay>,
    keys: HashMap<RelayId, KeyState>,
    key_channels: HashMap<RelayId, KeyChannel>,
    batch_attempts: i32,
}

impl KeyCache {
    pub fn new(config: Arc<Config>, upstream: Addr<UpstreamRelay>) -> Self {
        KeyCache {
            config,
            upstream,
            keys: HashMap::new(),
            key_channels: HashMap::new(),
            batch_attempts: -1,
        }
    }

    fn fetch_keys(&mut self, context: &mut Context<Self>) {
        let channels = mem::replace(&mut self.key_channels, HashMap::new());
        self.batch_attempts += 1;

        debug!(
            "updating public keys for {} relays (attempt {})",
            channels.len(),
            self.batch_attempts,
        );

        let request = GetPublicKeys {
            relay_ids: channels.keys().cloned().collect(),
        };

        self.upstream
            .send(SendQuery(request))
            .into_actor(self)
            .map_err(|_, _, _| KeyError)
            .and_then(|response, actor, context| {
                match response {
                    Ok(mut response) => {
                        for (id, channel) in channels {
                            let key = response.public_keys.remove(&id).unwrap_or(None);
                            actor.keys.insert(id, KeyState::from_option(key.clone()));
                            debug!("relay {} public key updated", id);
                            channel.send(key).ok();
                        }

                        // We have successfully handled a request and can reset the retry count. It
                        // will be set to -1 only if there are no more key_channels to try,
                        // otherwise the next
                        actor.batch_attempts = 0;
                    }
                    Err(error) => {
                        // Put the channels back into the queue, in addition to channels that have
                        // been pushed in the meanwhile. We will retry again shortly.
                        actor.key_channels.extend(channels);
                        error!("error fetching public keys: {}", error);
                    }
                }

                if actor.key_channels.is_empty() {
                    // Since there are no more open channels (nothing we still want to fetch), we
                    // can reset now.
                    actor.batch_attempts = -1;
                } else {
                    // There are more key_channels, either due to an error or due to newly scheduled
                    // requests. Reschedule an upstream request with exponential backoff in addition
                    // to the default batch interval. If our attempt count is zero (due to a
                    // successfully completed upstream request), the backoff will be zero and the
                    // total delay query_batch_interval.
                    let timeout = actor.config.query_batch_interval()
                        + Duration::from_secs(1) * 2u32.pow(actor.batch_attempts as u32 - 1);
                    context.run_later(timeout, Self::fetch_keys);
                }

                future::ok(()).into_actor(actor)
            })
            .drop_err()
            .spawn(context);
    }

    fn get_or_fetch_key(
        &mut self,
        relay_id: RelayId,
        context: &mut Context<Self>,
    ) -> Response<(RelayId, Option<PublicKey>), KeyError> {
        if let Some(key) = self.keys.get(&relay_id) {
            if key.is_valid_cache(&self.config) {
                return Response::ok((relay_id, key.as_option().cloned()));
            }
        }

        debug!("relay {} public key requested", relay_id);
        if self.batch_attempts < 0 {
            // We have not started a batch request yet, so we spawn one and set the attempt counter
            // to zero. This indicates that the request has spawned but not yet started.
            self.batch_attempts = 0;
            context.run_later(self.config.query_batch_interval(), Self::fetch_keys);
        }

        let receiver = self
            .key_channels
            .entry(relay_id)
            .or_insert_with(KeyChannel::new)
            .receiver()
            .map(move |key| (relay_id, (*key).clone()))
            .map_err(|_| KeyError);

        Response::async(receiver)
    }
}

impl Actor for KeyCache {
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Self::Context) {
        info!("key manager started");
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        info!("key manager stopped");
    }
}

#[derive(Debug)]
pub struct GetPublicKey {
    pub relay_id: RelayId,
}

#[derive(Debug)]
pub struct GetPublicKeyResult {
    pub public_key: Option<PublicKey>,
}

impl Message for GetPublicKey {
    type Result = Result<GetPublicKeyResult, KeyError>;
}

impl Handler<GetPublicKey> for KeyCache {
    type Result = Response<GetPublicKeyResult, KeyError>;

    fn handle(&mut self, message: GetPublicKey, context: &mut Self::Context) -> Self::Result {
        self.get_or_fetch_key(message.relay_id, context)
            .map(|(_id, public_key)| GetPublicKeyResult { public_key })
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct GetPublicKeys {
    pub relay_ids: Vec<RelayId>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GetPublicKeysResult {
    pub public_keys: HashMap<RelayId, Option<PublicKey>>,
}

impl Message for GetPublicKeys {
    type Result = Result<GetPublicKeysResult, KeyError>;
}

impl UpstreamQuery for GetPublicKeys {
    type Response = GetPublicKeysResult;

    fn method(&self) -> Method {
        Method::POST
    }

    fn path(&self) -> Cow<'static, str> {
        Cow::Borrowed("/api/0/relays/publickeys/")
    }
}

impl Handler<GetPublicKeys> for KeyCache {
    type Result = Response<GetPublicKeysResult, KeyError>;

    fn handle(&mut self, message: GetPublicKeys, context: &mut Self::Context) -> Self::Result {
        let mut public_keys = HashMap::new();
        let mut key_futures = Vec::new();

        for id in message.relay_ids {
            match self.get_or_fetch_key(id, context) {
                Response::Async(fut) => {
                    key_futures.push(fut);
                }
                Response::Reply(Ok((id, key))) => {
                    public_keys.insert(id, key);
                }
                Response::Reply(Err(_)) => {
                    // Cannot happen
                }
            }
        }

        if key_futures.is_empty() {
            return Response::reply(Ok(GetPublicKeysResult { public_keys }));
        }

        let future = future::join_all(key_futures).map(move |responses| {
            public_keys.extend(responses);
            GetPublicKeysResult { public_keys }
        });

        Response::async(future)
    }
}
