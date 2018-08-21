//! This actor caches known public keys.
use std::borrow::Cow;
use std::collections::HashMap;
use std::mem;
use std::time::{Duration, Instant};

use actix::fut::wrap_future;
use actix::Actor;
use actix::ActorFuture;
use actix::Addr;
use actix::AsyncContext;
use actix::Context;
use actix::Handler;
use actix::Message;
use actix::Response;

use actix_web::http::Method;
use actix_web::HttpResponse;
use actix_web::ResponseError;

use futures::future::Shared;
use futures::sync::oneshot;
use futures::{future, Future};

use semaphore_aorta::ApiErrorResponse;
use semaphore_aorta::PublicKey;
use semaphore_aorta::RelayId;

use actors::upstream::SendRequest;
use actors::upstream::UpstreamRelay;
use actors::upstream::UpstreamRequest;
use constants::{BATCH_TIMEOUT, MISSING_PUBLIC_KEY_EXPIRY, PUBLIC_KEY_EXPIRY};

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
    fn is_valid_cache(&self) -> bool {
        match *self {
            KeyState::Exists {
                public_key: _,
                checked_at,
            } => checked_at.elapsed().as_secs() < PUBLIC_KEY_EXPIRY,
            KeyState::DoesNotExist { checked_at } => {
                checked_at.elapsed().as_secs() < MISSING_PUBLIC_KEY_EXPIRY
            }
        }
    }

    fn as_option(&self) -> Option<&PublicKey> {
        match *self {
            KeyState::Exists {
                ref public_key,
                checked_at: _,
            } => Some(public_key),
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

enum KeyResponse {
    Reply(RelayId, Option<PublicKey>),
    Async(Box<Future<Item = (RelayId, Option<PublicKey>), Error = KeyError>>),
}

pub struct KeyManager {
    upstream: Addr<UpstreamRelay>,
    keys: HashMap<RelayId, KeyState>,
    key_channels: HashMap<RelayId, KeyChannel>,
}

impl KeyManager {
    pub fn new(upstream: Addr<UpstreamRelay>) -> Self {
        KeyManager {
            upstream,
            keys: HashMap::new(),
            key_channels: HashMap::new(),
        }
    }

    fn schedule_fetch(&mut self, context: &mut Context<Self>) {
        if self.key_channels.is_empty() {
            context.run_later(Duration::from_secs(BATCH_TIMEOUT), Self::fetch_keys);
        }
    }

    fn fetch_keys(&mut self, context: &mut Context<Self>) {
        let channels = mem::replace(&mut self.key_channels, HashMap::new());

        let request = GetPublicKeys {
            relay_ids: channels.keys().cloned().collect(),
        };

        let future = wrap_future::<_, Self>(self.upstream.send(SendRequest(request)))
            .map_err(|_, _, _| KeyError)
            .and_then(|response, actor, _| {
                match response {
                    Ok(mut response) => {
                        for (id, channel) in channels {
                            let key = response.public_keys.remove(&id).unwrap_or(None);
                            actor.keys.insert(id, KeyState::from_option(key.clone()));
                            channel.send(key).ok();
                        }
                    }
                    Err(error) => {
                        error!("error fetching public keys: {}", error);

                        // NOTE: We're dropping `channels` here, which closes the receiver on the
                        // other end with a `oneshot::Canceled` error which gets mapped to a
                        // `KeyError`.
                    }
                }

                wrap_future(future::ok(()))
            });

        context.spawn(future.drop_err());
    }

    fn get_or_fetch_key(&mut self, relay_id: RelayId, context: &mut Context<Self>) -> KeyResponse {
        if let Some(key) = self.keys.get(&relay_id) {
            if key.is_valid_cache() {
                return KeyResponse::Reply(relay_id, key.as_option().cloned());
            }
        }

        self.schedule_fetch(context);

        let receiver = self
            .key_channels
            .entry(relay_id.clone())
            .or_insert_with(|| KeyChannel::new())
            .receiver()
            .map(move |key| (relay_id, (*key).clone()))
            .map_err(|_| KeyError);

        KeyResponse::Async(Box::new(receiver))
    }
}

impl Actor for KeyManager {
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Context<Self>) {
        info!("Key manager started");
    }

    fn stopped(&mut self, _ctx: &mut Context<Self>) {
        println!("Key manager stopped");
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

impl Handler<GetPublicKey> for KeyManager {
    type Result = Response<GetPublicKeyResult, KeyError>;

    fn handle(&mut self, message: GetPublicKey, context: &mut Context<Self>) -> Self::Result {
        match self.get_or_fetch_key(message.relay_id, context) {
            KeyResponse::Reply(_id, public_key) => {
                Response::reply(Ok(GetPublicKeyResult { public_key }))
            }
            KeyResponse::Async(fut) => {
                Response::async(fut.map(|(_id, public_key)| GetPublicKeyResult { public_key }))
            }
        }
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

impl UpstreamRequest for GetPublicKeys {
    type Result = GetPublicKeysResult;

    fn method(&self) -> Method {
        Method::POST
    }

    fn path(&self) -> Cow<str> {
        Cow::Borrowed("/api/0/relays/publickeys/")
    }
}

impl Handler<GetPublicKeys> for KeyManager {
    type Result = Response<GetPublicKeysResult, KeyError>;

    fn handle(&mut self, message: GetPublicKeys, context: &mut Context<Self>) -> Self::Result {
        let mut public_keys = HashMap::new();
        let mut key_futures = Vec::new();

        for id in message.relay_ids {
            match self.get_or_fetch_key(id, context) {
                KeyResponse::Reply(id, key) => {
                    public_keys.insert(id, key);
                }
                KeyResponse::Async(fut) => {
                    key_futures.push(fut);
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
