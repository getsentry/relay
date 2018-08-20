//! This actor caches known public keys.
use std::borrow::Cow;
use std::collections::HashMap;
use std::time::Instant;

use actix::fut::wrap_future;
use actix::Actor;
use actix::ActorFuture;
use actix::ActorResponse;
use actix::Addr;
use actix::Context;
use actix::Handler;
use actix::Message;

use actix_web::http::Method;
use actix_web::HttpResponse;
use actix_web::ResponseError;

use futures::future;

use semaphore_aorta::ApiErrorResponse;
use semaphore_aorta::PublicKey;
use semaphore_aorta::RelayId;

use actors::upstream::SendRequest;
use actors::upstream::UpstreamRelay;
use actors::upstream::UpstreamRequest;

#[derive(Fail, Debug)]
#[fail(display = "failed to fetch keys")]
pub struct KeyError;

impl ResponseError for KeyError {
    fn error_response(&self) -> HttpResponse {
        HttpResponse::BadGateway().json(&ApiErrorResponse::from_fail(self))
    }
}

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
            } => checked_at.elapsed().as_secs() < 3600,
            KeyState::DoesNotExist { checked_at } => checked_at.elapsed().as_secs() < 60,
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

pub struct KeyManager {
    // XXX: only necessary because we mutate inside of a closure passed to future.and_then
    keys: HashMap<RelayId, KeyState>,
    upstream: Addr<UpstreamRelay>,
}

impl KeyManager {
    pub fn new(upstream: Addr<UpstreamRelay>) -> Self {
        KeyManager {
            keys: HashMap::new(),
            upstream,
        }
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

#[derive(Debug, Deserialize, Serialize)]
pub struct GetPublicKey {
    relay_ids: Vec<RelayId>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GetPublicKeyResult {
    public_keys: HashMap<RelayId, Option<PublicKey>>,
}

impl Message for GetPublicKey {
    type Result = Result<GetPublicKeyResult, KeyError>;
}

impl UpstreamRequest for GetPublicKey {
    type Response = GetPublicKeyResult;

    fn get_upstream_request_target(&self) -> (Method, Cow<str>) {
        (Method::POST, Cow::Borrowed("/api/0/relays/publickeys/"))
    }
}

impl Handler<GetPublicKey> for KeyManager {
    type Result = ActorResponse<Self, GetPublicKeyResult, KeyError>;

    fn handle(&mut self, msg: GetPublicKey, _ctx: &mut Context<Self>) -> Self::Result {
        let mut known_keys = HashMap::new();
        let mut unknown_ids = vec![];

        for id in msg.relay_ids.into_iter() {
            match self.keys.get(&id) {
                Some(key) if key.is_valid_cache() => {
                    known_keys.insert(id, key.as_option().cloned());
                }
                _ => {
                    unknown_ids.push(id);
                }
            }
        }

        if unknown_ids.is_empty() {
            ActorResponse::reply(Ok(GetPublicKeyResult {
                public_keys: known_keys,
            }))
        } else {
            let request = SendRequest(GetPublicKey {
                relay_ids: unknown_ids,
            });

            ActorResponse::async(
                wrap_future::<_, Self>(self.upstream.send(request))
                    .map_err(|_, _, _| KeyError)
                    .and_then(|response, actor, ctx| {
                        let response = match response.map_err(|_| KeyError) {
                            Ok(response) => response,
                            Err(e) => return wrap_future(future::err(e)),
                        };

                        for (id, key) in response.public_keys {
                            known_keys.insert(id, key.clone());
                            actor.keys.insert(id, KeyState::from_option(key));
                        }

                        wrap_future(future::ok(GetPublicKeyResult {
                            public_keys: known_keys,
                        }))
                    }),
            )
        }
    }
}
