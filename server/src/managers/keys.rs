//! This actor caches known public keys.
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use actix::Actor;
use actix::Addr;
use actix::Context;
use actix::Handler;
use actix::Message;
use actix::Response;

use actix_web::http::Method;
use actix_web::HttpResponse;
use actix_web::ResponseError;

use futures::Future;

use parking_lot::RwLock;

use semaphore_aorta::ApiErrorResponse;
use semaphore_aorta::PublicKey;
use semaphore_aorta::RelayId;

use managers::upstream_requests::SendRequest;
use managers::upstream_requests::UpstreamRequest;
use managers::upstream_requests::UpstreamRequestManager;

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
    keys: Arc<RwLock<HashMap<RelayId, KeyState>>>,
    upstream: Addr<UpstreamRequestManager>,
}

impl KeyManager {
    pub fn new(upstream: Addr<UpstreamRequestManager>) -> Self {
        KeyManager {
            keys: Arc::new(RwLock::new(HashMap::new())),
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
pub struct GetPublicKeyMessage {
    relay_ids: Vec<RelayId>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GetPublicKeyResult {
    public_keys: HashMap<RelayId, Option<PublicKey>>,
}

impl Message for GetPublicKeyMessage {
    type Result = Result<GetPublicKeyResult, KeyError>;
}

impl UpstreamRequest for GetPublicKeyMessage {
    type Response = GetPublicKeyResult;

    fn get_upstream_request_target(&self) -> (Method, Cow<str>) {
        (Method::POST, Cow::Borrowed("/api/0/relays/publickeys/"))
    }
}

impl Handler<GetPublicKeyMessage> for KeyManager {
    type Result = Response<GetPublicKeyResult, KeyError>;
    fn handle(&mut self, msg: GetPublicKeyMessage, _ctx: &mut Context<Self>) -> Self::Result {
        let mut known_keys = HashMap::new();
        let mut unknown_ids = vec![];

        for id in msg.relay_ids.into_iter() {
            match self.keys.read().get(&id) {
                Some(key) if key.is_valid_cache() => {
                    known_keys.insert(id, key.as_option().cloned());
                }
                _ => {
                    unknown_ids.push(id);
                }
            }
        }

        if unknown_ids.is_empty() {
            Response::reply(Ok(GetPublicKeyResult {
                public_keys: known_keys,
            }))
        } else {
            let self_keys = self.keys.clone();
            Response::async(
                self.upstream
                    .send(SendRequest(GetPublicKeyMessage {
                        relay_ids: unknown_ids,
                    }))
                    .map_err(|_| KeyError)
                    .and_then(move |response| {
                        for (id, key) in response.map_err(|_| KeyError)?.public_keys {
                            known_keys.insert(id, key.clone());
                            self_keys.write().insert(id, KeyState::from_option(key));
                        }

                        Ok(GetPublicKeyResult {
                            public_keys: known_keys,
                        })
                    }),
            )
        }
    }
}
