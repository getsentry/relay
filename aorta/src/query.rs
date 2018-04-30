use std::time::{Duration, Instant};
use std::borrow::Cow;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::fmt;

use hyper::Method;
use parking_lot::RwLock;
use serde::de::DeserializeOwned;
use serde::ser::Serialize;
use serde_json;
use uuid::Uuid;

use smith_common::ProjectId;

use api::ApiRequest;
use config::AortaConfig;
use projectstate::{ProjectState, ProjectStateSnapshot};

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct PackedRequest {
    #[serde(rename = "type")]
    pub ty: Cow<'static, str>,
    pub project_id: ProjectId,
    pub data: serde_json::Value,
}

/// Indicates the internal status of an aorta query.
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
#[serde(rename_all = "lowercase")]
pub enum QueryStatus {
    /// The query succeeded
    Ok,
    /// The query is still pending
    Pending,
    /// The query failed with an error
    Error,
}

/// Indicates how a query failed.
#[derive(Fail, Serialize, Deserialize, Debug)]
pub struct QueryError {
    /// Optionally a detailed error message about why the query failed.
    pub detail: Option<String>,
}

impl fmt::Display for QueryError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Some(ref detail) = self.detail {
            write!(f, "query error: {}", detail)
        } else {
            write!(f, "query error")
        }
    }
}

struct RequestManagerInner {
    pending_changesets: VecDeque<PackedRequest>,
    pending_queries: VecDeque<(Uuid, PackedRequest)>,
    // XXX: this should actually be FnOnce but current versions of rust do not
    // permit boxing this. See https://github.com/rust-lang/rfcs/issues/997
    query_callbacks: HashMap<
        Uuid,
        (
            ProjectId,
            Box<FnMut(&ProjectState, serde_json::Value, bool) -> () + Sync + Send>,
        ),
    >,
    last_heartbeat: Option<Instant>,
}

/// The request manager helps sending aorta queries.
pub struct RequestManager {
    config: Arc<AortaConfig>,
    inner: RwLock<RequestManagerInner>,
}

impl RequestManager {
    /// Creates a new request manager
    pub fn new(config: Arc<AortaConfig>) -> RequestManager {
        // TODO: queries can expire.  This means something needs to clean up very
        // old query callbacks eventually or we leak memory here.
        RequestManager {
            config: config,
            inner: RwLock::new(RequestManagerInner {
                pending_changesets: VecDeque::new(),
                pending_queries: VecDeque::new(),
                query_callbacks: HashMap::new(),
                last_heartbeat: None,
            }),
        }
    }

    /// Adds a changeset to the request manager.
    pub fn add_changeset<C: AortaChangeset>(&self, project_id: ProjectId, changeset: C) {
        self.inner
            .write()
            .pending_changesets
            .push_back(PackedRequest {
                ty: Cow::Borrowed(changeset.aorta_changeset_type()),
                project_id: project_id,
                data: serde_json::to_value(&changeset).unwrap(),
            })
    }

    /// Adds a query to the request manager.
    ///
    /// The callback is executed once the query returns with a result.
    pub fn add_query<Q, R, F, E>(&self, project_id: ProjectId, query: Q, callback: F) -> Uuid
    where
        Q: AortaQuery<Response = R>,
        R: DeserializeOwned + 'static,
        F: FnMut(&ProjectState, Result<R, QueryError>) -> Result<(), E> + Sync + Send + 'static,
        E: fmt::Debug,
    {
        let query_id = Uuid::new_v4();
        let callback = RwLock::new(callback);
        let mut inner = self.inner.write();
        inner.query_callbacks.insert(
            query_id,
            (
                project_id,
                Box::new(move |ps, value, success| {
                    let callback = &mut *callback.write();
                    if success {
                        let data: R = serde_json::from_value(value).unwrap();
                        callback(ps, Ok(data)).unwrap();
                    } else {
                        let err: QueryError = serde_json::from_value(value).unwrap();
                        callback(ps, Err(err)).unwrap();
                    }
                }),
            ),
        );
        inner.pending_queries.push_back((
            query_id,
            PackedRequest {
                ty: Cow::Borrowed(query.aorta_query_type()),
                project_id: project_id,
                data: serde_json::to_value(&query).unwrap(),
            },
        ));
        query_id
    }

    /// Given a query id removes and returns the callback.
    pub fn pop_callback(
        &self,
        query_id: Uuid,
    ) -> Option<
        (
            ProjectId,
            Box<FnMut(&ProjectState, serde_json::Value, bool) -> () + Sync + Send>,
        ),
    > {
        self.inner.write().query_callbacks.remove(&query_id)
    }

    fn heartbeat_interval(&self) -> Duration {
        self.config.heartbeat_interval.to_std().unwrap()
    }

    fn buffer_interval(&self) -> Duration {
        self.config.changeset_buffer_interval.to_std().unwrap()
    }

    fn fast_retry_interval(&self) -> Duration {
        Duration::from_millis(100)
    }

    fn normal_retry_interval(&self) -> Duration {
        Duration::from_secs(1)
    }

    /// Returns a single heartbeat request.
    ///
    /// This unschedules some pending queries from the request manager.  It also
    /// returns when the next heartbeat should be.
    pub fn next_heartbeat_request(&self) -> (Option<HeartbeatRequest>, Duration) {
        let mut rv = HeartbeatRequest {
            changesets: Vec::new(),
            queries: HashMap::new(),
        };

        let mut inner = self.inner.write();
        for _ in 0..50 {
            match inner.pending_changesets.pop_front() {
                Some(changeset) => rv.changesets.push(changeset),
                None => break,
            };
        }

        for _ in 0..50 {
            match inner.pending_queries.pop_front() {
                Some((query_id, query)) => rv.queries.insert(query_id, query),
                None => break,
            };
        }

        let last_heartbeat = inner.last_heartbeat;
        inner.last_heartbeat = Some(Instant::now());

        // if there is actual data in the heartbeat request, send it and come back in two seconds.
        if !rv.changesets.is_empty() || !rv.queries.is_empty() {
            (
                Some(rv),
                match inner.pending_queries.is_empty() || inner.pending_changesets.is_empty() {
                    true => self.buffer_interval(),
                    false => self.fast_retry_interval(),
                },
            )

        // we waited long enough without sending some data, send an empty heartbeat now and come
        // back quickly for more checks
        } else if last_heartbeat.map_or(true, |x| x.elapsed() > self.heartbeat_interval()) {
            (Some(rv), self.normal_retry_interval())

        // no request to send now, check back quickly
        } else {
            (None, self.normal_retry_interval())
        }
    }
}

impl fmt::Debug for RequestManager {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "RequestManager {{ ... }}")
    }
}

/// A trait for all objects that can trigger changes via aorta.
pub trait AortaChangeset: Serialize {
    /// Returns the type (name) of the query in the aorta protocol.
    fn aorta_changeset_type(&self) -> &'static str;
}

/// A trait for all objects that can trigger an aorta query.
pub trait AortaQuery: Serialize {
    /// The type of the query response
    type Response: DeserializeOwned + 'static;

    /// Returns the type (name) of the query in the aorta protocol.
    fn aorta_query_type(&self) -> &'static str;
}

/// A query to fetch the current project state.
#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct GetProjectConfigQuery;

impl AortaQuery for GetProjectConfigQuery {
    type Response = Option<ProjectStateSnapshot>;
    fn aorta_query_type(&self) -> &'static str {
        "get_project_config"
    }
}

/// An API request for the heartbeat request.
#[derive(Serialize, Deserialize, Debug)]
pub struct HeartbeatRequest {
    changesets: Vec<PackedRequest>,
    queries: HashMap<Uuid, PackedRequest>,
}

/// The response from a heartbeat query.
#[derive(Serialize, Deserialize, Debug)]
pub struct HeartbeatQueryResult {
    /// The status of the query
    pub status: QueryStatus,
    /// The raw response data as JSON.  Might be None if the
    /// query is pending.
    pub result: Option<serde_json::Value>,
}

/// The response format for a heartbeat request.
#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct HeartbeatResponse {
    /// A hashmap of query results.
    pub query_results: HashMap<Uuid, HeartbeatQueryResult>,
}

impl ApiRequest for HeartbeatRequest {
    type Response = HeartbeatResponse;

    fn get_aorta_request_target<'a>(&'a self) -> (Method, Cow<'a, str>) {
        (Method::Post, Cow::Borrowed("relays/heartbeat/"))
    }
}
