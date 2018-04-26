use std::borrow::Cow;
use std::collections::{HashMap, VecDeque};
use std::fmt;

use hyper::Method;
use parking_lot::RwLock;
use serde::de::DeserializeOwned;
use serde::ser::Serialize;
use serde_json;
use uuid::Uuid;

use smith_common::ProjectId;

use api::ApiRequest;
use projectstate::{ProjectState, ProjectStateSnapshot};

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct PackedRequest {
    #[serde(rename = "type")]
    pub ty: String,
    pub project_id: ProjectId,
    pub data: serde_json::Value,
}

/// Indicates the internal status of an aorta query.
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
#[serde(rename_all = "lowercase")]
pub enum QueryStatus {
    /// The query succeeded
    Success,
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

/// The request manager helps sending aorta queries.
pub struct RequestManager {
    pending_changesets: RwLock<VecDeque<PackedRequest>>,
    pending_queries: RwLock<VecDeque<(Uuid, PackedRequest)>>,
    // XXX: this should actually be FnOnce but current versions of rust do not
    // permit boxing this. See https://github.com/rust-lang/rfcs/issues/997
    query_callbacks: RwLock<
        HashMap<
            Uuid,
            (
                ProjectId,
                Box<FnMut(&ProjectState, serde_json::Value, bool) -> () + Sync + Send>,
            ),
        >,
    >,
}

impl RequestManager {
    /// Creates a new request manager
    pub fn new() -> RequestManager {
        // TODO: queries can expire.  This means something needs to clean up very
        // old query callbacks eventually or we leak memory here.
        RequestManager {
            pending_changesets: RwLock::new(VecDeque::new()),
            pending_queries: RwLock::new(VecDeque::new()),
            query_callbacks: RwLock::new(HashMap::new()),
        }
    }

    /// Adds a changeset to the request manager.
    pub fn add_changeset<C: AortaChangeset>(&self, project_id: ProjectId, changeset: C) {
        self.pending_changesets.write().push_back(PackedRequest {
            ty: changeset.aorta_changeset_type().to_string(),
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
        self.query_callbacks.write().insert(
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
        self.pending_queries.write().push_back((
            query_id,
            PackedRequest {
                ty: query.aorta_query_type().to_string(),
                project_id: project_id,
                data: serde_json::to_value(&query).unwrap(),
            },
        ));
        query_id
    }

    fn unschedule_pending_changeset(&self) -> Option<PackedRequest> {
        self.pending_changesets.write().pop_front()
    }

    fn unschedule_pending_query(&self) -> Option<(Uuid, PackedRequest)> {
        self.pending_queries.write().pop_front()
    }

    /// Given a query id removes and returns the callback.
    pub fn pop_callback(
        &self,
        query_id: Uuid,
    ) -> Option<(
        ProjectId,
        Box<FnMut(&ProjectState, serde_json::Value, bool) -> () + Sync + Send>,
    )> {
        self.query_callbacks.write().remove(&query_id)
    }

    /// Returns a single heartbeat request.
    ///
    /// This unschedules some pending queries from the request manager.
    pub fn next_heartbeat_request(&self) -> HeartbeatRequest {
        let mut rv = HeartbeatRequest {
            changesets: Vec::new(),
            queries: HashMap::new(),
        };

        for _ in 0..50 {
            match self.unschedule_pending_changeset() {
                Some(changeset) => rv.changesets.push(changeset),
                None => break,
            };
        }

        for _ in 0..50 {
            match self.unschedule_pending_query() {
                Some((query_id, query)) => rv.queries.insert(query_id, query),
                None => break,
            };
        }
        rv
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
    fn aorta_changeset_type(&self) -> &str;
}

/// A trait for all objects that can trigger an aorta query.
pub trait AortaQuery: Serialize {
    /// The type of the query response
    type Response: DeserializeOwned + 'static;

    /// Returns the type (name) of the query in the aorta protocol.
    fn aorta_query_type(&self) -> &str;
}

/// A query to fetch the current project state.
#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct GetProjectConfigQuery;

impl AortaQuery for GetProjectConfigQuery {
    type Response = ProjectStateSnapshot;
    fn aorta_query_type(&self) -> &str {
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
