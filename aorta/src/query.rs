use std::fmt;
use std::borrow::Cow;
use std::collections::{HashMap, VecDeque};

use serde::ser::Serialize;
use serde::de::DeserializeOwned;
use serde_json;
use uuid::Uuid;
use parking_lot::RwLock;
use hyper::Method;

use smith_common::ProjectId;

use api::AortaApiRequest;
use projectstate::{ProjectState, ProjectStateSnapshot};

#[derive(Fail, Debug)]
#[fail(display = "a query failed")]
pub struct AortaQueryError;

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct PackedQuery {
    #[serde(rename = "type")] pub ty: String,
    pub data: serde_json::Value,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
#[serde(rename_all = "lowercase")]
pub enum QueryStatus {
    Success,
    Pending,
    Error,
}

#[derive(Fail, Serialize, Deserialize, Debug)]
pub struct QueryError {
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

/// The query manager helps sending aorta queries.
pub struct QueryManager {
    pending_queries: RwLock<VecDeque<(Uuid, PackedQuery)>>,
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

impl QueryManager {
    /// Creates a new query manager
    pub fn new() -> QueryManager {
        QueryManager {
            pending_queries: RwLock::new(VecDeque::new()),
            query_callbacks: RwLock::new(HashMap::new()),
        }
    }

    /// Adds a query to the query manager.
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
            PackedQuery {
                ty: query.aorta_query_type().to_string(),
                data: serde_json::to_value(&query).unwrap(),
            },
        ));
        query_id
    }

    fn unschedule_pending_query(&self) -> Option<(Uuid, PackedQuery)> {
        self.pending_queries.write().pop_front()
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
        self.query_callbacks.write().remove(&query_id)
    }

    /// Returns a single heartbeat request.
    ///
    /// This unschedules some pending queries from the query manager.
    pub fn next_heartbeat_request(&self) -> HeartbeatRequest {
        let mut rv = HeartbeatRequest {
            queries: HashMap::new(),
        };
        for _ in 0..50 {
            if let Some((query_id, query)) = self.unschedule_pending_query() {
                rv.queries.insert(query_id, query);
            } else {
                break;
            }
        }
        rv
    }
}

impl fmt::Debug for QueryManager {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "QueryManager {{ ... }}")
    }
}

pub trait AortaQuery: Serialize {
    type Response: DeserializeOwned + 'static;
    fn aorta_query_type(&self) -> &str;
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct GetProjectConfigQuery {
    pub project_id: ProjectId,
}

impl GetProjectConfigQuery {
    pub fn new(project_id: ProjectId) -> GetProjectConfigQuery {
        GetProjectConfigQuery {
            project_id: project_id,
        }
    }
}

impl AortaQuery for GetProjectConfigQuery {
    type Response = ProjectStateSnapshot;
    fn aorta_query_type(&self) -> &str {
        "get_project_config"
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct HeartbeatRequest {
    queries: HashMap<Uuid, PackedQuery>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct HeartbeatQueryResult {
    pub status: QueryStatus,
    pub result: Option<serde_json::Value>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct HeartbeatResponse {
    pub query_results: HashMap<Uuid, HeartbeatQueryResult>,
}

impl AortaApiRequest for HeartbeatRequest {
    type Response = HeartbeatResponse;

    fn get_aorta_request_target<'a>(&'a self) -> (Method, Cow<'a, str>) {
        (Method::Post, Cow::Borrowed("relays/heartbeat/"))
    }
}
