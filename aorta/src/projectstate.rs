use std::sync::Arc;
use std::collections::HashMap;

use parking_lot::RwLock;
use chrono::{DateTime, Utc};
use uuid::Uuid;

use config::AortaConfig;
use upstream::UpstreamDescriptor;
use query::Query;
use smith_common::ProjectId;

/// These are config values that the user can modify in the UI.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ProjectConfig {
    /// URLs that are permitted for cross original JavaScript requests.
    pub allowed_domains: Vec<String>,
}

/// The project state snapshot represents a known server state of
/// a project.
///
/// This is generally used by an indirection of `ProjectState` which
/// manages a view over it which supports concurrent updates in the
/// background.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProjectStateSnapshot {
    /// The timestamp of when the snapshot was received.
    pub last_fetch: DateTime<Utc>,
    /// The timestamp of when the last snapshot was changed.
    ///
    /// This might be `None` in some rare cases like where snapshots
    /// are faked locally.
    pub last_change: Option<DateTime<Utc>>,
    /// Indicates that the project is disabled.
    pub disabled: bool,
    /// A container of known public keys in the project.
    pub public_keys: HashMap<String, bool>,
    /// The project's slug if available.
    pub slug: Option<String>,
    /// The project's current config
    pub config: ProjectConfig,
    /// The project state's revision id.
    pub rev: Option<Uuid>,
}

/// A helper enum indicating the public key state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PublicKeyStatus {
    /// The state of the public key is not known.
    ///
    /// This can indicate that the key is not yet known or that the
    /// key just does not exist.  We can not tell these two cases
    /// apart as there is always a lag since the last update from the
    /// upstream server.  As such the project state uses a heuristic
    /// to decide if it should treat a key as not existing or just
    /// not yet known.
    Unknown,
    /// This key is known but was disabled.
    Disabled,
    /// This key is known and is enabled.
    Enabled,
}

/// Indicates what should happen to events based on the public key
/// of a project.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PublicKeyEventAction {
    /// Indicates that this event should be queued.
    Queue,
    /// Indicates that this event should be discarded.
    Discard,
    /// Indicates that this event should be sent now.
    Send,
}

/// Gives access to the project's remote state.
///
/// This wrapper is sync and can be updated concurrently.  As the type is
/// sync all of the methods can be used on a shared instance.  The type
/// internally locks automatically.
#[derive(Debug)]
pub struct ProjectState {
    config: Arc<AortaConfig>,
    project_id: ProjectId,
    current_snapshot: RwLock<Option<Arc<ProjectStateSnapshot>>>,
    pending_queries: RwLock<HashMap<Uuid, Query>>,
    last_event: RwLock<Option<DateTime<Utc>>>,
}

impl ProjectStateSnapshot {
    /// Returns the current status of a key.
    pub fn get_public_key_status(&self, public_key: &str) -> PublicKeyStatus {
        match self.public_keys.get(public_key) {
            Some(&true) => PublicKeyStatus::Enabled,
            Some(&false) => PublicKeyStatus::Disabled,
            None => PublicKeyStatus::Unknown,
        }
    }

    /// Checks if a public key is enabled.
    pub fn public_key_is_enabled(&self, public_key: &str) -> bool {
        self.get_public_key_status(public_key) == PublicKeyStatus::Enabled
    }

    /// Returns `true` if the entire project should be considered
    /// disabled (blackholed, deleted etc.).
    pub fn disabled(&self) -> bool {
        self.disabled
    }

    /// Returns true if the snapshot is outdated.
    pub fn outdated(&self, config: &AortaConfig) -> bool {
        // TODO(armin): change this to a value from the config
        self.last_fetch < Utc::now() - config.snapshot_expiry
    }
}

impl ProjectState {
    /// Creates a new project state.
    ///
    /// The project state is created without storing a snapshot.  This means
    /// that accessing the snapshot will panic until the data becomes available.
    ///
    /// The config is taken as `Arc` so we can share it effectively across
    /// multiple project states and troves.
    pub fn new(project_id: ProjectId, config: Arc<AortaConfig>) -> ProjectState {
        ProjectState {
            project_id: project_id,
            config: config,
            current_snapshot: RwLock::new(None),
            pending_queries: RwLock::new(HashMap::new()),
            last_event: RwLock::new(None),
        }
    }

    /// Adds a query that should be issued with the next heartbeat.
    pub fn add_query(&self, query: Query) -> Uuid {
        let query_id = Uuid::new_v4();
        self.pending_queries.write().insert(query_id, query);
        query_id
    }

    /// Adds a query if it's not in there already (debounces).  If the query
    /// is already there, the old uuid is returned.
    pub fn add_query_uniq(&self, query: Query) -> Uuid {
        for (query_id, existing_query) in self.pending_queries.read().iter() {
            if existing_query == &query {
                return query_id.clone();
            }
        }
        self.add_query(query)
    }

    /// The project ID of this project.
    pub fn project_id(&self) -> ProjectId {
        self.project_id
    }

    /// The direct upstream that reported the snapshot.
    ///
    /// Currently an relay only ever has one trove and that trove can only have
    /// one upstream descriptor.  As a result of this, this descriptor will always
    /// match the one of the trove which holds the project state.
    pub fn upstream(&self) -> &UpstreamDescriptor {
        &self.config.upstream
    }

    /// Returns the time of the last event received (but not forwarded).
    ///
    /// This timestamp is used to indicate that the project has activity
    /// for the trove.  As the trove needs to expire items it uses this
    /// timestamp to expire.
    pub fn last_event(&self) -> Option<DateTime<Utc>> {
        *self.last_event.read()
    }

    /// Returns the time of the last config fetch.
    pub fn last_config_fetch(&self) -> Option<DateTime<Utc>> {
        self.current_snapshot
            .read()
            .as_ref()
            .map(|x| x.last_fetch.clone())
    }

    /// Returns the time of the last config change.
    pub fn last_config_change(&self) -> Option<DateTime<Utc>> {
        self.current_snapshot
            .read()
            .as_ref()
            .and_then(|x| x.last_change.clone())
    }

    /// Checks if events should be buffered for a public key.
    ///
    /// Events should be buffered until a key becomes available nor we
    /// absolutely know that the key does not exist.  There is some
    /// internal logic here that based on the age of the snapshot and
    /// some global disabled settings will indicate different behaviors.
    pub fn get_public_key_event_action(&self, public_key: &str) -> PublicKeyEventAction {
        match *self.current_snapshot.read() {
            Some(ref snapshot) => {
                // in case the entire project is disabled we always discard
                // events unless the snapshot is outdated.  In case the
                // snapshot is outdated we might fall back to sending or
                // discarding as well based on the key status.
                if !snapshot.outdated(&self.config) && snapshot.disabled() {
                    return PublicKeyEventAction::Discard;
                }
                match snapshot.get_public_key_status(public_key) {
                    PublicKeyStatus::Enabled => PublicKeyEventAction::Send,
                    PublicKeyStatus::Disabled => PublicKeyEventAction::Discard,
                    PublicKeyStatus::Unknown => {
                        // we don't know the key yet, ensure we fetch it on the next
                        // heartbeat.
                        self.add_query_uniq(Query::GetProjectConfig {
                            project_id: self.project_id(),
                        });

                        // if the last config fetch was more than a minute ago we just
                        // accept the event because at this point the dsn might have
                        // become available upstream.
                        if snapshot.outdated(&self.config) {
                            PublicKeyEventAction::Queue
                        // we just assume the config did not change in the last 60
                        // seconds and the dsn is indeed not seen yet.
                        } else {
                            PublicKeyEventAction::Discard
                        }
                    }
                }
            }
            // in the absence of a snapshot we generally queue
            None => PublicKeyEventAction::Queue,
        }
    }

    /// Returns `true` if the project state is available.
    pub fn snapshot_available(&self) -> bool {
        self.current_snapshot.read().is_some()
    }

    /// Returns the current project state.
    pub fn snapshot(&self) -> Arc<ProjectStateSnapshot> {
        let lock = self.current_snapshot.read();
        match *lock {
            Some(ref arc) => arc.clone(),
            None => panic!("Snapshot not yet available"),
        }
    }

    /// Sets a new snapshot.
    pub fn set_snapshot(&self, new_snapshot: ProjectStateSnapshot) {
        *self.current_snapshot.write() = Some(Arc::new(new_snapshot));
    }

    /// Sets a "project does not exist" snapshot.
    ///
    /// This is used when the server indicates that this project does not actually
    /// exist or the relay has no permissions to work with it (these are both
    /// reported as the same thing to the relay).
    pub fn set_missing_snapshot(&self) {
        self.set_snapshot(ProjectStateSnapshot {
            last_fetch: Utc::now(),
            last_change: None,
            disabled: true,
            public_keys: HashMap::new(),
            slug: None,
            config: Default::default(),
            rev: None,
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_assert_sync() {
        struct Assert<T: Sync> {
            x: Option<T>,
        }
        let val: Assert<ProjectState> = Assert { x: None };
        assert_eq!(val.x.is_none(), true);
    }
}
