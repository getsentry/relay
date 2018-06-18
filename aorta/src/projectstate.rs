use std::collections::HashMap;
use std::fmt;
use std::mem;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::de::DeserializeOwned;
use url::Url;
use uuid::Uuid;

use config::AortaConfig;
use event::StoreChangeset;
use query::{AortaQuery, GetProjectConfigQuery, QueryError, RequestManager};
use semaphore_common::ProjectId;
use upstream::UpstreamDescriptor;

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

/// An event that was not sent yet.
#[derive(Debug)]
struct PendingStore {
    added_at: Instant,
    changeset: StoreChangeset,
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
    pending_stores: RwLock<Vec<PendingStore>>,
    requested_new_snapshot: AtomicBool,
    request_manager: Arc<RequestManager>,
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

    /// Returns the project config.
    pub fn config(&self) -> &ProjectConfig {
        &self.config
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
    pub fn new(
        project_id: ProjectId,
        config: Arc<AortaConfig>,
        request_manager: Arc<RequestManager>,
    ) -> ProjectState {
        ProjectState {
            project_id: project_id,
            config: config,
            current_snapshot: RwLock::new(None),
            pending_stores: RwLock::new(Vec::new()),
            requested_new_snapshot: AtomicBool::new(false),
            request_manager: request_manager,
            last_event: RwLock::new(None),
        }
    }

    /// Adds a query that should be issued with the next heartbeat.
    pub fn add_query<Q, R, F, E>(&self, query: Q, callback: F) -> Uuid
    where
        Q: AortaQuery<Response = R>,
        R: DeserializeOwned + 'static,
        F: FnMut(&ProjectState, Result<R, QueryError>) -> Result<(), E> + Sync + Send + 'static,
        E: fmt::Debug,
    {
        self.request_manager
            .add_query(self.project_id(), query, callback)
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

    /// Requests an update to the project config to be fetched.
    pub fn request_updated_project_config(&self) {
        if self.requested_new_snapshot
            .compare_and_swap(false, true, Ordering::Relaxed)
        {
            return;
        }

        debug!("requesting updated project config for {}", self.project_id);
        self.add_query(GetProjectConfigQuery, move |ps, rv| -> Result<(), ()> {
            if let Ok(snapshot_opt) = rv {
                ps.requested_new_snapshot.store(false, Ordering::Relaxed);
                match snapshot_opt {
                    Some(snapshot) => ps.set_snapshot(snapshot),
                    None => ps.set_missing_snapshot(),
                }
            } else {
                // TODO: error handling
                rv.unwrap();
            }
            Ok(())
        });
    }

    /// Checks if events should be buffered for a public key.
    ///
    /// Events should be buffered until a key becomes available nor we
    /// absolutely know that the key does not exist.  There is some
    /// internal logic here that based on the age of the snapshot and
    /// some global disabled settings will indicate different behaviors.
    pub fn get_public_key_event_action(&self, public_key: &str) -> PublicKeyEventAction {
        let guard = self.current_snapshot.read();
        let snapshot = match *guard {
            Some(ref snapshot) => snapshot,
            None => {
                // in the absence of a snapshot we generally queue.
                self.request_updated_project_config();
                return PublicKeyEventAction::Queue;
            }
        };

        if snapshot.outdated(&self.config) {
            // if the snapshot is out of date, we schedule an update in the next
            // heartbeat but proceed like it was still up to date. the upstream
            // semaphore (or sentry) will still filter events.
            self.request_updated_project_config();

            // we assume it is unlikely to re-activate a disabled public key.
            // thus we handle events pretending the config is still valid,
            // except queueing events for unknown DSNs as they might have become
            // available in the meanwhile.
            match snapshot.get_public_key_status(public_key) {
                PublicKeyStatus::Enabled => PublicKeyEventAction::Send,
                PublicKeyStatus::Disabled => PublicKeyEventAction::Discard,
                PublicKeyStatus::Unknown => PublicKeyEventAction::Queue,
            }
        } else {
            // only drop events if we know for sure the project is disabled.
            if snapshot.disabled() {
                return PublicKeyEventAction::Discard;
            }

            // since the config has been fetched recently, we assume unknown
            // public keys do not exist and drop events eagerly.
            match snapshot.get_public_key_status(public_key) {
                PublicKeyStatus::Enabled => PublicKeyEventAction::Send,
                PublicKeyStatus::Disabled | PublicKeyStatus::Unknown => {
                    PublicKeyEventAction::Discard
                }
            }
        }
    }

    /// Validates the origin.
    pub fn is_valid_origin(&self, origin: &Url) -> bool {
        self.snapshot_opt().map_or(true, |snapshot| {
            let allowed = &snapshot.config().allowed_domains;
            !allowed.is_empty()
                && allowed
                    .iter()
                    .any(|x| x.as_str() == "*" || Some(x.as_str()) == origin.host_str())
        })
    }

    /// Given a public key and an event this handles an event.
    ///
    /// It either puts it into an internal queue, sends it or discards it.  If the item
    /// was discarded `false` is returned.
    pub fn store_changeset<'a>(&self, changeset: StoreChangeset) -> bool {
        if let Some(ref origin) = changeset.meta.origin {
            if !self.is_valid_origin(origin) {
                debug!(
                    "{}#{} -> access denied (bad origin {})",
                    self.project_id, changeset.event, origin
                );
                return false;
            }
        }
        match self.get_public_key_event_action(&changeset.public_key) {
            PublicKeyEventAction::Queue => {
                debug!("{}#{} -> pending", self.project_id, changeset.event);
                self.pending_stores.write().push(PendingStore {
                    added_at: Instant::now(),
                    changeset,
                });
                true
            }
            PublicKeyEventAction::Send => {
                debug!("{}#{} -> changeset", self.project_id, changeset.event);
                self.request_manager
                    .add_changeset(self.project_id, changeset);
                true
            }
            PublicKeyEventAction::Discard => {
                debug!("{}#{} -> discarded", self.project_id, changeset.event);
                false
            }
        }
    }

    /// Returns `true` if the project state is available.
    pub fn snapshot_available(&self) -> bool {
        self.current_snapshot.read().is_some()
    }

    /// Returns the current project snapshot.
    pub fn snapshot(&self) -> Arc<ProjectStateSnapshot> {
        self.snapshot_opt().expect("Snapshot not yet available")
    }

    /// Returns the current project snapshot as option.
    pub fn snapshot_opt(&self) -> Option<Arc<ProjectStateSnapshot>> {
        match *self.current_snapshot.read() {
            Some(ref arc) => Some(arc.clone()),
            None => None,
        }
    }

    /// Sets a new snapshot.
    pub fn set_snapshot(&self, new_snapshot: ProjectStateSnapshot) {
        *self.current_snapshot.write() = Some(Arc::new(new_snapshot));
        self.retry_pending_events();
    }

    /// Attempts to send all pending requests now.
    fn retry_pending_events(&self) {
        let snapshot = self.snapshot();
        let mut to_send = vec![];
        let timeout = self.config.pending_events_timeout.to_std().unwrap();

        // acquire the lock locally so we can then acquire the lock later on send
        // without deadlocking
        {
            let mut lock = self.pending_stores.write();
            let pending = mem::replace(&mut *lock, Vec::new());

            lock.extend(pending.into_iter().filter_map(|pending_store| {
                if pending_store.added_at.elapsed() > timeout {
                    return None;
                }

                match snapshot.get_public_key_status(&pending_store.changeset.public_key) {
                    PublicKeyStatus::Enabled => {
                        to_send.push(pending_store);
                        None
                    }
                    PublicKeyStatus::Disabled => None,
                    PublicKeyStatus::Unknown => Some(pending_store),
                }
            }));
        }

        for pending_store in to_send {
            debug!(
                "unpend {}#{}",
                self.project_id, pending_store.changeset.event
            );
            self.store_changeset(pending_store.changeset);
        }
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
