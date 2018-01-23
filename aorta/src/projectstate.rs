use std::sync::Arc;
use std::collections::HashMap;

use parking_lot::RwLock;
use chrono::{DateTime, Duration, Utc};

use upstream::UpstreamDescriptor;
use smith_common::ProjectId;

/// The project state snapshot represents a known server state of
/// a project.
///
/// This is generally used by an indirection of `ProjectState` which
/// manages a view over it which supports concurrent updates in the
/// background.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectStateSnapshot {
    /// The timestamp of when the snapshot was received.
    last_fetch: DateTime<Utc>,
    /// The timestamp of when the last snapshot was changed.
    last_change: DateTime<Utc>,
    /// Indicates that the project is disabled.
    disabled: bool,
    /// A container of known public keys in the project.
    public_keys: HashMap<String, bool>,
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
    upstream: UpstreamDescriptor<'static>,
    project_id: ProjectId,
    current_snapshot: RwLock<Option<Arc<ProjectStateSnapshot>>>,
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
}

impl ProjectState {
    /// Creates a new project state.
    ///
    /// The project state is created without storing a snapshot.  This means
    /// that accessing the snapshot will panic until the data becomes available.
    pub fn new(project_id: ProjectId, upstream: &UpstreamDescriptor) -> ProjectState {
        ProjectState {
            project_id: project_id,
            upstream: upstream.clone().into_owned(),
            current_snapshot: RwLock::new(None),
            last_event: RwLock::new(None),
        }
    }

    /// The project ID of this project.
    pub fn project_id(&self) -> ProjectId {
        self.project_id
    }

    /// The direct upstream that reported the snapshot.
    ///
    /// Currently an agent only ever has one trove and that trove can only have
    /// one upstream descriptor.  As a result of this, this descriptor will always
    /// match the one of the trove which holds the project state.
    pub fn upstream(&self) -> &UpstreamDescriptor {
        &self.upstream
    }

    /// Returns the time of the last event received (but not forwarded).
    ///
    /// This timestamp is used to indicate that the project has activity
    /// for the trove.  As the trove needs to expire items it uses this
    /// timestamp to expire.
    pub fn last_event(&self) -> Option<DateTime<Utc>> {
        *self.last_event.read()
    }

    /// Checks if events should be buffered for a public key.
    ///
    /// Events should be buffered until a key becomes available nor we
    /// absolutely know that the key does not exist.
    pub fn get_public_key_event_action(&self, public_key: &str) -> PublicKeyEventAction {
        match *self.current_snapshot.read() {
            Some(ref snapshot) => {
                match snapshot.get_public_key_status(public_key) {
                    PublicKeyStatus::Enabled => PublicKeyEventAction::Send,
                    PublicKeyStatus::Disabled => PublicKeyEventAction::Discard,
                    PublicKeyStatus::Unknown => {
                        // if the last config fetch was more than a minute ago we just
                        // accept the event because at this point the dsn might have
                        // become available upstream.
                        if snapshot.last_fetch < Utc::now() - Duration::seconds(60) {
                            PublicKeyEventAction::Queue
                        // we just assume the config did not change in the last 60
                        // seconds and the dsn is indeed not seen yet.
                        } else {
                            PublicKeyEventAction::Discard
                        }
                    }
                }
            }
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
