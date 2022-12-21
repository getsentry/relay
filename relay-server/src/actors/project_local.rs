use std::collections::HashMap;
use std::ffi::OsStr;
use std::path::Path;
use std::sync::Arc;

use relay_common::{ProjectId, ProjectKey};
use relay_config::Config;
use relay_log::LogError;
use relay_system::AsyncResponse;
use relay_system::FromMessage;
use relay_system::Interface;
use relay_system::Sender;
use relay_system::Service;
use tokio::sync::mpsc;

use crate::actors::project::ProjectState;
use crate::actors::project_cache::FetchOptionalProjectState;

/// Service interface of the local project source.
/// Fetches the project state for a given project key. Returns `None` if the project state is not available locally.

#[derive(Debug)]
pub struct LocalProjectSource(FetchOptionalProjectState, Sender<Option<Arc<ProjectState>>>);

impl Interface for LocalProjectSource {}

impl FromMessage<FetchOptionalProjectState> for LocalProjectSource {
    type Response = AsyncResponse<Option<Arc<ProjectState>>>;
    fn from_message(
        message: FetchOptionalProjectState,
        sender: Sender<Option<Arc<ProjectState>>>,
    ) -> Self {
        Self(message, sender)
    }
}

/// A service which periodically loads project states from disk.
#[derive(Debug)]
pub struct LocalProjectSourceService {
    config: Arc<Config>,
    local_states: HashMap<ProjectKey, Arc<ProjectState>>,
}

impl LocalProjectSourceService {
    pub fn new(config: Arc<Config>) -> Self {
        Self {
            config,
            local_states: HashMap::new(),
        }
    }

    fn handle_fetch_optional_project_state(
        &self,
        message: FetchOptionalProjectState,
        sender: Sender<Option<Arc<ProjectState>>>,
    ) {
        let states = self.local_states.get(&message.project_key()).cloned();
        sender.send(states);
    }

    fn handle_message(&mut self, message: LocalProjectSource) {
        let LocalProjectSource(message, sender) = message;
        self.handle_fetch_optional_project_state(message, sender)
    }
}

impl Service for LocalProjectSourceService {
    type Interface = LocalProjectSource;

    /// Spawns two loops:
    /// 1. one for periodically polling local states, and
    /// 2. one to handle both external messages _and_ updates to the local cache.
    fn spawn_handler(mut self, mut rx: relay_system::Receiver<Self::Interface>) {
        let project_path = self.config.project_configs_path();
        let (mut state_tx, mut state_rx) = mpsc::channel(1);

        let mut ticker = tokio::time::interval(self.config.local_cache_interval());
        tokio::spawn(async move {
            loop {
                ticker.tick().await;
                poll_local_states(&project_path, &mut state_tx).await;
            }
        });

        tokio::spawn(async move {
            // Poll local states once before handling any message, such that the projects are
            // populated.
            // FIXME restore this
            // self.poll_local_states().await;

            relay_log::info!("project local cache started");
            loop {
                tokio::select! {
                    Some(message) = rx.recv() => self.handle_message(message),
                    Some(states) = state_rx.recv() => self.local_states = states,

                    else => break,
                }
            }
            relay_log::info!("project local cache stopped");
        });
    }
}

fn get_project_id(path: &Path) -> Option<ProjectId> {
    path.file_stem()
        .and_then(OsStr::to_str)
        .and_then(|stem| stem.parse().ok())
}

async fn load_local_states(
    projects_path: &Path,
) -> tokio::io::Result<HashMap<ProjectKey, Arc<ProjectState>>> {
    let mut states = HashMap::new();

    let mut directory = match tokio::fs::read_dir(projects_path).await {
        Ok(directory) => directory,
        Err(error) => {
            return match error.kind() {
                tokio::io::ErrorKind::NotFound => Ok(states),
                _ => Err(error),
            };
        }
    };

    // only printed when directory even exists.
    relay_log::debug!("Loading local states from directory {:?}", projects_path);

    while let Some(entry) = directory.next_entry().await? {
        let path = entry.path();

        if !entry.metadata().await?.is_file() {
            relay_log::warn!("skipping {:?}, not a file", path);
            continue;
        }

        if path.extension().map(|x| x != "json").unwrap_or(true) {
            relay_log::warn!("skipping {:?}, file extension must be .json", path);
            continue;
        }

        fn parse_file(
            path: std::path::PathBuf,
        ) -> tokio::io::Result<(std::path::PathBuf, ProjectState)> {
            let file = std::fs::File::open(&path)?;
            let reader = std::io::BufReader::new(file);
            Ok((path, serde_json::from_reader(reader)?))
        }

        // serde_json is not async, so spawn a blocking task here:
        let handle = tokio::task::spawn_blocking(move || parse_file(path));
        let (path, state) = handle.await??;

        let mut sanitized = ProjectState::sanitize(state);
        if sanitized.project_id.is_none() {
            if let Some(project_id) = get_project_id(&path) {
                sanitized.project_id = Some(project_id);
            } else {
                relay_log::warn!("skipping {:?}, filename is not a valid project id", path);
                continue;
            }
        }

        let arc = Arc::new(sanitized);
        for key in &arc.public_keys {
            states.insert(key.public_key, arc.clone());
        }
    }

    Ok(states)
}

async fn poll_local_states(
    path: &Path,
    tx: &mut mpsc::Sender<HashMap<ProjectKey, Arc<ProjectState>>>,
) {
    let states = load_local_states(path).await;
    match states {
        Ok(states) => {
            let _ = tx.send(states).await;
        }
        Err(error) => relay_log::error!(
            "failed to load static project configs: {}",
            LogError(&error)
        ),
    };
}
