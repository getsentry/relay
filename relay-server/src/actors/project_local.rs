use std::collections::HashMap;
use std::ffi::OsStr;
use std::path::Path;
use std::sync::Arc;

use relay_common::{ProjectId, ProjectKey};
use relay_config::Config;
use relay_log::LogError;
use relay_system::{AsyncResponse, FromMessage, Interface, Receiver, Sender, Service};
use tokio::sync::mpsc;
use tokio::time::Instant;

use crate::actors::project::ProjectState;
use crate::actors::project_cache::FetchOptionalProjectState;

/// Service interface of the local project source.
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

    fn handle_message(&mut self, message: LocalProjectSource) {
        let LocalProjectSource(message, sender) = message;
        let states = self.local_states.get(&message.project_key()).cloned();
        sender.send(states);
    }
}

fn get_project_id(path: &Path) -> Option<ProjectId> {
    path.file_stem()
        .and_then(OsStr::to_str)
        .and_then(|stem| stem.parse().ok())
}

fn parse_file(path: std::path::PathBuf) -> tokio::io::Result<(std::path::PathBuf, ProjectState)> {
    let file = std::fs::File::open(&path)?;
    let reader = std::io::BufReader::new(file);
    Ok((path, serde_json::from_reader(reader)?))
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

        // serde_json is not async, so spawn a blocking task here:
        let (path, state) = tokio::task::spawn_blocking(move || parse_file(path)).await??;

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

async fn poll_local_states(path: &Path, tx: &mpsc::Sender<HashMap<ProjectKey, Arc<ProjectState>>>) {
    let states = load_local_states(path).await;
    match states {
        Ok(states) => {
            let res = tx.send(states).await;
            if res.is_err() {
                relay_log::error!("failed to store static project configs");
            }
        }
        Err(error) => relay_log::error!(
            "failed to load static project configs: {}",
            LogError(&error)
        ),
    };
}

async fn spawn_poll_local_states(
    config: &Config,
    tx: mpsc::Sender<HashMap<ProjectKey, Arc<ProjectState>>>,
) {
    let project_path = config.project_configs_path();
    let period = config.local_cache_interval();

    // Poll local states once before handling any message, such that the projects are
    // populated.
    poll_local_states(&project_path, &tx).await;

    // Start a background loop that polls periodically:
    tokio::spawn(async move {
        // To avoid running two load tasks simultaneously at startup, we delay the interval by one period:
        let start_at = Instant::now() + period;
        let mut ticker = tokio::time::interval_at(start_at, period);

        loop {
            ticker.tick().await;
            poll_local_states(&project_path, &tx).await;
        }
    });
}

impl Service for LocalProjectSourceService {
    type Interface = LocalProjectSource;

    fn spawn_handler(mut self, mut rx: Receiver<Self::Interface>) {
        // Use a channel with size 1. If the channel is full because the consumer does not
        // collect the result, the producer will block, which is acceptable.
        let (state_tx, mut state_rx) = mpsc::channel(1);

        tokio::spawn(async move {
            relay_log::info!("project local cache started");

            // Start the background task that periodically reloads projects from disk:
            spawn_poll_local_states(&self.config, state_tx).await;

            loop {
                tokio::select! {
                    biased;
                    Some(message) = rx.recv() => self.handle_message(message),
                    Some(states) = state_rx.recv() => self.local_states = states,

                    else => break,
                }
            }
            relay_log::info!("project local cache stopped");
        });
    }
}
