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

use crate::actors::project::ProjectState;
use crate::actors::project_cache::FetchOptionalProjectState;

/// Service interface of the local project source.
#[derive(Debug)]
pub enum LocalProjectSource {
    /// TODO: docs
    FetchOptionalProjectState(FetchOptionalProjectState, Sender<Option<Arc<ProjectState>>>),
}

impl Interface for LocalProjectSource {}

impl FromMessage<FetchOptionalProjectState> for LocalProjectSource {
    type Response = AsyncResponse<Option<Arc<ProjectState>>>;
    fn from_message(
        message: FetchOptionalProjectState,
        sender: Sender<Option<Arc<ProjectState>>>,
    ) -> Self {
        Self::FetchOptionalProjectState(message, sender)
    }
}

/// TODO: docs
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
        match message {
            LocalProjectSource::FetchOptionalProjectState(message, sender) => {
                self.handle_fetch_optional_project_state(message, sender)
            }
        }
    }

    async fn poll_local_states(&mut self) {
        let path = self.config.project_configs_path();
        let states = load_local_states(&path).await;
        match states {
            Ok(states) => self.local_states = states,
            Err(error) => relay_log::error!(
                "failed to load static project configs: {}",
                LogError(&error)
            ),
        };
    }
}

impl Service for LocalProjectSourceService {
    type Interface = LocalProjectSource;

    fn spawn_handler(mut self, mut rx: relay_system::Receiver<Self::Interface>) {
        tokio::spawn(async move {
            relay_log::info!("project local cache started");
            let mut ticker = tokio::time::interval(self.config.local_cache_interval());

            // Poll local states once before handling any message, such that the projects are
            // populated.
            self.poll_local_states().await;

            loop {
                tokio::select! {
                    biased;

                    _ = ticker.tick() => self.poll_local_states().await,
                    Some(message) = rx.recv() => self.handle_message(message),

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
