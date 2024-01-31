use std::collections::HashMap;
use std::ffi::OsStr;
use std::path::Path;
use std::sync::Arc;

use relay_base_schema::project::{ProjectId, ProjectKey};
use relay_config::Config;
use relay_system::{AsyncResponse, FromMessage, Interface, Receiver, Sender, Service};
use tokio::sync::mpsc;
use tokio::time::Instant;

use crate::services::project::ProjectState;
use crate::services::project_cache::FetchOptionalProjectState;

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
    relay_log::debug!(directory = ?projects_path, "loading local states from file system");

    while let Some(entry) = directory.next_entry().await? {
        let path = entry.path();

        let metadata = entry.metadata().await?;
        if !(metadata.is_file() || metadata.is_symlink()) {
            relay_log::warn!(?path, "skipping file, not a file");
            continue;
        }

        if path.extension().map(|x| x != "json").unwrap_or(true) {
            relay_log::warn!(?path, "skipping file, file extension must be .json");
            continue;
        }

        // serde_json is not async, so spawn a blocking task here:
        let (path, state) = tokio::task::spawn_blocking(move || parse_file(path)).await??;

        let mut sanitized = ProjectState::sanitize(state);
        if sanitized.project_id.is_none() {
            if let Some(project_id) = get_project_id(&path) {
                sanitized.project_id = Some(project_id);
            } else {
                relay_log::warn!(?path, "skipping file, filename is not a valid project id");
                continue;
            }
        }

        // Keep a separate project state per key.
        let keys = std::mem::take(&mut sanitized.public_keys);
        for key in keys {
            sanitized.public_keys = smallvec::smallvec![key.clone()];
            states.insert(key.public_key, Arc::new(sanitized.clone()));
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
            error = &error as &dyn std::error::Error,
            "failed to load static project configs",
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

/// This works only on Unix systems.
#[cfg(not(target_os = "windows"))]
#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;
    use crate::services::project::{ProjectState, PublicKeyConfig};

    /// Tests that we can follow the symlinks and read the project file properly.
    #[tokio::test]
    async fn test_symlinked_projects() {
        let temp1 = tempfile::tempdir().unwrap();
        let temp2 = tempfile::tempdir().unwrap();

        let tmp_project_file = "111111.json";
        let project_key = ProjectKey::parse("55f6b2d962564e99832a39890ee4573e").unwrap();

        let mut tmp_project_state = ProjectState::allowed();
        tmp_project_state.public_keys.push(PublicKeyConfig {
            public_key: project_key,
            numeric_id: None,
        });

        // create the project file
        let project_state = serde_json::to_string(&tmp_project_state).unwrap();
        tokio::fs::write(
            temp1.path().join(tmp_project_file),
            project_state.as_bytes(),
        )
        .await
        .unwrap();

        tokio::fs::symlink(
            temp1.path().join(tmp_project_file),
            temp2.path().join(tmp_project_file),
        )
        .await
        .unwrap();

        let extracted_project_state = load_local_states(temp2.path()).await.unwrap();

        assert_eq!(
            extracted_project_state
                .get(&project_key)
                .unwrap()
                .project_id,
            Some(ProjectId::from_str("111111").unwrap())
        );

        assert_eq!(
            extracted_project_state
                .get(&project_key)
                .unwrap()
                .public_keys
                .first()
                .unwrap()
                .public_key,
            project_key,
        )
    }

    #[tokio::test]
    async fn test_multi_pub_static_config() {
        let temp = tempfile::tempdir().unwrap();

        let tmp_project_file = "111111.json";
        let project_key1 = ProjectKey::parse("55f6b2d962564e99832a39890ee4573e").unwrap();
        let project_key2 = ProjectKey::parse("55bbb2d96256bb9983bb39890bb457bb").unwrap();

        let mut tmp_project_state = ProjectState::allowed();
        tmp_project_state.public_keys.extend(vec![
            PublicKeyConfig {
                public_key: project_key1,
                numeric_id: None,
            },
            PublicKeyConfig {
                public_key: project_key2,
                numeric_id: None,
            },
        ]);

        // create the project file
        let project_state = serde_json::to_string(&tmp_project_state).unwrap();
        tokio::fs::write(temp.path().join(tmp_project_file), project_state.as_bytes())
            .await
            .unwrap();

        let extracted_project_state = load_local_states(temp.path()).await.unwrap();

        assert_eq!(extracted_project_state.len(), 2);
        assert!(extracted_project_state.get(&project_key1).is_some());
        assert!(extracted_project_state.get(&project_key2).is_some());
    }
}
