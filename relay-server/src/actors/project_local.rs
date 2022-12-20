use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs;
use std::io;
use std::path::Path;
use std::sync::Arc;
use std::thread;

use futures01::{sync::oneshot, Future};

use relay_common::{ProjectId, ProjectKey};
use relay_config::Config;
use relay_log::LogError;
use relay_system::Addr;
use relay_system::AsyncResponse;
use relay_system::FromMessage;
use relay_system::Interface;
use relay_system::NoResponse;
use relay_system::Sender;
use relay_system::Service;

use crate::actors::project::ProjectState;
use crate::actors::project_cache::FetchOptionalProjectState;

/// TODO: docs
#[derive(Debug)]
pub struct UpdateLocalStates {
    states: HashMap<ProjectKey, Arc<ProjectState>>,
}

/// Service interface of the local project source.
#[derive(Debug)]
pub enum LocalProjectSource {
    /// TODO: docs
    FetchOptionalProjectState(FetchOptionalProjectState, Sender<Option<Arc<ProjectState>>>),
    /// TODO: docs
    UpdateLocalStates(UpdateLocalStates),
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

impl FromMessage<UpdateLocalStates> for LocalProjectSource {
    type Response = NoResponse;
    fn from_message(message: UpdateLocalStates, _sender: ()) -> Self {
        Self::UpdateLocalStates(message)
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

    fn handle_update_local_states(&mut self, message: UpdateLocalStates) {
        self.local_states = message.states;
    }

    async fn handle_message(&mut self, message: LocalProjectSource) {
        match message {
            LocalProjectSource::FetchOptionalProjectState(message, sender) => {
                self.handle_fetch_optional_project_state(message, sender)
            }
            LocalProjectSource::UpdateLocalStates(message) => {
                self.handle_update_local_states(message)
            }
        }
    }
}

impl Service for LocalProjectSourceService {
    type Interface = LocalProjectSource;

    fn spawn_handler(mut self, mut rx: relay_system::Receiver<Self::Interface>) {
        tokio::spawn(async move {
            while let Some(message) = rx.recv().await {
                self.handle_message(message).await;
                // TODO: do we need a shutdown handler here?
            }
        });
    }
}

// impl Actor for LocalProjectSource {
//     type Context = Context<Self>;

//     fn started(&mut self, context: &mut Self::Context) {
//         relay_log::info!("project local cache started");

//         // Start the background thread that reads the local states from disk.
//         // `poll_local_states` returns a future that resolves as soon as the first read is done.
//         poll_local_states(context.address(), self.config.clone())
//             .into_actor(self)
//             // Block entire actor on first local state read, such that we don't e.g. drop events on
//             // startup
//             .wait(context);
//     }

//     fn stopped(&mut self, _ctx: &mut Self::Context) {
//         relay_log::info!("project local cache stopped");
//     }
// }

fn get_project_id(path: &Path) -> Option<ProjectId> {
    path.file_stem()
        .and_then(OsStr::to_str)
        .and_then(|stem| stem.parse().ok())
}

fn load_local_states(projects_path: &Path) -> io::Result<HashMap<ProjectKey, Arc<ProjectState>>> {
    let mut states = HashMap::new();

    let directory = match fs::read_dir(projects_path) {
        Ok(directory) => directory,
        Err(error) => {
            return match error.kind() {
                io::ErrorKind::NotFound => Ok(states),
                _ => Err(error),
            };
        }
    };

    // only printed when directory even exists.
    relay_log::debug!("Loading local states from directory {:?}", projects_path);

    for entry in directory {
        let entry = entry?;
        let path = entry.path();

        if !entry.metadata()?.is_file() {
            relay_log::warn!("skipping {:?}, not a file", path);
            continue;
        }

        if path.extension().map(|x| x != "json").unwrap_or(true) {
            relay_log::warn!("skipping {:?}, file extension must be .json", path);
            continue;
        }

        let state = serde_json::from_reader(io::BufReader::new(fs::File::open(&path)?))?;
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

fn poll_local_states(
    manager: Addr<LocalProjectSource>,
    config: Arc<Config>,
) -> impl Future<Item = (), Error = ()> {
    let (sender, receiver) = oneshot::channel();

    let _ = thread::spawn(move || {
        let path = config.project_configs_path();
        let mut sender = Some(sender);

        loop {
            match load_local_states(&path) {
                Ok(states) => {
                    manager.send(UpdateLocalStates { states }); // TODO: test this
                    sender.take().map(|sender| sender.send(()).ok());
                }
                Err(error) => relay_log::error!(
                    "failed to load static project configs: {}",
                    LogError(&error)
                ),
            }

            thread::sleep(config.local_cache_interval());
        }
    });

    receiver.map_err(|_| ())
}
