use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs;
use std::io;
use std::path::Path;
use std::sync::Arc;
use std::thread;

use actix::prelude::*;
use futures::{sync::oneshot, Future};

use relay_common::{LogError, ProjectId};
use relay_config::Config;

use crate::actors::project::ProjectState;
use crate::actors::project_cache::FetchOptionalProjectState;

pub struct LocalProjectSource {
    config: Arc<Config>,
    local_states: HashMap<ProjectId, Arc<ProjectState>>,
}

impl LocalProjectSource {
    pub fn new(config: Arc<Config>) -> Self {
        LocalProjectSource {
            config,
            local_states: HashMap::new(),
        }
    }
}

impl Actor for LocalProjectSource {
    type Context = Context<Self>;

    fn started(&mut self, context: &mut Self::Context) {
        log::info!("project local cache started");

        // Start the background thread that reads the local states from disk.
        // `poll_local_states` returns a future that resolves as soon as the first read is done.
        poll_local_states(context.address(), self.config.clone())
            .into_actor(self)
            // Block entire actor on first local state read, such that we don't e.g. drop events on
            // startup
            .wait(context);
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        log::info!("project local cache stopped");
    }
}

impl Handler<FetchOptionalProjectState> for LocalProjectSource {
    type Result = Option<Arc<ProjectState>>;

    fn handle(
        &mut self,
        message: FetchOptionalProjectState,
        _context: &mut Self::Context,
    ) -> Self::Result {
        self.local_states.get(&message.id).cloned()
    }
}

struct UpdateLocalStates {
    states: HashMap<ProjectId, Arc<ProjectState>>,
}

impl Message for UpdateLocalStates {
    type Result = ();
}

impl Handler<UpdateLocalStates> for LocalProjectSource {
    type Result = ();

    fn handle(&mut self, message: UpdateLocalStates, _context: &mut Context<Self>) -> Self::Result {
        self.local_states = message.states;
    }
}

fn load_local_states(projects_path: &Path) -> io::Result<HashMap<ProjectId, Arc<ProjectState>>> {
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
    log::debug!("Loading local states from directory {:?}", projects_path);

    for entry in directory {
        let entry = entry?;
        let path = entry.path();

        if !entry.metadata()?.is_file() {
            log::warn!("skipping {:?}, not a file", path);
            continue;
        }

        if path.extension().map(|x| x != "json").unwrap_or(true) {
            log::warn!("skipping {:?}, file extension must be .json", path);
            continue;
        }

        let id = match path
            .file_stem()
            .and_then(OsStr::to_str)
            .and_then(|stem| stem.parse().ok())
        {
            Some(id) => id,
            None => {
                log::warn!("skipping {:?}, filename is not a valid project id", path);
                continue;
            }
        };

        let state = serde_json::from_reader(io::BufReader::new(fs::File::open(path)?))?;
        states.insert(id, Arc::new(state));
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
                    manager.do_send(UpdateLocalStates { states });
                    sender.take().map(|sender| sender.send(()).ok());
                }
                Err(error) => log::error!(
                    "failed to load static project configs: {}",
                    LogError(&error)
                ),
            }

            thread::sleep(config.local_cache_interval());
        }
    });

    receiver.map_err(|_| ())
}
