use std::sync::Arc;

use relay_base_schema::project::ProjectKey;
use relay_config::Config;
use tokio::sync::mpsc;

use crate::services::buffer::EnvelopeBuffer;
use crate::services::projects::cache::ProjectSource;
use crate::services::projects::cache2::state::{CompletedFetch, Fetch};
use crate::services::projects::project::{ProjectFetchState, ProjectState};

pub enum ProjectCache {
    Fetch(ProjectKey),
}

impl relay_system::Interface for ProjectCache {}

impl relay_system::FromMessage<Self> for ProjectCache {
    type Response = relay_system::NoResponse;

    fn from_message(message: Self, _: ()) -> Self {
        message
    }
}

pub struct ProjectCacheService {
    store: super::state::ProjectStore,
    source: ProjectSource,
    config: Arc<Config>,

    buffer: relay_system::Addr<EnvelopeBuffer>,

    project_update_rx: mpsc::UnboundedReceiver<CompletedFetch>,
    project_update_tx: mpsc::UnboundedSender<CompletedFetch>,
}

impl ProjectCacheService {
    fn schedule_fetch(&self, fetch: Fetch) {
        let source = self.source.clone();
        let project_updates = self.project_update_tx.clone();

        tokio::spawn(async move {
            tokio::time::sleep_until(fetch.when().into()).await;

            // TODO: cached state for delta fetches, maybe this should just be a revision?
            let state = match source
                .fetch(fetch.project_key(), false, ProjectFetchState::pending())
                .await
            {
                // TODO: verify if the sanitized here is correct
                Ok(state) => state.sanitized().into(),
                Err(err) => {
                    relay_log::error!(
                        error = &err as &dyn std::error::Error,
                        "failed to fetch project state for {fetch:?}"
                    );
                    ProjectState::Pending
                }
            };

            project_updates.send(fetch.complete(state));
        });
    }
}

impl ProjectCacheService {
    fn handle_fetch(&mut self, project_key: ProjectKey) {
        if let Some(fetch) = self.store.try_begin_fetch(project_key, &self.config) {
            self.schedule_fetch(fetch);
        }
    }

    fn handle_project_update(&mut self, fetch: CompletedFetch) {
        let project_key = fetch.project_key();

        if let Some(fetch) = self.store.complete_fetch(fetch, &self.config) {
            relay_log::trace!(
                project_key = fetch.project_key().as_str(),
                "re-scheduling project fetch: {fetch:?}"
            );
            self.schedule_fetch(fetch);
            return;
        }

        // TODO: should there just be a broadcast channel for events?
        self.buffer.send(EnvelopeBuffer::Ready(project_key));

        // TODO: notify spool (prevous merge_state)
    }

    fn handle_evict_stale_projects(&mut self) {
        self.store.evict_stale_projects(&self.config);

        // TODO: notify spool (previous evict_stale_project_caches)
    }

    fn handle(&mut self, message: ProjectCache) {
        match message {
            ProjectCache::Fetch(project_key) => self.handle_fetch(project_key),
        }
    }
}

impl relay_system::Service for ProjectCacheService {
    type Interface = ProjectCache;

    fn spawn_handler(mut self, mut rx: relay_system::Receiver<Self::Interface>) {
        tokio::spawn(async move {
            let mut eviction_ticker = tokio::time::interval(self.config.cache_eviction_interval());

            loop {
                tokio::select! {
                    biased;

                    Some(update) = self.project_update_rx.recv() => {
                        self.handle_project_update(update)
                    },
                    Some(message) = rx.recv() => {
                        self.handle(message);
                    },
                    _ = eviction_ticker.tick() => {
                        self.handle_evict_stale_projects()
                    }
                }
            }
        });
    }
}
