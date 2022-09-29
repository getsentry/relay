use std::collections::BTreeMap;
use std::sync::Arc;

use relay_config::{Config, RelayMode};
use relay_general::protocol::EventId;
use relay_system::{Addr, AsyncResponse, FromMessage, NoResponse, Sender};

use crate::actors::outcome::Outcome;
use crate::envelope::Envelope;
use crate::service::REGISTRY;

/// Either a captured envelope or an error that occured during processing.
pub type CapturedEnvelope = Result<Envelope, String>;

/// Inserts an envelope or failure into internal captures.
///
/// Can be retrieved using [`GetCapturedEnvelope`]. Use [`Capture::should_capture`] to check whether
/// the message should even be sent to reduce the overheads.
#[derive(Debug)]
pub struct Capture {
    event_id: Option<EventId>,
    capture: CapturedEnvelope,
}

impl Capture {
    /// Returns `true` if Relay is in capture mode.
    ///
    /// The `Capture` message can still be sent and and will be ignored. This function is purely for
    /// optimization purposes.
    pub fn should_capture(config: &Config) -> bool {
        matches!(config.relay_mode(), RelayMode::Capture)
    }

    /// Captures an accepted envelope.
    pub fn accepted(envelope: Envelope) -> Self {
        Self {
            event_id: envelope.event_id(),
            capture: Ok(envelope),
        }
    }

    /// Captures the error that lead to envelope rejection.
    pub fn rejected(event_id: Option<EventId>, outcome: &Outcome) -> Self {
        Self {
            event_id,
            capture: Err(outcome.to_string()),
        }
    }
}

/// Resolves a [`CapturedEnvelope`] by the given `event_id`.
#[derive(Debug)]
pub struct GetCapturedEnvelope {
    pub event_id: EventId,
}

/// Stores and retrieves Envelopes for integration testing.
#[derive(Debug)]
pub enum TestStore {
    Capture(Box<Capture>),
    Get(GetCapturedEnvelope, Sender<Option<CapturedEnvelope>>),
}

impl TestStore {
    pub fn from_registry() -> Addr<Self> {
        REGISTRY.get().unwrap().test_store.clone()
    }
}

impl relay_system::Interface for TestStore {}

impl FromMessage<Capture> for TestStore {
    type Response = NoResponse;

    fn from_message(message: Capture, _: ()) -> Self {
        Self::Capture(Box::new(message))
    }
}

impl FromMessage<GetCapturedEnvelope> for TestStore {
    type Response = AsyncResponse<Option<CapturedEnvelope>>;

    fn from_message(
        message: GetCapturedEnvelope,
        sender: Sender<Option<CapturedEnvelope>>,
    ) -> Self {
        Self::Get(message, sender)
    }
}

/// Service implementing the [`TestStore`] interface.
pub struct TestStoreService {
    config: Arc<Config>,
    captures: BTreeMap<EventId, CapturedEnvelope>,
}

impl TestStoreService {
    pub fn new(config: Arc<Config>) -> Self {
        Self {
            config,
            captures: BTreeMap::new(),
        }
    }

    fn capture(&mut self, msg: Capture) {
        if let RelayMode::Capture = self.config.relay_mode() {
            match (msg.event_id, msg.capture) {
                (Some(event_id), Ok(envelope)) => {
                    relay_log::debug!("capturing envelope");
                    self.captures.insert(event_id, Ok(envelope));
                }
                (Some(event_id), Err(message)) => {
                    relay_log::debug!("capturing failed event {}", event_id);
                    self.captures.insert(event_id, Err(message));
                }

                // XXX: does not work with envelopes without event_id
                (None, Ok(_)) => relay_log::debug!("dropping non event envelope"),
                (None, Err(_)) => relay_log::debug!("dropping failed envelope without event"),
            }
        }
    }

    fn get(&self, message: GetCapturedEnvelope) -> Option<CapturedEnvelope> {
        self.captures.get(&message.event_id).cloned()
    }

    fn handle_message(&mut self, message: TestStore) {
        match message {
            TestStore::Capture(message) => self.capture(*message),
            TestStore::Get(message, sender) => sender.send(self.get(message)),
        }
    }
}

impl relay_system::Service for TestStoreService {
    type Interface = TestStore;

    fn spawn_handler(mut self, mut rx: relay_system::Receiver<Self::Interface>) {
        tokio::spawn(async move {
            while let Some(message) = rx.recv().await {
                self.handle_message(message);
            }
        });
    }
}
