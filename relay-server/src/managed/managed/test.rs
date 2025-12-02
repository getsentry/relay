use std::net::IpAddr;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use relay_base_schema::organization::OrganizationId;
use relay_base_schema::project::{ProjectId, ProjectKey};
use relay_event_schema::protocol::EventId;
use relay_quotas::{DataCategory, Scoping};
use relay_system::Addr;
use tokio::sync::mpsc::{UnboundedReceiver, error::TryRecvError};

use crate::managed::managed::Meta;
use crate::managed::{Counted, Managed};
use crate::services::outcome::{DiscardReason, Outcome, TrackOutcome};

impl<T: Counted> Managed<T> {
    /// Creates a [`Managed`] instance for unit testing.
    pub fn for_test(value: T) -> ManagedTestBuilder<T> {
        ManagedTestBuilder::new(value)
    }
}

/// Builder to create mocked [`Managed`] instances.
pub struct ManagedTestBuilder<T> {
    value: T,
    outcome_aggregator: Addr<TrackOutcome>,
    outcome_aggregator_rx: UnboundedReceiver<TrackOutcome>,
    received_at: DateTime<Utc>,
    scoping: Scoping,
    event_id: Option<EventId>,
    remote_addr: Option<IpAddr>,
}

impl<T> ManagedTestBuilder<T> {
    fn new(value: T) -> Self {
        let (outcome_aggregator, outcome_aggregator_rx) = Addr::custom();

        Self {
            value,
            outcome_aggregator,
            outcome_aggregator_rx,
            received_at: Utc::now(),
            scoping: Scoping {
                organization_id: OrganizationId::new(1),
                project_id: ProjectId::new(42),
                project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
                key_id: Some(3),
            },
            event_id: None,
            remote_addr: None,
        }
    }

    /// Creates a new [`Managed`] instance and a [`ManagedTestHandle`] to assert outcomes.
    pub fn build(self) -> (Managed<T>, ManagedTestHandle)
    where
        T: Counted,
    {
        let managed = Managed::from_parts(
            self.value,
            Arc::new(Meta {
                outcome_aggregator: self.outcome_aggregator,
                received_at: self.received_at,
                scoping: self.scoping,
                event_id: self.event_id,
                remote_addr: self.remote_addr,
            }),
        );
        let handle = ManagedTestHandle {
            outcomes: self.outcome_aggregator_rx,
            received_at: self.received_at,
            scoping: self.scoping,
            event_id: self.event_id,
            remote_addr: self.remote_addr,
        };

        (managed, handle)
    }
}

/// A testing helper which can make sure the right outcomes have been emitted by the [`Managed`] instance.
pub struct ManagedTestHandle {
    outcomes: UnboundedReceiver<TrackOutcome>,
    received_at: DateTime<Utc>,
    scoping: Scoping,
    event_id: Option<EventId>,
    remote_addr: Option<IpAddr>,
}

impl ManagedTestHandle {
    /// Asserts that no outcomes have been emitted from the associated [`Managed`] instance.
    #[track_caller]
    pub fn assert_no_outcomes(&mut self) {
        match self.outcomes.try_recv() {
            Ok(next) => panic!("expected no more outcomes, got {next:?}"),
            Err(TryRecvError::Empty | TryRecvError::Disconnected) => {}
        }
    }

    /// Asserts a single emitted outcome.
    #[track_caller]
    pub fn assert_outcome(&mut self, outcome: &Outcome, category: DataCategory, quantity: u32) {
        match self.outcomes.try_recv() {
            Ok(next) => {
                assert_eq!(&next.outcome, outcome);
                assert_eq!(next.category, category);
                assert_eq!(next.quantity, quantity);
                assert_eq!(next.timestamp, self.received_at);
                assert_eq!(next.scoping, self.scoping);
                assert_eq!(next.event_id, self.event_id);
                assert_eq!(next.remote_addr, self.remote_addr);
            }
            Err(TryRecvError::Empty | TryRecvError::Disconnected) => panic!(
                "expected an outcome '{outcome}' with quantity '{quantity}', but none were emitted"
            ),
        }
    }

    /// Asserts a single emitted invalid outcome.
    ///
    /// A shorthand for [`Self::assert_outcome`] with a [`Outcome::Invalid`] /
    /// [`DiscardReason::Internal`].
    #[track_caller]
    pub fn assert_internal_outcome(&mut self, category: DataCategory, quantity: u32) {
        self.assert_outcome(
            &Outcome::Invalid(DiscardReason::Internal),
            category,
            quantity,
        );
    }
}

impl Drop for ManagedTestHandle {
    fn drop(&mut self) {
        // If there is already an ongoing panic, we're possible in an inconsistent state already
        // and adding another panic to it makes it just more confusing.
        if std::thread::panicking() {
            return;
        }

        match self.outcomes.try_recv() {
            Ok(next) => panic!("expected no more outcomes, got {next:?}"),
            Err(TryRecvError::Empty) => {
                panic!("expected no more outcomes, but managed instance has not been dropped yet")
            }
            Err(TryRecvError::Disconnected) => {}
        }
    }
}
