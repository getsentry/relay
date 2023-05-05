use std::fmt;
use std::sync::Arc;

use relay_system::Addr;
use tokio::sync::Semaphore;

use crate::actors::outcome::TrackOutcome;
use crate::actors::test_store::TestStore;
use crate::envelope::Envelope;
use crate::statsd::RelayHistograms;
use crate::utils::ManagedEnvelope;

/// An error returned by [`BufferGuard::enter`] indicating that the buffer capacity has been
/// exceeded.
#[derive(Debug)]
pub struct BufferError;

impl fmt::Display for BufferError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "envelope buffer capacity exceeded")
    }
}

impl std::error::Error for BufferError {}

/// Access control for envelope processing.
///
/// The buffer guard is basically a semaphore that ensures the buffer does not outgrow the maximum
/// number of envelopes configured through `envelope_buffer_size`. To enter a new envelope
/// into the processing pipeline, use [`BufferGuard::enter`].
#[derive(Debug)]
pub struct BufferGuard {
    inner: Arc<tokio::sync::Semaphore>,
    capacity: usize,
}

impl BufferGuard {
    /// Creates a new `BufferGuard` based on config values.
    pub fn new(capacity: usize) -> Self {
        let inner = Semaphore::new(capacity).into();
        Self { inner, capacity }
    }

    /// Returns the unused capacity of the pipeline.
    pub fn available(&self) -> usize {
        self.inner.available_permits()
    }

    /// Returns the number of envelopes in the pipeline.
    pub fn used(&self) -> usize {
        self.capacity.saturating_sub(self.available())
    }

    /// Reserves resources for processing an envelope in Relay.
    ///
    /// Returns `Ok(ManagedEnvelope)` on success, which internally holds a handle to the reserved
    /// resources. When the managed envelope is dropped, the slot is automatically reclaimed and can
    /// be reused by a subsequent call to `enter`.
    ///
    /// If the buffer is full, this function returns `Err`.
    pub fn enter(
        &self,
        envelope: Box<Envelope>,
        outcome_aggregator: Addr<TrackOutcome>,
        test_store: Addr<TestStore>,
    ) -> Result<ManagedEnvelope, BufferError> {
        let permit = self.inner.try_acquire_owned().map_err(|_| BufferError)?;

        relay_statsd::metric!(histogram(RelayHistograms::EnvelopeQueueSize) = self.used() as u64);

        relay_statsd::metric!(
            histogram(RelayHistograms::EnvelopeQueueSizePct) = {
                let queue_size_pct = self.used() as f64 * 100.0 / self.capacity as f64;
                queue_size_pct.floor() as u64
            }
        );

        Ok(ManagedEnvelope::new(
            envelope,
            permit,
            outcome_aggregator,
            test_store,
        ))
    }
}
