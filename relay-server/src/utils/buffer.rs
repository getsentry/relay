use std::fmt;

use relay_system::Addr;

use crate::envelope::Envelope;
use crate::services::outcome::TrackOutcome;
use crate::services::processor::ProcessingGroup;
use crate::services::test_store::TestStore;
use crate::statsd::RelayHistograms;
use crate::utils::{ManagedEnvelope, Semaphore};

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
    inner: Semaphore,
    capacity: usize,
    high_watermark: f64,
    low_watermark: f64,
}

impl BufferGuard {
    /// Creates a new `BufferGuard` based on config values.
    pub fn new(capacity: usize) -> Self {
        let inner = Semaphore::new(capacity);
        Self {
            inner,
            capacity,
            high_watermark: 0.8,
            low_watermark: 0.5,
        }
    }

    /// Returns the current usage of `BufferGuard` permits.
    #[inline]
    fn usage(&self) -> f64 {
        self.used() as f64 / self.capacity() as f64
    }

    /// Returns `true` if the `BufferGuard` exhausted more permits then defined in
    /// `high_watermark`.
    #[inline]
    pub fn is_over_high_watermark(&self) -> bool {
        self.usage() >= self.high_watermark
    }

    /// Returns `true` if the `BufferGuard` number of permits is still under the `low_watermark`.
    #[inline]
    pub fn is_below_low_watermark(&self) -> bool {
        self.usage() <= self.low_watermark
    }

    /// Returns the total capacity of the pipeline.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Returns the unused capacity of the pipeline.
    pub fn available(&self) -> usize {
        self.inner.available()
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
        group: ProcessingGroup,
    ) -> Result<ManagedEnvelope, BufferError> {
        let permit = self.inner.try_acquire().ok_or(BufferError)?;

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
            group,
        ))
    }
}
