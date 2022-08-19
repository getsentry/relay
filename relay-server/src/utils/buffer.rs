use std::fmt;

use crate::envelope::Envelope;
use crate::statsd::RelayHistograms;
use crate::utils::{EnvelopeContext, Semaphore};

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
/// number of envelopes configured through [`Config::envelope_buffer_size`]. To enter a new envelope
/// into the processing pipeline, use [`BufferGuard::enter`].
#[derive(Debug)]
pub struct BufferGuard {
    inner: Semaphore,
    capacity: usize,
}

impl BufferGuard {
    /// Creates a new `BufferGuard` based on config values.
    pub fn new(capacity: usize) -> Self {
        let inner = Semaphore::new(capacity);
        Self { inner, capacity }
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
    /// Returns `Ok(EnvelopeContext)` on success, which internally holds a handle to the reserved
    /// resources. When the envelope context is dropped, the slot is automatically reclaimed and can
    /// be reused by a subsequent call to `enter`.
    ///
    /// If the buffer is full, this function returns `Err`.
    pub fn enter(&self, envelope: &Envelope) -> Result<EnvelopeContext, BufferError> {
        let permit = self.inner.try_acquire().ok_or(BufferError)?;

        relay_statsd::metric!(histogram(RelayHistograms::EnvelopeQueueSize) = self.used() as u64);

        relay_statsd::metric!(
            histogram(RelayHistograms::EnvelopeQueueSizePct) = {
                let queue_size_pct = self.used() as f64 * 100.0 / self.capacity as f64;
                queue_size_pct.floor() as u64
            }
        );

        Ok(EnvelopeContext::new(envelope, permit))
    }
}
