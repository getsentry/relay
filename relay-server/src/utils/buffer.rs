use std::fmt;
use std::path::{Path, PathBuf};

use sqlx::migrate::Migrator;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePool};

use relay_config::Config;

use crate::actors::project_buffer;
use crate::envelope::Envelope;
use crate::service::create_runtime;
use crate::statsd::RelayHistograms;
use crate::utils::{ManagedEnvelope, Semaphore, SemaphorePermit};

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

    /// Reserver the number of requested permits.
    ///
    /// Returns `Ok(Vec<SemaphorePermit>)` on success, which can be then use in
    /// `ManagedEnvelope::new` to create a managed envelope.
    pub fn try_reserve(&self, num: usize) -> Result<Vec<SemaphorePermit>, BufferError> {
        let permits = self.inner.try_reserve(num).ok_or(BufferError)?;

        relay_statsd::metric!(histogram(RelayHistograms::EnvelopeQueueSize) = self.used() as u64);
        relay_statsd::metric!(
            histogram(RelayHistograms::EnvelopeQueueSizePct) = {
                let queue_size_pct = self.used() as f64 * 100.0 / self.capacity as f64;
                queue_size_pct.floor() as u64
            }
        );

        Ok(permits)
    }

    /// Reserves resources for processing an envelope in Relay.
    ///
    /// Returns `Ok(ManagedEnvelope)` on success, which internally holds a handle to the reserved
    /// resources. When the managed envelope is dropped, the slot is automatically reclaimed and can
    /// be reused by a subsequent call to `enter`.
    ///
    /// If the buffer is full, this function returns `Err`.
    pub fn enter(&self, envelope: Box<Envelope>) -> Result<ManagedEnvelope, BufferError> {
        let permit = self.inner.try_acquire().ok_or(BufferError)?;

        relay_statsd::metric!(histogram(RelayHistograms::EnvelopeQueueSize) = self.used() as u64);

        relay_statsd::metric!(
            histogram(RelayHistograms::EnvelopeQueueSizePct) = {
                let queue_size_pct = self.used() as f64 * 100.0 / self.capacity as f64;
                queue_size_pct.floor() as u64
            }
        );

        Ok(ManagedEnvelope::new(envelope, permit))
    }
}

/// Run the persistent buffer setup with migrations if the persistent envelope buffer is enabled.
///
/// This function internally creates a Tokio runtime, and executes all the configuration steps
/// within its context. After the setup is done, used runtime will be dropped.
pub fn setup_persistent_buffer(config: &Config) -> Result<(), project_buffer::BufferError> {
    if let Some(buffer_config) = config.cache_persistent_buffer() {
        relay_log::info!("Configuring the persistent envelope buffer");

        let options = SqliteConnectOptions::new()
            .filename(PathBuf::from("sqlite://").join(buffer_config.buffer_path()))
            .journal_mode(SqliteJournalMode::Wal)
            .create_if_missing(true);

        // All DB operations are async and must be run in the context of the tokio runtime.
        // Also, the DB must be created and migrations run before all the services start, and if there
        // are any errors we must bail out ASAP.
        let setup_rt = create_runtime("buffer-setup", 1);
        setup_rt
            .block_on(async move {
                let pool = SqlitePool::connect_with(options).await?;
                let migrator = Migrator::new(Path::new("./migrations")).await?;
                migrator
                    .run(&pool)
                    .await
                    .map_err(project_buffer::BufferError::from)
            })
            .ok();
    }
    Ok(())
}
