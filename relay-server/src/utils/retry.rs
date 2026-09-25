use std::time::Duration;

use backon::BackoffBuilder;
use backon::ExponentialBackoff;
use backon::ExponentialBuilder;

/// Backoff multiplier (1.5 which is 50% increase per backoff).
const DEFAULT_MULTIPLIER: f32 = 1.5;
/// Initial interval in milliseconds (1 second).
const INITIAL_INTERVAL: Duration = Duration::from_millis(1000);

/// A retry interval generator that increases timeouts with exponential backoff.
#[derive(Debug)]
pub struct RetryBackoff {
    builder: ExponentialBuilder,
    backoff: ExponentialBackoff,
    attempt: usize,
}

impl RetryBackoff {
    /// Creates a new retry backoff based on configured thresholds.
    pub fn new(max_interval: Duration) -> Self {
        let builder = ExponentialBuilder::new()
            .with_factor(DEFAULT_MULTIPLIER)
            .with_min_delay(INITIAL_INTERVAL)
            .with_max_delay(max_interval)
            .without_max_times();

        RetryBackoff {
            backoff: builder.build(),
            builder,
            attempt: 0,
        }
    }

    /// Resets this backoff to its initial state.
    pub fn reset(&mut self) {
        self.backoff = self.builder.build();
        self.attempt = 0;
    }

    /// Indicates whether a backoff attempt has started.
    pub fn started(&self) -> bool {
        self.attempt > 0
    }

    /// Returns the number of the retry attempt.
    pub fn attempt(&self) -> usize {
        self.attempt
    }

    /// Returns the next backoff duration.
    pub fn next_backoff(&mut self) -> Duration {
        let duration = match self.attempt {
            0 => Duration::ZERO,
            _ => self.backoff.next().unwrap(),
        };

        self.attempt += 1;
        duration
    }
}
