use std::time::{Duration, Instant};

use backoff::backoff::Backoff;
use backoff::ExponentialBackoff;

/// Backoff multiplier (1.5 which is 50% increase per backoff).
const DEFAULT_MULTIPLIER: f64 = 1.5;
/// Randomization factor (0 which is no randomization).
const DEFAULT_RANDOMIZATION: f64 = 0.0;
/// Initial interval in milliseconds (1 second).
const INITIAL_INTERVAL: u64 = 1000;

/// A retry interval generator that increases timeouts with exponential backoff.
#[derive(Debug)]
pub struct RetryBackoff {
    backoff: ExponentialBackoff,
    attempt: usize,
}

impl RetryBackoff {
    /// Creates a new retry backoff based on configured thresholds.
    pub fn new(max_interval: Duration) -> Self {
        let backoff = ExponentialBackoff {
            current_interval: Duration::from_millis(INITIAL_INTERVAL),
            initial_interval: Duration::from_millis(INITIAL_INTERVAL),
            randomization_factor: DEFAULT_RANDOMIZATION,
            multiplier: DEFAULT_MULTIPLIER,
            max_interval,
            max_elapsed_time: None,
            clock: Default::default(),
            start_time: Instant::now(),
        };

        RetryBackoff {
            backoff,
            attempt: 0,
        }
    }

    /// Resets this backoff to its initial state.
    pub fn reset(&mut self) {
        self.backoff.reset();
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
            0 => Duration::new(0, 0),
            _ => self.backoff.next_backoff().unwrap(),
        };

        self.attempt += 1;
        duration
    }
}
