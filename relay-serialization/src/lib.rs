//! Relay serialization toolkit.
//!
//! Used to ensure we can bound the work when deserializing untrusted data.
#![recursion_limit = "256"]
#![warn(missing_docs)]

use std::fmt;

/// A budget for the ops a single deserialization is allowed to spend.
struct Meter {
    limit: usize,
    remaining: usize,
    exceeded: bool,
}

impl Meter {
    /// Creates a meter which allows spending at most `limit` operations.
    pub fn new(limit: usize) -> Self {
        Self {
            limit,
            remaining: limit,
            exceeded: false,
        }
    }

    /// Returns the number of ops spent.
    pub fn spent(&self) -> usize {
        self.limit - self.remaining
    }

    /// Returns true if we've exceeded our budget.
    pub fn exceeded(&self) -> bool {
        self.exceeded
    }

    /// Tries to charge `amount` operations to the budget.  If we exceed, we return an error,
    /// set the remaining budget to 0, and mark the budget as exceeded.
    pub fn spend(&mut self, amount: usize) -> Result<(), LimitExceeded> {
        match self.remaining.checked_sub(amount) {
            Some(remaining) => {
                self.remaining = remaining;
                Ok(())
            }
            None => {
                self.remaining = 0;
                self.exceeded = true;
                Err(LimitExceeded)
            }
        }
    }
}

/// The error produced when a [`Meter`] runs out of budget.
struct LimitExceeded;

impl fmt::Display for LimitExceeded {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "deserialization exceeds the operation budget")
    }
}

pub mod prost;
pub mod serde;
