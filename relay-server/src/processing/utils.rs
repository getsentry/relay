use crate::processing::{Counted, Managed, OutcomeError, Rejected};
use crate::services::outcome::Outcome;

/// Extension trait for Results, which adds convenience functions to work with errors and [`Managed`].
pub trait ManagedResult<T, E> {
    /// Reject the entire [`Managed`] instance with `E`.
    fn reject<M>(self, managed: &Managed<M>) -> Result<T, Rejected<E::Error>>
    where
        Self: Sized,
        M: Counted,
        E: OutcomeError;

    /// Wraps the error of the [`Result`] with an outcome.
    fn with_outcome(self, outcome: Outcome) -> Result<T, (Outcome, E)>
    where
        Self: Sized;
}

impl<T, E> ManagedResult<T, E> for Result<T, E> {
    fn reject<M>(self, managed: &Managed<M>) -> Result<T, Rejected<E::Error>>
    where
        Self: Sized,
        M: Counted,
        E: OutcomeError,
    {
        self.map_err(|err| managed.reject_err(err))
    }

    fn with_outcome(self, outcome: Outcome) -> Result<T, (Outcome, E)>
    where
        Self: Sized,
    {
        self.map_err(|err| (outcome, err))
    }
}
