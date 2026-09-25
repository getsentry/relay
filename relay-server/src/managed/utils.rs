use crate::managed::{Counted, Managed, OutcomeError, Rejected};
use crate::services::outcome::Outcome;

/// Extension trait for Results, which adds convenience functions to work with errors and [`Managed`].
pub trait ManagedResult<T, E> {
    /// Reject the entire [`Managed`] instance with `E`.
    fn reject<M>(self, managed: &Managed<M>) -> Result<T, Rejected<E::Error>>
    where
        Self: Sized,
        M: Counted,
        E: OutcomeError;

    /// Reject two [`Managed`] instances while still only returning a single [`Rejected`] error in
    /// the result.
    fn reject2<M, N>(
        self,
        managed1: &Managed<M>,
        managed2: &Managed<N>,
    ) -> Result<T, Rejected<E::Error>>
    where
        Self: Sized,
        M: Counted,
        N: Counted,
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

    fn reject2<M, N>(
        self,
        managed1: &Managed<M>,
        managed2: &Managed<N>,
    ) -> Result<T, Rejected<E::Error>>
    where
        Self: Sized,
        M: Counted,
        N: Counted,
        E: OutcomeError,
    {
        self.map_err(|err| {
            let (outcome, err) = err.consume();
            let _ = managed1.reject_err(outcome.clone());
            let r = managed2.reject_err(outcome);
            r.map(|_| err)
        })
    }

    fn with_outcome(self, outcome: Outcome) -> Result<T, (Outcome, E)>
    where
        Self: Sized,
    {
        self.map_err(|err| (outcome, err))
    }
}
