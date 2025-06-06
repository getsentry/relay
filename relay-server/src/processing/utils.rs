use crate::processing::{Counted, Managed, OutcomeError, Rejected};
use crate::services::outcome::Outcome;

pub trait ManagedResult<T, E> {
    fn reject<M>(self, managed: &Managed<M>) -> Result<T, Rejected<E::Error>>
    where
        Self: Sized,
        M: Counted,
        E: OutcomeError;

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
