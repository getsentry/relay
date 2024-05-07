use std::marker::PhantomData;
use std::ops::Deref;

use relay_metrics::aggregator::AggregatorBuckets;
use relay_metrics::Bucket;

mod post;
mod pre;

pub use self::post::PostProjectTransform;
pub use self::pre::PreProjectTransform;

/// Transforms [`Buckets`] into different [`Buckets`] applying pre-defined transforms.
pub trait Transformer<S> {
    /// The new state the buckets will be transformed into.
    type NewState;

    /// Transforms [`Buckets`] with state `S` into a new vector of buckets in state [`Self::NewState`].
    fn transform(self, buckets: Vec<Bucket>) -> Vec<Bucket>;
}

/// Container for a vector of buckets.
#[derive(Debug)]
pub struct Buckets<State = ()> {
    buckets: Vec<Bucket>,
    _state: PhantomData<State>,
}

impl Buckets {
    /// Creates a list of buckets in their initial state.
    pub fn new(buckets: Vec<Bucket>) -> Self {
        Self {
            buckets,
            _state: PhantomData,
        }
    }
}

impl<S> Buckets<S> {
    /// Applies a [`Transformer`] to the contained list of buckets.
    pub fn transform<T: Transformer<S>>(self, transformer: T) -> Buckets<T::NewState> {
        Buckets {
            buckets: transformer.transform(self.buckets),
            _state: PhantomData,
        }
    }
}

impl<S> Deref for Buckets<S> {
    type Target = [Bucket];

    fn deref(&self) -> &Self::Target {
        &self.buckets
    }
}

impl<S> AggregatorBuckets for Buckets<S> {
    fn from_aggregator(buckets: Vec<Bucket>) -> Self {
        Self {
            buckets,
            _state: PhantomData,
        }
    }
}

impl<S> IntoIterator for Buckets<S> {
    type Item = Bucket;
    type IntoIter = std::vec::IntoIter<Bucket>;

    fn into_iter(self) -> Self::IntoIter {
        self.buckets.into_iter()
    }
}

// State in a private module, to make sure no one outside the module has access to it.
mod state {
    pub struct PreProject;
    pub struct PostProject;
}

/// Metric buckets which can be aggregated before the project state is available.
pub type PreProject = Buckets<state::PreProject>;
/// Metric buckets which have been filtered and processed with the project state.
pub type PostProject = Buckets<state::PostProject>;

#[cfg(test)]
mod tests {
    use super::*;

    impl<S> Buckets<S> {
        /// Constructor for tests which bypasses the state requirements.
        pub fn test(buckets: Vec<Bucket>) -> Self {
            Self {
                buckets,
                _state: PhantomData,
            }
        }
    }
}
