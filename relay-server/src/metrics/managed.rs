use core::fmt;
use std::any::Any;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::usize;

use relay_base_schema::project::ProjectKey;
use relay_metrics::Bucket;
use relay_quotas::Scoping;

use crate::metrics::{ExtractionMode, MetricOutcomes};
use crate::services::outcome::{DiscardReason, Outcome};
use crate::services::project::ProjectState;
use crate::utils::{self, ItemAction};

pub struct ManagedBuckets<S> {
    buckets: Vec<Bucket>,
    outcomes: MetricOutcomes,
    state: S,
}

impl ManagedBuckets<()> {
    pub fn new<S>(outcomes: MetricOutcomes, buckets: Vec<Bucket>, state: S) -> ManagedBuckets<S> {
        ManagedBuckets {
            buckets,
            outcomes,
            state,
        }
    }
}

impl<S> ManagedBuckets<S> {
    pub fn len(&self) -> usize {
        self.buckets.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buckets.is_empty()
    }

    pub fn into_buckets(mut self) -> Vec<Bucket> {
        std::mem::take(&mut self.buckets)
    }

    pub fn iter(&self) -> impl Iterator<Item = &Bucket> {
        self.buckets.iter()
    }

    pub fn project_key(&self) -> ProjectKey
    where
        S: BucketState,
    {
        self.state.project_key()
    }

    pub fn project_state(&self) -> &Arc<ProjectState>
    where
        S: ProjectBucketState,
    {
        self.state.project_state()
    }

    fn track(&self, buckets: Vec<Bucket>, outcome: Outcome) {
        let Some(project) = resolve_project(&self.state) else {
            // TODO metric
            return;
        };

        let mode = project.project_state.get_extraction_mode();
        self.outcomes
            .track(project.scoping, &buckets, mode, outcome)
    }
}

impl<S: BucketState> ManagedBuckets<S> {
    pub fn mutate<T>(
        mut self,
        mut f: impl FnMut(&mut Bucket),
        state_fn: impl FnOnce(S) -> T,
    ) -> ManagedBuckets<T>
    where
        T: BucketState<Previous = S>,
    {
        for bucket in &mut self.buckets {
            f(bucket);
        }

        self.migrate(state_fn)
    }

    pub fn retain_mut<T>(
        mut self,
        mut f: impl FnMut(&mut Bucket) -> ItemAction,
        state_fn: impl FnOnce(S) -> T,
    ) -> ManagedBuckets<T>
    where
        T: BucketState<Previous = S>,
    {
        let mut outcomes = BTreeMap::<Outcome, Vec<Bucket>>::new();

        self.buckets = std::mem::take(&mut self.buckets)
            .into_iter()
            .filter_map(|mut bucket| match f(&mut bucket) {
                ItemAction::Keep => Some(bucket),
                ItemAction::DropSilently => None,
                ItemAction::Drop(outcome) => {
                    outcomes.entry(outcome).or_default().push(bucket);
                    None
                }
            })
            .collect();

        for (outcome, buckets) in outcomes {
            self.track(buckets, outcome);
        }

        self.migrate(state_fn)
    }

    pub fn remove_if<T>(
        mut self,
        outcome: Outcome,
        f: impl FnMut(&Bucket) -> bool,
        state_fn: impl FnOnce(S) -> T,
    ) -> ManagedBuckets<T> {
        let (buckets, rejected) = utils::split_off(std::mem::take(&mut self.buckets), f);
        self.buckets = buckets;

        self.track(rejected, outcome);

        self.migrate(state_fn)
    }

    pub fn reject(mut self, outcome: Outcome) {
        let buckets = std::mem::take(&mut self.buckets);
        self.track(buckets, outcome);
    }

    pub fn transition<T>(self, state_fn: impl FnOnce(S) -> T) -> ManagedBuckets<T>
    where
        S: Into<T>,
    {
        self.migrate(state_fn)
    }

    fn migrate<T>(mut self, state_fn: impl FnOnce(S) -> T) -> ManagedBuckets<T> {
        // unsafe { std::mem::transmute(self) }
        ManagedBuckets {
            buckets: std::mem::take(&mut self.buckets),
            outcomes: self.outcomes.clone(),
            state: state_fn(self.state.clone()),
        }
    }
}

impl<S> Drop for ManagedBuckets<S> {
    fn drop(&mut self) {
        // if self.buckets.is_empty() {
        //     return;
        // }
        //
        // let buckets = std::mem::take(&mut self.buckets);
        // self.track(buckets, Outcome::Invalid(DiscardReason::Internal));
    }
}

impl<S> fmt::Debug for ManagedBuckets<S>
where
    S: BucketState,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ManagedBuckets<{}>(buckets:{}, project:{})",
            std::any::type_name::<S>(),
            self.buckets.len(),
            self.state.project_key(),
        )
    }
}

pub trait BucketState: Clone {
    type Previous;

    fn project_key(&self) -> ProjectKey;
}

pub trait ProjectBucketState: BucketState {
    fn project_state(&self) -> &Arc<ProjectState>;
    fn scoping(&self) -> Scoping;
}

#[derive(Clone, Copy, Debug)]
pub struct Parsed {
    pub project_key: ProjectKey,
}

impl BucketState for Parsed {
    type Previous = ();

    fn project_key(&self) -> ProjectKey {
        self.project_key
    }
}

#[derive(Clone, Copy, Debug)]
pub struct ClockDriftCorrected {
    pub project_key: ProjectKey,
}

impl BucketState for ClockDriftCorrected {
    type Previous = Parsed;

    fn project_key(&self) -> ProjectKey {
        self.project_key
    }
}

impl From<Parsed> for ClockDriftCorrected {
    fn from(Parsed { project_key }: Parsed) -> Self {
        Self { project_key }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Filtered {
    pub project_key: ProjectKey,
}

impl BucketState for Filtered {
    type Previous = ClockDriftCorrected;

    fn project_key(&self) -> ProjectKey {
        self.project_key
    }
}

impl From<ClockDriftCorrected> for Filtered {
    fn from(ClockDriftCorrected { project_key }: ClockDriftCorrected) -> Self {
        Self { project_key }
    }
}

#[derive(Clone, Debug)]
pub struct WithProjectState {
    pub project_key: ProjectKey,
    pub project_state: Arc<ProjectState>,
    pub scoping: Scoping,
}

impl BucketState for WithProjectState {
    type Previous = Filtered;

    fn project_key(&self) -> ProjectKey {
        self.project_key
    }
}

impl ProjectBucketState for WithProjectState {
    fn scoping(&self) -> Scoping {
        self.scoping
    }

    fn project_state(&self) -> &Arc<ProjectState> {
        &self.project_state
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Aggregated {
    pub project_key: ProjectKey,
}

impl BucketState for Aggregated {
    type Previous = ();

    fn project_key(&self) -> ProjectKey {
        self.project_key
    }
}

impl From<WithProjectState> for Aggregated {
    fn from(_: WithProjectState) -> Self {
        Self
    }
}

struct ProjectData<'a> {
    project_key: ProjectKey,
    project_state: &'a Arc<ProjectState>,
    scoping: Scoping,
}

/// Specialization trick https://github.com/dtolnay/case-studies/blob/master/autoref-specialization/README.md
fn resolve_project<T>(state: &T) -> Option<ProjectData<'_>> {
    type Ret<'a> = Option<ProjectData<'a>>;

    trait ProjectViaProjectBucketState<'a> {
        fn project(&self) -> Ret<'a>;
    }

    trait ProjectViaDefault<'a> {
        fn project(&self) -> Ret<'a>;
    }

    struct ProjectVia<T>(T);

    impl<'a, T: ProjectBucketState> ProjectViaProjectBucketState<'a> for &ProjectVia<&'a T> {
        fn project(&self) -> Ret<'a> {
            Some(ProjectData {
                project_key: self.0.project_key(),
                project_state: self.0.project_state(),
                scoping: self.0.scoping(),
            })
        }
    }

    impl<'a, T> ProjectViaDefault<'a> for &&ProjectVia<&'a T> {
        fn project(&self) -> Ret<'a> {
            None
        }
    }

    (&&ProjectVia(state)).project()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolve_project_state() {
        // TODO
    }
}
