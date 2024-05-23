use core::fmt;
use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::usize;

use relay_base_schema::project::ProjectKey;
use relay_metrics::Bucket;
use relay_quotas::Scoping;

use crate::metrics::{ExtractionMode, MetricOutcomes};
use crate::services::outcome::{DiscardReason, Outcome};
use crate::utils::{self, ItemAction};

#[derive(Debug, Clone, Copy)]
enum ProjectData {
    Partial {
        project_key: ProjectKey,
    },
    Full {
        scoping: Scoping,
        mode: ExtractionMode,
    },
}

pub struct ManagedBuckets<S> {
    buckets: Vec<Bucket>,
    outcomes: MetricOutcomes,
    project: ProjectData,
    _state: PhantomData<S>,
}

impl ManagedBuckets<()> {
    pub fn new(outcomes: MetricOutcomes, project_key: ProjectKey, buckets: Vec<Bucket>) -> Self {
        ManagedBuckets {
            buckets,
            outcomes,
            project: ProjectData::Partial { project_key },
            _state: PhantomData,
        }
    }
}

impl<S> ManagedBuckets<S> {
    pub fn with_state(
        outcomes: MetricOutcomes,
        project_key: ProjectKey,
        buckets: Vec<Bucket>,
    ) -> Self {
        ManagedBuckets {
            buckets,
            outcomes,
            project: ProjectData::Partial { project_key },
            _state: PhantomData,
        }
    }

    pub fn with_state_and_scoping(
        outcomes: MetricOutcomes,
        scoping: Scoping,
        mode: ExtractionMode,
        buckets: Vec<Bucket>,
    ) -> Self {
        ManagedBuckets {
            buckets,
            outcomes,
            project: ProjectData::Full { scoping, mode },
            _state: PhantomData,
        }
    }

    pub fn scope(&mut self, scoping: Scoping, mode: ExtractionMode) {
        self.project = ProjectData::Full { scoping, mode };
    }

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

    pub fn project_key(&self) -> ProjectKey {
        match &self.project {
            ProjectData::Partial { project_key } => *project_key,
            ProjectData::Full { scoping, .. } => scoping.project_key,
        }
    }

    pub fn mutate<T>(mut self, mut f: impl FnMut(&mut Bucket)) -> ManagedBuckets<T>
    where
        T: BucketState<Previous = S>,
    {
        for bucket in &mut self.buckets {
            f(bucket);
        }

        self.migrate()
    }

    pub fn retain_mut<T>(
        mut self,
        mut f: impl FnMut(&mut Bucket) -> ItemAction,
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

        self.migrate()
    }

    pub fn remove_if<T>(
        mut self,
        outcome: Outcome,
        f: impl FnMut(&Bucket) -> bool,
    ) -> ManagedBuckets<T> {
        let (buckets, rejected) = utils::split_off(std::mem::take(&mut self.buckets), f);
        self.buckets = buckets;

        self.track(rejected, outcome);

        self.migrate()
    }

    pub fn reject(mut self, outcome: Outcome) {
        let buckets = std::mem::take(&mut self.buckets);
        self.track(buckets, outcome);
    }

    pub fn transition<T>(self) -> ManagedBuckets<T>
    where
        S: Into<T>,
    {
        self.migrate()
    }

    fn migrate<T>(mut self) -> ManagedBuckets<T> {
        // unsafe { std::mem::transmute(self) }
        ManagedBuckets {
            buckets: std::mem::take(&mut self.buckets),
            outcomes: self.outcomes.clone(),
            project: self.project,
            _state: PhantomData,
        }
    }

    fn track(&self, buckets: Vec<Bucket>, outcome: Outcome) {
        let ProjectData::Full { scoping, mode } = self.project else {
            // TODO metric
            return;
        };

        self.outcomes.track(scoping, &buckets, mode, outcome)
    }
}

impl<S> Drop for ManagedBuckets<S> {
    fn drop(&mut self) {
        if self.buckets.is_empty() {
            return;
        }

        let buckets = std::mem::take(&mut self.buckets);
        self.track(buckets, Outcome::Invalid(DiscardReason::Internal));
    }
}

impl<S> fmt::Debug for ManagedBuckets<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let project = match self.project {
            ProjectData::Partial { .. } => "partial",
            ProjectData::Full { .. } => "full",
        };

        write!(
            f,
            "ManagedBuckets<{}>(buckets:{}, project:{})",
            std::any::type_name::<S>(),
            self.buckets.len(),
            project,
        )
    }
}

pub trait BucketState {
    type Previous;
}

pub struct Extracted;

impl From<Extracted> for Parsed {
    fn from(_: Extracted) -> Self {
        Self
    }
}

pub struct Parsed;

impl BucketState for Parsed {
    type Previous = ();
}

pub struct Filtered;

impl BucketState for Filtered {
    type Previous = Parsed;
}

pub struct WithProjectState;

impl BucketState for WithProjectState {
    type Previous = Filtered;
}

pub struct Aggregated;

impl From<WithProjectState> for Aggregated {
    fn from(_: WithProjectState) -> Self {
        Self
    }
}
