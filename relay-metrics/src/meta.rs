use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
};

use chrono::{DateTime, Utc};
use relay_base_schema::project::ProjectKey;
use relay_common::time::UnixTimestamp;
use serde::{Deserialize, Serialize};

use crate::{statsd::MetricCounters, MetricResourceIdentifier};

/// A metric metadata item.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MetricMeta {
    /// Timestamp scope for the contained metadata.
    ///
    /// Metric metadata is collected in daily intervals, so this may be truncated
    /// to the start of the day (UTC) already.
    pub timestamp: StartOfDayUnixTimestamp,

    /// The contained metadata mapped by MRI.
    pub mapping: HashMap<MetricResourceIdentifier<'static>, Vec<MetricMetaItem>>,
}

/// A metadata item.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum MetricMetaItem {
    /// A location metadata pointing to the code location where the metric originates from.
    Location(Location),
    /// Unknown item.
    #[serde(other)]
    Unknown,
}

/// A code location.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Deserialize, Serialize)]
pub struct Location {
    /// The relative file path.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filename: Option<String>,
    /// The absolute file path.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub abs_path: Option<String>,
    /// The containing module name or path.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub module: Option<String>,
    /// The containing function name.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub function: Option<String>,
    /// The line number.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lineno: Option<u64>, // TODO nachschauen
}

/// A Unix timestamp that is truncated to the start of the day.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StartOfDayUnixTimestamp(UnixTimestamp);

impl StartOfDayUnixTimestamp {
    /// Creates a new `StartOfDayUnixTimestamp` from a timestamp by truncating it.
    ///
    /// May return none when passed an invalid date, but in practice this never fails
    /// since the [`UnixTimestamp`] is already sufficiently validated.
    pub fn new(ts: UnixTimestamp) -> Option<Self> {
        let dt: DateTime<Utc> = DateTime::from_timestamp(ts.as_secs().try_into().ok()?, 0)?;
        let beginning_of_day = dt.date_naive().and_hms_opt(0, 0, 0)?.and_utc();
        Some(Self(UnixTimestamp::from_datetime(beginning_of_day)?))
    }

    /// Returns the underlying unix timestamp, truncated to the start of the day.
    pub fn as_timestamp(&self) -> UnixTimestamp {
        self.0
    }
}

impl std::ops::Deref for StartOfDayUnixTimestamp {
    type Target = UnixTimestamp;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Serialize for StartOfDayUnixTimestamp {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for StartOfDayUnixTimestamp {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let ts = UnixTimestamp::deserialize(deserializer)?;

        StartOfDayUnixTimestamp::new(ts)
            .ok_or_else(|| serde::de::Error::custom("invalid timestamp"))
    }
}

/// A metrics meta aggregator.
///
/// Aggregates metric metadata based on their scope (project, mri, timestamp) and
/// only keeps the most relevant entries.
///
/// Currently we track the first N amount of unique metric meta elements we get.
///
/// This should represent the actual adoption rate of different code versions.
///
/// This aggregator is purely in memeory and will lose its state on restart,
/// which may cause multiple different items being emitted after restarts.
/// For this we have de-deuplication in the storage and the volume overall
/// of this happening is small enough to just add it to the storage worst case.
#[derive(Debug)]
pub struct MetaAggregator {
    ///
    locations: hashbrown::HashMap<Scope, HashSet<Location>>,

    /// Maximum tracked locations.
    max_locations: usize,
}

impl MetaAggregator {
    /// Creates a new metrics meta aggregator.
    pub fn new(max_locations: usize) -> Self {
        Self {
            locations: hashbrown::HashMap::new(),
            max_locations,
        }
    }

    /// Adds a new meta item to the aggregator.
    ///
    /// Returns a new [`MetricMeta`] element when the element should be stored
    /// or sent upstream for storage.
    ///
    /// Returns `None` when the meta item was already seen or is not considered relevant.
    pub fn add(&mut self, project_key: ProjectKey, meta: MetricMeta) -> Option<MetricMeta> {
        let mut send_upstream = HashMap::new();

        for (mri, items) in meta.mapping {
            let scope = Scope {
                timestamp: meta.timestamp,
                project_key,
                mri,
            };

            if let Some(items) = self.add_scoped(&scope, items) {
                send_upstream.insert(scope.mri, items);
            }
        }

        if send_upstream.is_empty() {
            return None;
        }

        relay_statsd::metric!(counter(MetricCounters::MetaLocationUpdate) += 1);
        Some(MetricMeta {
            timestamp: meta.timestamp,
            mapping: send_upstream,
        })
    }

    /// Retrieves all currently relevant metric meta for a project.
    pub fn get_relevant(&self, project_key: ProjectKey) -> impl Iterator<Item = MetricMeta> {
        let locations = self
            .locations
            .iter()
            .filter(|(scope, _)| scope.project_key == project_key);

        let mut result = HashMap::new();

        for (scope, locations) in locations {
            result
                .entry(scope.timestamp)
                .or_insert_with(|| MetricMeta {
                    timestamp: scope.timestamp,
                    mapping: HashMap::new(),
                })
                .mapping
                .entry(scope.mri.clone()) // This clone sucks
                .or_insert_with(Vec::new)
                .extend(locations.iter().cloned().map(MetricMetaItem::Location));
        }

        result.into_values()
    }

    /// Remove all contained state related to a project.
    pub fn clear(&mut self, project_key: ProjectKey) {
        self.locations
            .retain(|scope, _| scope.project_key != project_key);
    }

    fn add_scoped(
        &mut self,
        scope: &Scope,
        items: Vec<MetricMetaItem>,
    ) -> Option<Vec<MetricMetaItem>> {
        // Entry ref needs hashbrown, we would have to clone the scope without or do a separate lookup.
        let locations = self.locations.entry_ref(scope).or_default();
        let mut send_upstream = Vec::new();

        for item in items {
            match item {
                MetricMetaItem::Location(location) => {
                    if locations.len() > self.max_locations {
                        break;
                    }

                    if !locations.contains(&location) {
                        locations.insert(location.clone());
                        send_upstream.push(MetricMetaItem::Location(location));
                    }
                }
                MetricMetaItem::Unknown => {}
            }
        }

        (!send_upstream.is_empty()).then_some(send_upstream)
    }
}

/// The metadata scope.
///
/// We scope metadata by project, mri and day,
/// represented as a unix timestamp at the beginning of the day.
///
/// The technical scope (e.g. redis key) also includes the organization id, but this
/// can be inferred from the project.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Scope {
    pub timestamp: StartOfDayUnixTimestamp,
    pub project_key: ProjectKey,
    pub mri: MetricResourceIdentifier<'static>,
}

impl From<&Scope> for Scope {
    fn from(value: &Scope) -> Self {
        value.clone()
    }
}
