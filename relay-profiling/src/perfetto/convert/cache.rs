use hashbrown::HashMap;
use hashbrown::hash_map::Entry;

use crate::perfetto::convert::budget::{
    BudgetExceeded, BudgetMap, BudgetSize, LocalBudget, MemoryBudget,
};
use crate::perfetto::convert::utils::{self, SequenceId};
use crate::perfetto::convert::{consts, intern};
use crate::perfetto::proto;
use crate::sample::v2::{FrameId, StackId};

#[derive(Debug)]
pub struct Database {
    inner: HashMap<SequenceId, Caches>,
    budget: MemoryBudget,
}

impl Database {
    pub fn new(budget: &MemoryBudget) -> Self {
        Self {
            inner: Default::default(),
            budget: budget.clone(),
        }
    }

    pub fn for_packet(
        &mut self,
        packet: &proto::TracePacket,
    ) -> Result<&mut Caches, BudgetExceeded> {
        let seq_id = SequenceId::new(packet);

        // Technically this is off by-one, but significantly easier to handle.
        if self.inner.len() >= consts::MAX_SEQUENCE_IDS {
            return Err(BudgetExceeded);
        }

        if utils::has_incremental_state_cleared(packet) {
            self.inner.remove(&seq_id);
        }

        Ok(self.inner.entry(seq_id).or_insert_with(|| Caches {
            mapping_paths: MappingPathCache {
                inner: Default::default(),
                budget: self.budget.local(),
            },
            frames: self.budget.map(),
            callstacks: self.budget.map(),
        }))
    }
}

/// List of caches used to cache intermediate artifacts for a single sequence id.
#[derive(Debug)]
pub struct Caches {
    /// Computed mapping paths.
    pub mapping_paths: MappingPathCache,
    /// Computed frames.
    pub frames: BudgetMap<u64, FrameId>,
    /// Computed call-stacks.
    pub callstacks: BudgetMap<u64, StackId>,
}

#[derive(Debug)]
pub struct MappingPathCache {
    inner: HashMap<u64, Option<String>>,
    budget: LocalBudget,
}

impl MappingPathCache {
    /// Resolves a mapping path from the cache or computes it if necessary.
    pub fn resolve(
        &mut self,
        id: Option<u64>,
        mapping: Option<&proto::Mapping>,
        tables: &intern::Tables,
    ) -> Result<Option<&str>, BudgetExceeded> {
        let Some(id) = id else { return Ok(None) };

        Ok(match self.inner.entry(id) {
            Entry::Occupied(e) => e.into_mut().as_deref(),
            Entry::Vacant(e) => {
                let mut path = String::new();
                self.budget.try_add(path.size())?;

                let parts = mapping
                    .map(|m| &m.path_string_ids)
                    .filter(|p| p.len() <= consts::MAX_PATH_SEGMENTS)
                    .into_iter()
                    .flatten()
                    .filter_map(|id| tables.resolve_mapping_path(*id));
                for part in itertools::Itertools::intersperse(parts, "/") {
                    self.budget.try_add(part.len())?;
                    path.push_str(part);
                }

                let path = match path.is_empty() {
                    true => None,
                    false => Some(path),
                };

                self.budget.try_add(id.size())?;
                e.insert(path).as_deref()
            }
        })
    }
}
