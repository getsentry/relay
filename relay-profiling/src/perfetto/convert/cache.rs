use hashbrown::HashMap;

use crate::perfetto::convert::utils::{self, SequenceId};
use crate::perfetto::convert::{consts, intern};
use crate::perfetto::proto;
use crate::sample::v2::{FrameId, StackId};

#[derive(Debug, Default)]
pub struct Database {
    cache: HashMap<SequenceId, Caches>,
}

impl Database {
    pub fn for_packet(&mut self, packet: &proto::TracePacket) -> &mut Caches {
        let seq_id = SequenceId::new(packet);

        let cache = self.cache.entry(seq_id).or_default();
        if utils::has_incremental_state_cleared(packet) {
            *cache = Default::default();
        }
        cache
    }
}

/// List of caches used to cache intermediate artifacts for a single sequence id.
#[derive(Debug, Default)]
pub struct Caches {
    /// Computed mapping paths.
    pub mapping_paths: MappingPathCache,
    /// Computed frames.
    pub frames: HashMap<u64, FrameId>,
    /// Computed call-stacks.
    pub callstacks: HashMap<u64, StackId>,
}

#[derive(Debug, Default)]
pub struct MappingPathCache(HashMap<u64, Option<String>>);

impl MappingPathCache {
    /// Resolves a mapping path from the cache or computes it if necessary.
    pub fn resolve(
        &mut self,
        id: Option<u64>,
        mapping: Option<&proto::Mapping>,
        tables: &intern::Tables,
    ) -> Option<&str> {
        let id = id?;

        self.0
            .entry(id)
            .or_insert_with(|| {
                let mapping =
                    mapping.filter(|m| m.path_string_ids.len() <= consts::MAX_PATH_SEGMENTS)?;

                let path = mapping
                    .path_string_ids
                    .iter()
                    .filter_map(|id| tables.resolve_mapping_path(*id));
                let path: String = itertools::Itertools::intersperse(path, "/").collect();

                match path.is_empty() {
                    true => None,
                    false => Some(path),
                }
            })
            .as_deref()
    }
}
