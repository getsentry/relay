use hashbrown::HashMap;

use crate::perfetto::convert::utils::{self, SequenceId};
use crate::perfetto::proto;

#[derive(Debug, Default)]
pub struct Database {
    inner: HashMap<SequenceId, Tables>,
}

impl Database {
    /// Removes and merges intern information contained in the `packet` into the database.
    pub fn intern(&mut self, packet: &mut proto::TracePacket) -> &Tables {
        let data = packet.interned_data.take();
        let seq_id = SequenceId::new(packet);

        let tables = self.inner.entry(seq_id).or_default();
        if utils::has_incremental_state_cleared(packet) {
            *tables = Default::default();
        }
        if let Some(data) = data {
            tables.merge(data);
        };
        tables
    }
}

#[derive(Debug, Default)]
pub struct Tables {
    function_names: HashMap<u64, String>,
    mapping_paths: HashMap<u64, String>,
    build_ids: HashMap<u64, Vec<u8>>,
    frames: HashMap<u64, proto::Frame>,
    callstacks: HashMap<u64, proto::Callstack>,
    mappings: HashMap<u64, proto::Mapping>,
}

impl Tables {
    pub fn resolve_callstack(&self, id: u64) -> Option<&proto::Callstack> {
        self.callstacks.get(&id)
    }

    pub fn resolve_frame(&self, id: u64) -> Option<&proto::Frame> {
        self.frames.get(&id)
    }

    pub fn resolve_function_name(&self, id: Option<u64>) -> Option<&str> {
        id.and_then(|id| self.function_names.get(&id))
            .map(String::as_str)
    }

    pub fn resolve_mapping(&self, id: Option<u64>) -> Option<&proto::Mapping> {
        id.and_then(|id| self.mappings.get(&id))
    }

    pub fn resolve_mapping_path(&self, id: u64) -> Option<&str> {
        self.mapping_paths.get(&id).map(String::as_str)
    }

    pub fn resolve_build_id(&self, id: Option<u64>) -> Option<&[u8]> {
        id.and_then(|id| self.build_ids.get(&id)).map(Vec::as_slice)
    }

    fn merge(&mut self, data: proto::InternedData) {
        let proto::InternedData {
            function_names,
            mapping_paths,
            build_ids,
            frames,
            callstacks,
            mappings,
        } = data;

        self.function_names.extend(interned_strings(function_names));
        self.mapping_paths.extend(interned_strings(mapping_paths));
        self.build_ids.extend(
            build_ids
                .into_iter()
                .filter_map(|is| Some((is.iid?, is.r#str?))),
        );
        self.frames
            .extend(frames.into_iter().filter_map(|f| Some((f.iid?, f))));
        self.callstacks
            .extend(callstacks.into_iter().filter_map(|c| Some((c.iid?, c))));
        self.mappings
            .extend(mappings.into_iter().filter_map(|m| Some((m.iid?, m))));
    }
}

fn interned_strings(strings: Vec<proto::InternedString>) -> impl Iterator<Item = (u64, String)> {
    strings
        .into_iter()
        .filter_map(|is| Some((is.iid?, String::from_utf8(is.r#str?).ok()?)))
}
