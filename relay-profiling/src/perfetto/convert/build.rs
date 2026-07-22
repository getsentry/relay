use std::hash::BuildHasher as _;

use hashbrown::hash_table::Entry;
use hashbrown::{DefaultHashBuilder, HashTable};
use relay_event_schema::protocol::Addr;

use crate::ProfileError;
use crate::debug_image::{DebugImage, ImageType};
use crate::perfetto::convert::budget::{BudgetExceeded, BudgetVec, MemoryBudget};
use crate::perfetto::convert::{consts, intern, utils};
use crate::perfetto::proto;
use crate::sample::Frame;
use crate::sample::v2::{FrameId, StackId};

#[derive(Debug)]
pub struct Images {
    images: BudgetVec<DebugImage>,
    image_index: HashTable<usize>,
    hasher: DefaultHashBuilder,
}

impl Images {
    pub fn new(budget: &MemoryBudget) -> Self {
        Self {
            images: budget.vec(),
            image_index: Default::default(),
            hasher: Default::default(),
        }
    }

    pub fn add(
        &mut self,
        mapping: &proto::Mapping,
        code_file: &str,
        tables: &intern::Tables,
    ) -> Result<(), BudgetExceeded> {
        if utils::is_java_mapping(code_file) {
            return Ok(());
        }

        let image_addr = mapping.start.map(Addr);

        // The closure is here to make sure the hash is always calculated from the same inputs.
        let do_hash = |code_file: Option<&str>, image: Option<Addr>| {
            debug_assert!(code_file.is_some());
            self.hasher.hash_one((code_file, image))
        };

        let hash = do_hash(Some(code_file), image_addr);
        let already_exists = self
            .image_index
            .find(hash, |idx| {
                self.images.get(*idx).is_some_and(|image| {
                    let cf = image.code_file.as_ref().map(|cf| cf.as_str());
                    cf.is_some_and(|cf| cf == code_file) && image.image_addr == image_addr
                })
            })
            .is_some();
        if already_exists {
            return Ok(());
        }

        let Some(debug_id) = tables
            .resolve_build_id(mapping.build_id)
            .and_then(utils::build_id_to_debug_id)
        else {
            return Ok(());
        };

        let image_size = mapping
            .end
            .unwrap_or(0)
            .saturating_sub(mapping.start.unwrap_or(0));
        let image_vmaddr = mapping.load_bias.map(Addr);

        let idx = self.images.len();
        self.images.push(DebugImage {
            code_file: Some(code_file.to_owned().into()),
            debug_id: Some(debug_id),
            image_type: ImageType::Symbolic,
            image_addr,
            image_vmaddr,
            image_size,
            uuid: None,
        })?;
        self.image_index.insert_unique(hash, idx, |x| {
            let image = self.images.get(*x);
            let code_file = image
                .and_then(|d| d.code_file.as_ref())
                .map(|cf| cf.as_str());
            let image_addr = image.and_then(|d| d.image_addr);
            do_hash(code_file, image_addr)
        });
        Ok(())
    }

    pub fn into_images(self) -> Vec<DebugImage> {
        self.images.into_inner()
    }
}

#[derive(Debug)]
pub struct Frames {
    frames: BudgetVec<Frame>,
    frame_index: HashTable<usize>,
    hasher: DefaultHashBuilder,
}

impl Frames {
    pub fn new(budget: &MemoryBudget) -> Self {
        Self {
            frames: budget.vec(),
            frame_index: Default::default(),
            hasher: Default::default(),
        }
    }

    pub fn add(
        &mut self,
        function_name: Option<&str>,
        mapping_path: Option<&str>,
        pf: &proto::Frame,
        mapping: Option<&proto::Mapping>,
    ) -> Result<FrameId, ProfileError> {
        let instruction_addr = {
            let start = mapping.and_then(|m| m.start).unwrap_or(0);
            pf.rel_pc.and_then(|rel_pc| rel_pc.checked_add(start))
        };
        let fr = FrameRef::new(function_name, mapping_path, instruction_addr);

        // Make sure the hash is always calculated from the same inputs.
        let do_hash = |fr: Option<FrameRef<'_>>| {
            debug_assert!(fr.is_some());
            self.hasher.hash_one(fr)
        };

        let entry = self.frame_index.entry(
            do_hash(Some(fr)),
            |&i| Some(fr) == self.frames.get(i).map(FrameRef::from_frame),
            |&i| do_hash(self.frames.get(i).map(FrameRef::from_frame)),
        );

        let index = match entry {
            Entry::Occupied(e) => *e.get(),
            Entry::Vacant(e) => {
                let next_idx = self.frames.len();
                self.frames.push(fr.to_owned())?;
                *e.insert(next_idx).get()
            }
        };

        if index >= consts::MAX_UNIQUE_FRAMES {
            return Err(ProfileError::ExceedSizeLimit);
        }

        Ok(FrameId(index))
    }

    pub fn into_frames(self) -> Vec<Frame> {
        self.frames.into_inner()
    }
}

#[derive(Debug)]
pub struct Stacks {
    stacks: BudgetVec<Vec<FrameId>>,
    stack_index: HashTable<usize>,
    hasher: DefaultHashBuilder,
}

impl Stacks {
    pub fn new(budget: &MemoryBudget) -> Self {
        Self {
            stacks: budget.vec(),
            stack_index: Default::default(),
            hasher: Default::default(),
        }
    }

    pub fn add(&mut self, frames: Vec<FrameId>) -> Result<StackId, BudgetExceeded> {
        let do_hash = |frames: &[FrameId]| self.hasher.hash_one(frames);

        let entry = self.stack_index.entry(
            do_hash(&frames),
            |&i| Some(&frames) == self.stacks.get(i),
            |&i| do_hash(self.stacks.get(i).map(Vec::as_slice).unwrap_or(&[])),
        );

        let index = match entry {
            Entry::Occupied(e) => *e.get(),
            Entry::Vacant(e) => {
                let next_idx = self.stacks.len();
                self.stacks.push(frames)?;
                *e.insert(next_idx).get()
            }
        };

        Ok(StackId(index))
    }

    pub fn into_stacks(self) -> Vec<Vec<FrameId>> {
        self.stacks.into_inner()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct FrameRef<'a> {
    function: Option<&'a str>,
    module: Option<&'a str>,
    package: Option<&'a str>,
    instruction_addr: Option<u64>,
}

impl<'a> FrameRef<'a> {
    fn new(
        function: Option<&'a str>,
        package: Option<&'a str>,
        mut instruction_addr: Option<u64>,
    ) -> Self {
        let is_java = package.is_some_and(utils::is_java_mapping);
        let (module, function) = match is_java {
            false => (None, function),
            true => match function {
                // For Java frames, split "com.example.MyClass.myMethod" into
                // module="com.example.MyClass" and function="myMethod".
                Some(name) => match name.rsplit_once('.') {
                    Some((class, method)) => (Some(class), Some(method)),
                    None => (None, Some(name)),
                },
                None => (None, None),
            },
        };

        if is_java {
            instruction_addr = None;
        }

        Self {
            function,
            module,
            package,
            instruction_addr,
        }
    }

    fn from_frame(frame: &'a Frame) -> Self {
        Self {
            function: frame.function.as_deref(),
            module: frame.module.as_deref(),
            package: frame.package.as_deref(),
            instruction_addr: frame.instruction_addr.map(|addr| addr.0),
        }
    }

    fn to_owned(self) -> Frame {
        let is_java = self.package.is_some_and(utils::is_java_mapping);
        Frame {
            function: self.function.map(str::to_owned),
            module: self.module.map(str::to_owned),
            package: self.package.map(str::to_owned),
            instruction_addr: self.instruction_addr.map(Addr),
            platform: Some(if is_java { "java" } else { "native" }.to_owned()),
            ..Default::default()
        }
    }
}
