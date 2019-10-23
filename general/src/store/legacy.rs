use std::mem;

use debugid::DebugId;

use crate::processor::{ProcessingState, Processor};
use crate::protocol::{DebugImage, NativeDebugImage};
use crate::types::{Annotated, Meta, Object, ValueAction};

/// Converts legacy data structures to current format.
pub struct LegacyProcessor;

impl Processor for LegacyProcessor {
    fn process_debug_image(
        &mut self,
        image: &mut DebugImage,
        _meta: &mut Meta,
        _state: &ProcessingState<'_>,
    ) -> ValueAction {
        if let DebugImage::Apple(ref mut apple) = image {
            let native = NativeDebugImage {
                code_id: Annotated::empty(),
                code_file: mem::replace(&mut apple.name, Annotated::empty()),
                debug_id: mem::replace(&mut apple.uuid, Annotated::empty())
                    .map_value(DebugId::from),
                debug_file: Annotated::empty(),
                arch: mem::replace(&mut apple.arch, Annotated::empty()),
                image_addr: mem::replace(&mut apple.image_addr, Annotated::empty()),
                image_size: mem::replace(&mut apple.image_size, Annotated::empty()),
                image_vmaddr: mem::replace(&mut apple.image_vmaddr, Annotated::empty()),
                other: mem::replace(&mut apple.other, Object::new()),
            };

            *image = DebugImage::MachO(Box::new(native));
        }

        Ok(())
    }
}
