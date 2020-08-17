//! Minidump handling for Relay

use std::convert::TryFrom;
use std::ops::Deref;

use bytes::{Bytes, BytesMut};
use failure::Fail;
use minidump::format::{MINIDUMP_LOCATION_DESCRIPTOR, RVA};
use minidump::{Error as MinidumpError, Minidump, MinidumpMemoryList, MinidumpThreadList};

use relay_general::pii::{AttachmentBytesType, PiiAttachmentsProcessor};

#[derive(Debug, Fail)]
pub enum Error {
    #[fail(display = "invalid minidump attachment: {}", _0)]
    InvalidMinidump(#[cause] MinidumpError),
    #[fail(display = "failed parsing {}", what)]
    Parse {
        what: String,
        #[cause]
        cause: MinidumpError,
    },
}

pub fn scrub_minidump<'a, T>(
    filename: impl AsRef<str>,
    minidump: Minidump<'a, T>,
    minidump_data: Bytes,
    processor: PiiAttachmentsProcessor,
) -> Result<Bytes, Error>
where
    T: Deref<Target = [u8]> + 'a,
{
    let offsets = MinidumpOffsets::try_from(minidump)?;
    let mut raw = BytesMut::from(minidump_data);

    for desc in offsets.mem_stack.iter() {
        processor.scrub_attachment_bytes(
            filename.as_ref(),
            raw.desc_mut_slice(desc),
            AttachmentBytesType::MinidumpStack,
        );
    }
    for desc in offsets.mem_nonstack.iter() {
        processor.scrub_attachment_bytes(
            filename.as_ref(),
            raw.desc_mut_slice(desc),
            AttachmentBytesType::MinidumpHeap,
        );
    }
    Ok(raw.freeze())
}

/// Locations of interesting byte-ranges in the raw minidump data.
///
/// To mutate a minidump we need offsets into the data.  We can get those from the minidump
/// crate but this involves using various structs which have immutable references to the
/// minidump data.  As we can not have mutable and immutable references to the minidump data
/// at the same time we collect all the offsets up-front in this struct.
struct MinidumpOffsets {
    mem_stack: Vec<MINIDUMP_LOCATION_DESCRIPTOR>,
    mem_nonstack: Vec<MINIDUMP_LOCATION_DESCRIPTOR>,
}

impl<'a, T> TryFrom<Minidump<'a, T>> for MinidumpOffsets
where
    T: Deref<Target = [u8]> + 'a,
{
    type Error = Error;

    fn try_from(minidump: Minidump<'a, T>) -> Result<Self, Self::Error> {
        let thread_list: MinidumpThreadList = minidump.get_stream().map_err(|e| Error::Parse {
            what: "ThreadList".to_string(),
            cause: e,
        })?;
        let stack_rvas: Vec<RVA> = thread_list
            .threads
            .iter()
            .map(|t| t.raw.stack.memory.rva)
            .collect();
        let mem_list: MinidumpMemoryList = minidump.get_stream().map_err(|e| Error::Parse {
            what: "MemoryList".to_string(),
            cause: e,
        })?;
        let mut mem_stack = Vec::new();
        let mut mem_nonstack = Vec::new();
        for mem in mem_list.iter() {
            if stack_rvas.contains(&mem.desc.memory.rva) {
                mem_stack.push(mem.desc.memory);
            } else {
                mem_nonstack.push(mem.desc.memory);
            }
        }
        Ok(Self {
            mem_stack,
            mem_nonstack,
        })
    }
}

/// Easy slices for our raw data from minidump descriptors.
trait MinidumpSlicer {
    fn desc_slice(&self, desc: &MINIDUMP_LOCATION_DESCRIPTOR) -> &[u8];

    fn desc_mut_slice(&mut self, desc: &MINIDUMP_LOCATION_DESCRIPTOR) -> &mut [u8];
}

impl MinidumpSlicer for BytesMut {
    fn desc_slice(&self, desc: &MINIDUMP_LOCATION_DESCRIPTOR) -> &[u8] {
        let start = desc.rva as usize;
        let end = (desc.rva + desc.data_size) as usize;
        &self[start..end]
    }

    fn desc_mut_slice(&mut self, desc: &MINIDUMP_LOCATION_DESCRIPTOR) -> &mut [u8] {
        let start = desc.rva as usize;
        let end = (desc.rva + desc.data_size) as usize;
        &mut self[start..end]
    }
}
