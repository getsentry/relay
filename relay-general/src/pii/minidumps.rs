//! Minidump scrubbing.

use std::convert::TryFrom;

use bytes::{Bytes, BytesMut};
use failure::Fail;
use minidump::format::{MINIDUMP_LOCATION_DESCRIPTOR, MINIDUMP_STREAM_TYPE, RVA};
use minidump::{Error as MinidumpError, Minidump, MinidumpMemoryList, MinidumpThreadList};

use crate::pii::{AttachmentBytesType, PiiAttachmentsProcessor};

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

pub fn scrub_minidump(
    filename: impl AsRef<str>,
    raw: Bytes,
    processor: PiiAttachmentsProcessor,
) -> Result<Bytes, Error> {
    let md = MinidumpData::try_from(raw.clone())?;
    let mut raw = BytesMut::from(raw);

    for mem in md.memory_descriptors()? {
        match mem {
            MemoryDescriptor::Stack(ref desc) => {
                processor.scrub_attachment_bytes(
                    filename.as_ref(),
                    raw.desc_mut_slice(desc),
                    AttachmentBytesType::MinidumpStack,
                );
            }
            MemoryDescriptor::NonStack(ref desc) => {
                processor.scrub_attachment_bytes(
                    filename.as_ref(),
                    raw.desc_mut_slice(desc),
                    AttachmentBytesType::MinidumpHeap,
                );
            }
        }
    }
    for desc in md.raw_descriptors()? {
        processor.scrub_attachment_bytes(
            filename.as_ref(),
            raw.desc_mut_slice(&desc),
            AttachmentBytesType::PlainAttachment,
        );
    }

    Ok(raw.freeze())
}

/// Internal struct to keep a minidump and it's raw data together.
struct MinidumpData {
    raw: Bytes,
    minidump: Minidump<'static, Bytes>,
}

impl TryFrom<Bytes> for MinidumpData {
    type Error = Error;

    fn try_from(raw: Bytes) -> Result<Self, Self::Error> {
        let minidump = Minidump::read(raw.clone()).map_err(Error::InvalidMinidump)?;
        Ok(Self { raw, minidump })
    }
}

enum MemoryDescriptor {
    Stack(MINIDUMP_LOCATION_DESCRIPTOR),
    NonStack(MINIDUMP_LOCATION_DESCRIPTOR),
}

impl MinidumpData {
    fn memory_descriptors(&self) -> Result<Vec<MemoryDescriptor>, Error> {
        let thread_list: MinidumpThreadList =
            self.minidump.get_stream().map_err(|e| Error::Parse {
                what: "ThreadList".to_string(),
                cause: e,
            })?;
        let stack_rvas: Vec<RVA> = thread_list
            .threads
            .iter()
            .map(|t| t.raw.stack.memory.rva)
            .collect();
        let mem_list: MinidumpMemoryList =
            self.minidump.get_stream().map_err(|e| Error::Parse {
                what: "MemoryList".to_string(),
                cause: e,
            })?;
        let mut ret = Vec::new();
        for mem in mem_list.iter() {
            if stack_rvas.contains(&mem.desc.memory.rva) {
                ret.push(MemoryDescriptor::Stack(mem.desc.memory));
            } else {
                ret.push(MemoryDescriptor::NonStack(mem.desc.memory));
            }
        }
        Ok(ret)
    }

    fn raw_descriptors(&self) -> Result<Vec<MINIDUMP_LOCATION_DESCRIPTOR>, Error> {
        let mut ret = Vec::new();
        let raw_stream_types = [
            MINIDUMP_STREAM_TYPE::LinuxEnviron,
            MINIDUMP_STREAM_TYPE::LinuxCmdLine,
        ];
        for stream_type in &raw_stream_types {
            match self.minidump.get_raw_stream(*stream_type) {
                Ok(stream) => {
                    let stream_p = stream.as_ptr() as usize;
                    let data_p = self.raw.as_ptr() as usize;
                    let offset = stream_p - data_p;
                    ret.push(MINIDUMP_LOCATION_DESCRIPTOR {
                        rva: offset as u32,
                        data_size: stream.len() as u32,
                    });
                }
                Err(minidump::Error::StreamNotFound) => (),
                Err(e) => {
                    return Err(Error::Parse {
                        what: "RawStream".to_string(),
                        cause: e,
                    })
                }
            }
        }
        Ok(ret)
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
