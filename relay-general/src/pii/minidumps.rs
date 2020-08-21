//! Minidump scrubbing.

use std::borrow::Cow;

use failure::Fail;
use minidump::format::{MINIDUMP_LOCATION_DESCRIPTOR, MINIDUMP_STREAM_TYPE, RVA};
use minidump::{Error as MinidumpError, Minidump, MinidumpMemoryList, MinidumpThreadList};

use crate::pii::PiiAttachmentsProcessor;
use crate::processor::{FieldAttrs, Pii, ValueType};

type Range = std::ops::Range<usize>;

#[derive(Debug, Fail)]
pub enum ScrubMinidumpError {
    #[fail(display = "failed to parse minidump")]
    InvalidMinidump(#[cause] MinidumpError),

    #[fail(display = "invalid memory address when reading {}", _0)]
    InvalidAddress(&'static str),

    #[fail(display = "failed to parse {}", _0)]
    Parse(&'static str, #[cause] MinidumpError),
}

#[derive(Clone, Debug)]
struct StreamDescriptor(ValueType, Range);

/// Internal struct to keep a minidump and it's raw data together.
struct MinidumpData<'a> {
    data: &'a [u8],
    minidump: Minidump<'a, &'a [u8]>,
}

impl<'a> MinidumpData<'a> {
    fn parse(data: &'a [u8]) -> Result<Self, ScrubMinidumpError> {
        let minidump = Minidump::read(data).map_err(ScrubMinidumpError::InvalidMinidump)?;
        Ok(Self { data, minidump })
    }

    fn offset(&self, slice: &[u8]) -> Option<usize> {
        let base = self.data.as_ptr() as usize;
        let pointer = slice.as_ptr() as usize;

        if pointer > base {
            Some(pointer - base)
        } else {
            None
        }
    }

    fn stream_range(&self, stream: &[u8]) -> Option<Range> {
        let start = self.offset(stream)?;
        let end = start + stream.len();
        Some(start..end)
    }

    fn location_range(&self, location: MINIDUMP_LOCATION_DESCRIPTOR) -> Range {
        let start = location.rva as usize;
        let end = (location.rva + location.data_size) as usize;
        start..end
    }

    fn streams(&self) -> Result<Vec<StreamDescriptor>, ScrubMinidumpError> {
        let thread_list: MinidumpThreadList = self
            .minidump
            .get_stream()
            .map_err(|e| ScrubMinidumpError::Parse("thread list", e))?;

        let stack_rvas: Vec<RVA> = thread_list
            .threads
            .iter()
            .map(|t| t.raw.stack.memory.rva)
            .collect();

        let mem_list: MinidumpMemoryList = self
            .minidump
            .get_stream()
            .map_err(|e| ScrubMinidumpError::Parse("memory list", e))?;

        let mut descriptors = Vec::new();
        for mem in mem_list.iter() {
            let value_type = if stack_rvas.contains(&mem.desc.memory.rva) {
                ValueType::StackMemory
            } else {
                ValueType::HeapMemory
            };

            let range = self.location_range(mem.desc.memory);
            descriptors.push(StreamDescriptor(value_type, range));
        }

        let aux_stream_types = [
            MINIDUMP_STREAM_TYPE::LinuxEnviron,
            MINIDUMP_STREAM_TYPE::LinuxCmdLine,
        ];

        for &stream_type in &aux_stream_types {
            match self.minidump.get_raw_stream(stream_type) {
                Ok(stream) => {
                    let range = self
                        .stream_range(stream)
                        .ok_or(ScrubMinidumpError::InvalidAddress("auxiliary stream"))?;
                    descriptors.push(StreamDescriptor(ValueType::Binary, range));
                }
                Err(minidump::Error::StreamNotFound) => (),
                Err(e) => return Err(ScrubMinidumpError::Parse("auxiliary stream", e)),
            }
        }

        Ok(descriptors)
    }
}

impl PiiAttachmentsProcessor<'_> {
    /// Applies PII rules to the given minidump.
    ///
    /// This function selectively opens minidump streams in order to avoid destroying the stack
    /// memory required for minidump processing. It visits:
    ///
    ///  1. All stack memory regions with `ValueType::StackMemory`
    ///  2. All other memory regions with `ValueType::HeapMemory`
    ///  3. Linux auxiliary streams with `ValueType::Binary`
    ///
    /// Returns `true`, if the minidump was modified.
    pub fn scrub_minidump(
        &self,
        filename: &str,
        data: &mut [u8],
    ) -> Result<bool, ScrubMinidumpError> {
        let file_state = self.state(filename, ValueType::Minidump);
        let streams = MinidumpData::parse(data)?.streams()?;
        let mut changed = false;

        for StreamDescriptor(value_type, range) in streams {
            let slice = data
                .get_mut(range)
                .ok_or(ScrubMinidumpError::InvalidAddress("foo"))?;

            // IMPORTANT: Minidump sections are always classified as Pii:Maybe. This avoids to
            // accidentally scrub stack memory with highly generic selectors. TODO: Update the PII
            // system with a better approach.
            let attrs = Cow::Owned(FieldAttrs::new().pii(Pii::Maybe));

            let state = file_state.enter_static("", Some(attrs), Some(value_type));
            changed |= self.scrub_bytes(slice, &state);
        }

        Ok(changed)
    }
}
