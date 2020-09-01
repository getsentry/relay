//! Minidump scrubbing.

use std::borrow::Cow;

use encoding::all::UTF_16LE;
use encoding::Encoding;
use failure::Fail;
use minidump::format::{MINIDUMP_LOCATION_DESCRIPTOR, MINIDUMP_STREAM_TYPE as StreamType, RVA};
use minidump::{Error as MinidumpError, Minidump, MinidumpMemoryList, MinidumpThreadList};

use crate::pii::PiiAttachmentsProcessor;
use crate::processor::{FieldAttrs, Pii, ValueType};

type Range = std::ops::Range<usize>;

#[derive(Debug, Fail)]
pub enum ScrubMinidumpError {
    #[fail(display = "failed to parse minidump")]
    InvalidMinidump(#[cause] MinidumpError),

    #[fail(display = "invalid memory address when reading {:?}", _0)]
    InvalidAddress(StreamType),

    #[fail(display = "failed to parse {:?}", _0)]
    Parse(StreamType, #[cause] MinidumpError),
}

#[derive(Clone, Debug)]
struct StreamDescriptor(StreamType, ValueType, Range);

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
            .map_err(|e| ScrubMinidumpError::Parse(StreamType::ThreadListStream, e))?;

        let stack_rvas: Vec<RVA> = thread_list
            .threads
            .iter()
            .map(|t| t.raw.stack.memory.rva)
            .collect();

        let mem_list: MinidumpMemoryList = self
            .minidump
            .get_stream()
            .map_err(|e| ScrubMinidumpError::Parse(StreamType::MemoryListStream, e))?;

        let mut descriptors = Vec::new();
        for mem in mem_list.iter() {
            let value_type = if stack_rvas.contains(&mem.desc.memory.rva) {
                ValueType::StackMemory
            } else {
                ValueType::HeapMemory
            };

            let range = self.location_range(mem.desc.memory);
            descriptors.push(StreamDescriptor(
                StreamType::MemoryListStream,
                value_type,
                range,
            ));
        }

        let aux_stream_types = [StreamType::LinuxEnviron, StreamType::LinuxCmdLine];

        for &stream_type in &aux_stream_types {
            match self.minidump.get_raw_stream(stream_type) {
                Ok(stream) => {
                    let range = self
                        .stream_range(stream)
                        .ok_or(ScrubMinidumpError::InvalidAddress(stream_type))?;
                    descriptors.push(StreamDescriptor(stream_type, ValueType::Binary, range));
                }
                Err(minidump::Error::StreamNotFound) => (),
                Err(e) => return Err(ScrubMinidumpError::Parse(stream_type, e)),
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

        for StreamDescriptor(stream_type, value_type, range) in streams {
            let slice = data
                .get_mut(range)
                .ok_or(ScrubMinidumpError::InvalidAddress(stream_type))?;

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

struct StringSegment {
    decoded: String,
    input_pos: Range,
}

// TODO: Make this an iterator to avoid more allocations?
fn extract_strings(data: &[u8]) -> Vec<StringSegment> {
    let mut ret = Vec::new();
    let mut offset = 0;
    let mut decoder = UTF_16LE.raw_decoder();

    while offset < data.len() {
        let mut decoded = String::new();
        let (unprocessed_offset, err) = decoder.raw_feed(&data[offset..], &mut decoded);

        if decoded.len() > 2 {
            let input_pos = Range {
                start: offset,
                end: offset + unprocessed_offset,
            };
            ret.push(StringSegment { decoded, input_pos });
        }

        if let Some(err) = err {
            if err.upto > 0 {
                offset += err.upto as usize;
            } else {
                // This should never happen, but if it does, re-set the decoder and skip
                // forward to the next 2 bytes.
                offset += std::mem::size_of::<u16>();
                decoder = decoder.from_self();
            }
        } else {
            // We are at the end of input.  There could be some unprocessed bytes left, but
            // we have no more data to feed to the decoder so just stop.
            break;
        }
    }
    ret
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_strings_entire() {
        let data = b"h\x00e\x00l\x00l\x00o\x00";
        let ret = extract_strings(&data[..]);
        assert_eq!(ret.len(), 1);
        assert_eq!(ret[0].decoded, "hello".to_string());
        assert_eq!(ret[0].input_pos, Range { start: 0, end: 10 });
        assert_eq!(&data[ret[0].input_pos.clone()], &data[..]);
    }

    #[test]
    fn test_extract_strings_middle_2_byte_aligned() {
        let data = b"\xd8\xd8\xd8\xd8h\x00e\x00l\x00l\x00o\x00\xd8\xd8";
        let ret = extract_strings(&data[..]);
        assert_eq!(ret.len(), 1);
        assert_eq!(ret[0].decoded, "hello".to_string());
        assert_eq!(ret[0].input_pos, Range { start: 4, end: 14 });
        assert_eq!(
            &data[ret[0].input_pos.clone()],
            b"h\x00e\x00l\x00l\x00o\x00"
        );
    }

    #[test]
    fn test_extract_strings_middle_unaligned() {
        let data = b"\xd8\xd8\xd8h\x00e\x00l\x00l\x00o\x00\xd8\xd8";
        let ret = extract_strings(&data[..]);
        assert_eq!(ret.len(), 1);
        assert_ne!(ret[0].decoded, "hello".to_string());
        assert_eq!(ret[0].input_pos, Range { start: 2, end: 12 });
    }

    #[test]
    fn test_extract_strings_end_aligned() {
        let data = b"\xd8\xd8h\x00e\x00l\x00l\x00o\x00";
        let ret = extract_strings(&data[..]);
        assert_eq!(ret.len(), 1);
        assert_eq!(ret[0].decoded, "hello".to_string());
    }

    #[test]
    fn test_extract_strings_garbage() {
        let data = b"\xd8\xd8";
        let ret = extract_strings(&data[..]);
        assert_eq!(ret.len(), 0);
    }

    #[test]
    fn test_extract_strings_short() {
        let data = b"\xd8\xd8y\x00o\x00\xd8\xd8h\x00e\x00l\x00l\x00o\x00";
        let ret = extract_strings(&data[..]);
        assert_eq!(ret.len(), 1);
        assert_eq!(ret[0].decoded, "hello".to_string());
    }

    #[test]
    fn test_extract_strings_minidump() {
        let data =
            std::fs::read("/Users/flub/code/symbolicator/tests/fixtures/windows.dmp").unwrap();
        let ret = extract_strings(&data[..]);
        println!("count: {}", ret.len());
        for segment in ret {
            println!("{}", &segment.decoded);
        }
        panic!("done");
    }
}
