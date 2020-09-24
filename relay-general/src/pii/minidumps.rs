//! Minidump scrubbing.

use std::borrow::Cow;
use std::convert::TryInto;
use std::num::TryFromIntError;
use std::ops::Range;
use std::str::Utf8Error;

use failure::Fail;
use minidump::format::{
    CvSignature, MINIDUMP_LOCATION_DESCRIPTOR, MINIDUMP_STREAM_TYPE as StreamType, RVA,
};
use minidump::{
    Error as MinidumpError, Minidump, MinidumpMemoryList, MinidumpModuleList, MinidumpThreadList,
};
use num_traits::FromPrimitive;

use relay_wstring::{Utf16Error, WStr};

use crate::pii::{PiiAttachmentsProcessor, ScrubEncodings};
use crate::processor::{FieldAttrs, Pii, ValueType};

#[derive(Debug, Fail)]
pub enum ScrubMinidumpError {
    #[fail(display = "failed to parse minidump")]
    InvalidMinidump(#[cause] MinidumpError),

    #[fail(display = "invalid memory address")]
    InvalidAddress,

    #[fail(display = "minidump offsets out of usize range")]
    OutOfRange,

    #[fail(display = "string decoding error")]
    Decoding,
}

impl From<TryFromIntError> for ScrubMinidumpError {
    fn from(_source: TryFromIntError) -> Self {
        Self::OutOfRange
    }
}

impl From<MinidumpError> for ScrubMinidumpError {
    fn from(source: MinidumpError) -> Self {
        Self::InvalidMinidump(source)
    }
}

impl From<Utf16Error> for ScrubMinidumpError {
    fn from(_source: Utf16Error) -> Self {
        Self::Decoding
    }
}

impl From<Utf8Error> for ScrubMinidumpError {
    fn from(_source: Utf8Error) -> Self {
        Self::Decoding
    }
}

/// Items of the minidump which we are interested in.
///
/// For our own convenience we like to be able to identify which areas of the minidump we
/// have.  This locates the data using [Range] slices since we can not take references to
/// the original data where we construct these.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
enum MinidumpItem {
    /// Stack memory region.
    StackMemory(Range<usize>),
    /// Memory region not associated with a stack stack/thread.
    NonStackMemory(Range<usize>),
    /// The Linux environ block.
    ///
    /// This is a NULL-byte separated list of `KEY=value` pairs.
    LinuxEnviron(Range<usize>),
    /// The Linux cmdline block.
    ///
    /// This is a NULL-byte separated list of arguments.
    LinuxCmdLine(Range<usize>),
    /// This is a UTF-16LE encoded pathname of a code module.
    CodeModuleName(Range<usize>),
    /// This is a UTF-16LE encoded pathname of a debug file.
    DebugModuleName(Range<usize>),
}

/// Internal struct to keep a minidump and it's raw data together.
struct MinidumpData<'a> {
    data: &'a [u8],
    minidump: Minidump<'a, &'a [u8]>,
}

impl<'a> MinidumpData<'a> {
    /// Parses raw minidump data into the readable `Minidump` struct.
    ///
    /// This does only read the stream index, individual streams might still be corrupt even
    /// when parsing this succeeds.
    fn parse(data: &'a [u8]) -> Result<Self, ScrubMinidumpError> {
        let minidump = Minidump::read(data).map_err(ScrubMinidumpError::InvalidMinidump)?;
        Ok(Self { data, minidump })
    }

    /// Returns the offset of a given slice into the minidump data.
    ///
    /// In minidump parlance this is also known as the RVA or Relative Virtual Address.
    /// E.g. if all the raw minidump data is `data` and you have `&data[start..end]` this
    /// returns you `start`.
    fn offset(&self, slice: &[u8]) -> Option<usize> {
        let base = self.data.as_ptr() as usize;
        let pointer = slice.as_ptr() as usize;

        if pointer > base {
            Some(pointer - base)
        } else {
            None
        }
    }

    /// Returns the `Range` in the raw minidump data of a slice in the minidump data.
    fn slice_range(&self, slice: &[u8]) -> Option<Range<usize>> {
        let start = self.offset(slice)?;
        let end = start + slice.len();
        Some(start..end)
    }

    /// Returns the `Range` in the raw minidump data of a `MINIDUMP_LOCATION_DESCRIPTOR`.
    ///
    /// This allows you to create a slice of the data specified in the location descriptor.
    fn location_range(
        &self,
        location: MINIDUMP_LOCATION_DESCRIPTOR,
    ) -> Result<Range<usize>, ScrubMinidumpError> {
        let start: usize = location.rva.try_into()?;
        let len: usize = location.data_size.try_into()?;
        Ok(start..start + len)
    }

    /// Returns the range of a raw stream, if the stream is preset.
    fn raw_stream_range(
        &self,
        stream_type: StreamType,
    ) -> Result<Option<Range<usize>>, ScrubMinidumpError> {
        let range = match self.minidump.get_raw_stream(stream_type) {
            Ok(stream) => Some(
                self.slice_range(stream)
                    .ok_or(ScrubMinidumpError::InvalidAddress)?,
            ),
            Err(MinidumpError::StreamNotFound) => None,
            Err(e) => return Err(ScrubMinidumpError::InvalidMinidump(e)),
        };
        Ok(range)
    }

    /// Extracts all items we care about.
    fn items(&self) -> Result<Vec<MinidumpItem>, ScrubMinidumpError> {
        let mut items = Vec::new();

        let thread_list: MinidumpThreadList = self.minidump.get_stream()?;
        let stack_rvas: Vec<RVA> = thread_list
            .threads
            .iter()
            .map(|t| t.raw.stack.memory.rva)
            .collect();

        let mem_list: MinidumpMemoryList = self.minidump.get_stream()?;
        for mem in mem_list.iter() {
            if stack_rvas.contains(&mem.desc.memory.rva) {
                items.push(MinidumpItem::StackMemory(
                    self.location_range(mem.desc.memory)?,
                ));
            } else {
                items.push(MinidumpItem::NonStackMemory(
                    self.location_range(mem.desc.memory)?,
                ));
            }
        }

        if let Some(range) = self.raw_stream_range(StreamType::LinuxEnviron)? {
            items.push(MinidumpItem::LinuxEnviron(range));
        }
        if let Some(range) = self.raw_stream_range(StreamType::LinuxCmdLine)? {
            items.push(MinidumpItem::LinuxCmdLine(range));
        }

        let mod_list: MinidumpModuleList = self.minidump.get_stream()?;
        let mut rvas = Vec::new();
        for module in mod_list.iter() {
            let rva: usize = module.raw.module_name_rva.try_into()?;
            if rvas.contains(&rva) {
                continue;
            } else {
                rvas.push(rva);
            }
            let len: usize = u32_from_le_bytes(&self.data[rva..])?.try_into()?;
            let start: usize = rva + 4;
            items.push(MinidumpItem::CodeModuleName(start..start + len));

            // Try to get the raw debug name range.  Minidump API only give us an owned version.
            let codeview_loc = module.raw.cv_record;
            let cv_start: usize = codeview_loc.rva.try_into()?;
            let cv_len: usize = codeview_loc.data_size.try_into()?;
            let signature = u32_from_le_bytes(&self.data[cv_start..])?;
            match CvSignature::from_u32(signature) {
                Some(CvSignature::Pdb70) => {
                    let offset: usize = 4 + (4 + 2 + 2 + 8) + 4; // cv_sig + sig GUID + age
                    items.push(MinidumpItem::DebugModuleName(
                        (cv_start + offset)..(cv_start + cv_len),
                    ));
                }
                Some(CvSignature::Pdb20) => {
                    let offset: usize = 4 + 4 + 4 + 4; // cv_sig + cv_offset + sig + age
                    items.push(MinidumpItem::DebugModuleName(
                        (cv_start + offset)..(cv_start + cv_len),
                    ));
                }
                _ => {}
            }
        }

        Ok(items)
    }
}

fn u32_from_le_bytes(bytes: &[u8]) -> Result<u32, ScrubMinidumpError> {
    let mut buf = [0u8; 4];
    buf.copy_from_slice(bytes.get(..4).ok_or(ScrubMinidumpError::InvalidAddress)?);
    Ok(u32::from_le_bytes(buf))
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
        let items = MinidumpData::parse(data)?.items()?;
        let mut changed = false;

        for item in items {
            // IMPORTANT: Minidump sections are always classified as Pii:Maybe. This avoids to
            // accidentally scrub stack memory with highly generic selectors. TODO: Update the PII
            // system with a better approach.
            let attrs = Cow::Owned(FieldAttrs::new().pii(Pii::Maybe));

            match item {
                MinidumpItem::StackMemory(range) => {
                    let slice = data
                        .get_mut(range)
                        .ok_or(ScrubMinidumpError::InvalidAddress)?;
                    let state =
                        file_state.enter_static("", Some(attrs), Some(ValueType::StackMemory));
                    changed |= self.scrub_bytes(slice, &state, ScrubEncodings::All);
                }
                MinidumpItem::NonStackMemory(range) => {
                    let slice = data
                        .get_mut(range)
                        .ok_or(ScrubMinidumpError::InvalidAddress)?;
                    let state =
                        file_state.enter_static("", Some(attrs), Some(ValueType::HeapMemory));
                    changed |= self.scrub_bytes(slice, &state, ScrubEncodings::All);
                }
                MinidumpItem::LinuxEnviron(range) | MinidumpItem::LinuxCmdLine(range) => {
                    let slice = data
                        .get_mut(range)
                        .ok_or(ScrubMinidumpError::InvalidAddress)?;
                    let state = file_state.enter_static("", Some(attrs), Some(ValueType::Binary));
                    changed |= self.scrub_bytes(slice, &state, ScrubEncodings::All);
                }
                MinidumpItem::CodeModuleName(range) => {
                    let slice = data
                        .get_mut(range)
                        .ok_or(ScrubMinidumpError::InvalidAddress)?;
                    let state = file_state.enter_static(
                        "code_file",
                        Some(attrs),
                        Some(ValueType::Filepath),
                    );
                    let wstr = WStr::from_utf16le_mut(slice)?; // TODO: Consider making this lossy?
                    changed |= self.scrub_utf16_filepath(wstr, &state);
                }
                MinidumpItem::DebugModuleName(range) => {
                    let slice = data
                        .get_mut(range)
                        .ok_or(ScrubMinidumpError::InvalidAddress)?;
                    let state = file_state.enter_static(
                        "debug_file",
                        Some(attrs),
                        Some(ValueType::Filepath),
                    );
                    let s = std::str::from_utf8_mut(slice)?;
                    changed |= self.scrub_utf8_filepath(s, &state);
                }
            };
        }

        Ok(changed)
    }
}
