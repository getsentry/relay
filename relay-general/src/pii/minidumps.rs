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
use scroll::Endian;
use utf16string::{Utf16Error, WStr};

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
            let len: usize = u32_from_bytes(&self.data[rva..], self.minidump.endian)?.try_into()?;
            let start: usize = rva + 4;
            items.push(MinidumpItem::CodeModuleName(start..start + len));

            // Try to get the raw debug name range.  Minidump API only give us an owned version.
            let codeview_loc = module.raw.cv_record;
            let cv_start: usize = codeview_loc.rva.try_into()?;
            let cv_len: usize = codeview_loc.data_size.try_into()?;
            let signature = u32_from_bytes(&self.data[cv_start..], self.minidump.endian)?;
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

/// Read a u32 from the start of a byte-slice.
///
/// This uses the [Endian] indicator as used by scroll.  It is exceedingly close in
/// functionality to `bytes.pread_with(0, endian)` from scroll directly, only differing in
/// the error type.
fn u32_from_bytes(bytes: &[u8], endian: Endian) -> Result<u32, ScrubMinidumpError> {
    let mut buf = [0u8; 4];
    buf.copy_from_slice(bytes.get(..4).ok_or(ScrubMinidumpError::InvalidAddress)?);
    match endian {
        Endian::Little => Ok(u32::from_le_bytes(buf)),
        Endian::Big => Ok(u32::from_be_bytes(buf)),
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
        let items = MinidumpData::parse(data)?.items()?;
        let mut changed = false;

        for item in items {
            match item {
                MinidumpItem::StackMemory(range) => {
                    // IMPORTANT: The stack is PII::Maybe to avoid accidentally scrubbing it
                    // with highly generic selectors.
                    let slice = data
                        .get_mut(range)
                        .ok_or(ScrubMinidumpError::InvalidAddress)?;

                    let attrs = Cow::Owned(FieldAttrs::new().pii(Pii::Maybe));
                    let state = file_state.enter_static(
                        "stack_memory",
                        Some(attrs),
                        ValueType::Binary | ValueType::StackMemory,
                    );
                    changed |= self.scrub_bytes(slice, &state, ScrubEncodings::All);
                }
                MinidumpItem::NonStackMemory(range) => {
                    let slice = data
                        .get_mut(range)
                        .ok_or(ScrubMinidumpError::InvalidAddress)?;
                    let attrs = Cow::Owned(FieldAttrs::new().pii(Pii::True));
                    let state = file_state.enter_static(
                        "heap_memory",
                        Some(attrs),
                        ValueType::Binary | ValueType::HeapMemory,
                    );
                    changed |= self.scrub_bytes(slice, &state, ScrubEncodings::All);
                }
                MinidumpItem::LinuxEnviron(range) | MinidumpItem::LinuxCmdLine(range) => {
                    let slice = data
                        .get_mut(range)
                        .ok_or(ScrubMinidumpError::InvalidAddress)?;
                    let attrs = Cow::Owned(FieldAttrs::new().pii(Pii::True));
                    let state = file_state.enter_static("", Some(attrs), Some(ValueType::Binary));
                    changed |= self.scrub_bytes(slice, &state, ScrubEncodings::All);
                }
                MinidumpItem::CodeModuleName(range) => {
                    let slice = data
                        .get_mut(range)
                        .ok_or(ScrubMinidumpError::InvalidAddress)?;
                    let attrs = Cow::Owned(FieldAttrs::new().pii(Pii::True));
                    // Mirrors decisions made on NativeImagePath type
                    let state =
                        file_state.enter_static("code_file", Some(attrs), Some(ValueType::String));
                    let wstr = WStr::from_utf16le_mut(slice)?; // TODO: Consider making this lossy?
                    changed |= self.scrub_utf16_filepath(wstr, &state);
                }
                MinidumpItem::DebugModuleName(range) => {
                    let slice = data
                        .get_mut(range)
                        .ok_or(ScrubMinidumpError::InvalidAddress)?;
                    let attrs = Cow::Owned(FieldAttrs::new().pii(Pii::True));
                    // Mirrors decisions made on NativeImagePath type
                    let state =
                        file_state.enter_static("debug_file", Some(attrs), Some(ValueType::String));
                    let s = std::str::from_utf8_mut(slice)?;
                    changed |= self.scrub_utf8_filepath(s, &state);
                }
            };
        }

        Ok(changed)
    }
}

#[cfg(test)]
mod tests {
    use minidump::{MinidumpModule, Module};

    use crate::pii::PiiConfig;

    use super::*;

    struct TestScrubber {
        orig_dump: Minidump<'static, &'static [u8]>,
        _scrubbed_data: Vec<u8>,
        scrubbed_dump: Minidump<'static, &'static [u8]>,
    }

    impl TestScrubber {
        fn new(filename: &str, orig_data: &'static [u8], json: serde_json::Value) -> Self {
            let orig_dump = Minidump::read(orig_data).expect("original minidump failed to parse");
            let mut scrubbed_data = Vec::from(orig_data);

            let config = serde_json::from_value::<PiiConfig>(json).expect("invalid config json");
            let compiled = config.compiled();
            let processor = PiiAttachmentsProcessor::new(&compiled);
            processor
                .scrub_minidump(filename, scrubbed_data.as_mut_slice())
                .expect("scrubbing failed");

            // We could let scrubbed_dump just consume the Vec<[u8]>, but that would give
            // both our dumps different types which is awkward to work with.  So we store
            // the Vec separately to keep the slice alive and pretend we give Minidump a
            // &'static [u8].
            let slice = unsafe { std::mem::transmute(scrubbed_data.as_slice()) };
            let scrubbed_dump = Minidump::read(slice).expect("scrubbed minidump failed to parse");
            Self {
                orig_dump,
                _scrubbed_data: scrubbed_data,
                scrubbed_dump,
            }
        }
    }

    enum Which {
        Original,
        Scrubbed,
    }

    enum MemRegion {
        Stack,
        Heap,
    }

    impl TestScrubber {
        fn main_module(&self, which: Which) -> MinidumpModule {
            let dump = match which {
                Which::Original => &self.orig_dump,
                Which::Scrubbed => &self.scrubbed_dump,
            };
            let modules: MinidumpModuleList = dump.get_stream().unwrap();
            modules.main_module().unwrap().clone()
        }

        fn other_modules(&self, which: Which) -> Vec<MinidumpModule> {
            let dump = match which {
                Which::Original => &self.orig_dump,
                Which::Scrubbed => &self.scrubbed_dump,
            };
            let modules: MinidumpModuleList = dump.get_stream().unwrap();
            let mut iter = modules.iter();
            iter.next(); // remove main module
            iter.cloned().collect()
        }

        /// Returns the raw stack or heap memory regions.
        fn memory_regions<'slf>(&'slf self, which: Which, region: MemRegion) -> Vec<&'slf [u8]> {
            let dump: &'slf Minidump<&'static [u8]> = match which {
                Which::Original => &self.orig_dump,
                Which::Scrubbed => &self.scrubbed_dump,
            };

            let thread_list: MinidumpThreadList = dump.get_stream().unwrap();
            let stack_rvas: Vec<RVA> = thread_list
                .threads
                .iter()
                .map(|t| t.raw.stack.memory.rva)
                .collect();

            // These bytes are kept alive by our struct itself, so returning them with the
            // lifetime of our struct is fine.  The lifetimes on the Minidump::MemoryRegions
            // iterator are currenty wrong and assumes we keep a reference to the
            // MinidumpMemoryList, hence we need to transmute this.  See
            // https://github.com/luser/rust-minidump/pull/111
            let mem_list: MinidumpMemoryList<'slf> = dump.get_stream().unwrap();
            mem_list
                .iter()
                .filter(|mem| match region {
                    MemRegion::Stack => stack_rvas.contains(&mem.desc.memory.rva),
                    MemRegion::Heap => !stack_rvas.contains(&mem.desc.memory.rva),
                })
                .map(|mem| unsafe { std::mem::transmute(mem.bytes) })
                .collect()
        }

        /// Returns the raw stack memory regions.
        fn stacks(&self, which: Which) -> Vec<&[u8]> {
            self.memory_regions(which, MemRegion::Stack)
        }

        /// Returns the raw heap memory regions.
        fn heaps(&self, which: Which) -> Vec<&[u8]> {
            self.memory_regions(which, MemRegion::Heap)
        }

        /// Returns the Linux environ region.
        ///
        /// Panics if there is no such region.
        fn environ(&self, which: Which) -> &[u8] {
            let dump = match which {
                Which::Original => &self.orig_dump,
                Which::Scrubbed => &self.scrubbed_dump,
            };
            dump.get_raw_stream(StreamType::LinuxEnviron).unwrap()
        }
    }

    #[test]
    fn test_module_list_removed_win() {
        let scrubber = TestScrubber::new(
            "windows.dmp",
            include_bytes!("../../../tests/fixtures/windows.dmp"),
            serde_json::json!(
                {
                    "applications": {
                        "debug_file": ["@anything:mask"],
                        "$attachments.'windows.dmp'.code_file": ["@anything:mask"]
                    }
                }
            ),
        );

        let main = scrubber.main_module(Which::Original);
        assert_eq!(
            main.code_file(),
            "C:\\projects\\breakpad-tools\\windows\\Release\\crash.exe"
        );
        assert_eq!(
            main.debug_file().unwrap(),
            "C:\\projects\\breakpad-tools\\windows\\Release\\crash.pdb"
        );

        let main = scrubber.main_module(Which::Scrubbed);
        assert_eq!(
            main.code_file(),
            "******************************************\\crash.exe"
        );
        assert_eq!(
            main.debug_file().unwrap(),
            "******************************************\\crash.pdb"
        );

        let modules = scrubber.other_modules(Which::Original);
        for module in modules {
            assert!(
                module.code_file().starts_with("C:\\Windows\\System32\\"),
                "code file without full path"
            );
            assert!(module.debug_file().unwrap().ends_with(".pdb"));
        }

        let modules = scrubber.other_modules(Which::Scrubbed);
        for module in modules {
            assert!(
                module.code_file().starts_with("*******************\\"),
                "code file path not scrubbed"
            );
            assert!(module.debug_file().unwrap().ends_with(".pdb"));
        }
    }

    #[test]
    fn test_module_list_removed_lin() {
        let scrubber = TestScrubber::new(
            "linux.dmp",
            include_bytes!("../../../tests/fixtures/linux.dmp"),
            serde_json::json!(
                {
                    "applications": {
                        "debug_file": ["@anything:mask"],
                        "$attachments.*.code_file": ["@anything:mask"]
                    }
                }
            ),
        );

        let main = scrubber.main_module(Which::Original);
        assert_eq!(main.code_file(), "/work/linux/build/crash");
        assert_eq!(main.debug_file().unwrap(), "/work/linux/build/crash");

        let main = scrubber.main_module(Which::Scrubbed);
        assert_eq!(main.code_file(), "*****************/crash");
        assert_eq!(main.debug_file().unwrap(), "*****************/crash");

        let modules = scrubber.other_modules(Which::Original);
        for module in modules {
            assert!(
                module.code_file().matches('/').count() > 1
                    || module.code_file() == "linux-gate.so",
                "code file does not contain path"
            );
            assert!(
                module.debug_file().unwrap().matches('/').count() > 1
                    || module.debug_file().unwrap() == "linux-gate.so",
                "debug file does not contain a path"
            );
        }

        let modules = scrubber.other_modules(Which::Scrubbed);
        for module in modules {
            assert!(
                module.code_file().matches('/').count() == 1
                    || module.code_file() == "linux-gate.so",
                "code file not scrubbed"
            );
            assert!(
                module.debug_file().unwrap().matches('/').count() == 1
                    || module.debug_file().unwrap() == "linux-gate.so",
                "scrubbed debug file contains a path"
            );
        }
    }

    #[test]
    fn test_module_list_removed_mac() {
        let scrubber = TestScrubber::new(
            "macos.dmp",
            include_bytes!("../../../tests/fixtures/macos.dmp"),
            serde_json::json!(
                {
                    "applications": {
                        "debug_file": ["@anything:mask"],
                        "$attachments.*.code_file": ["@anything:mask"]
                    }
                }
            ),
        );

        let main = scrubber.main_module(Which::Original);
        assert_eq!(
            main.code_file(),
            "/Users/travis/build/getsentry/breakpad-tools/macos/build/./crash"
        );
        assert_eq!(main.debug_file().unwrap(), "crash");

        let main = scrubber.main_module(Which::Scrubbed);
        assert_eq!(
            main.code_file(),
            "**********************************************************/crash"
        );
        assert_eq!(main.debug_file().unwrap(), "crash");

        let modules = scrubber.other_modules(Which::Original);
        for module in modules.iter() {
            dbg!(module.code_file());
            dbg!(module.debug_file());
        }
        for module in modules {
            assert!(
                module.code_file().matches('/').count() > 1,
                "code file does not contain path"
            );
            assert!(
                module.debug_file().unwrap().matches('/').count() == 0,
                "debug file contains a path"
            );
        }

        let modules = scrubber.other_modules(Which::Scrubbed);
        for module in modules {
            assert!(
                module.code_file().matches('/').count() == 1,
                "code file not scrubbed"
            );
            assert!(
                module.debug_file().unwrap().matches('/').count() == 0,
                "scrubbed debug file contains a path"
            );
        }
    }

    #[test]
    fn test_module_list_selectors() {
        // Since scrubbing the module list is safe, it should be scrubbed by valuetype.
        let scrubber = TestScrubber::new(
            "linux.dmp",
            include_bytes!("../../../tests/fixtures/linux.dmp"),
            serde_json::json!(
                {
                    "applications": {
                        "$string": ["@anything:mask"],
                    }
                }
            ),
        );
        let main = scrubber.main_module(Which::Scrubbed);
        assert_eq!(main.code_file(), "*****************/crash");
        assert_eq!(main.debug_file().unwrap(), "*****************/crash");
    }

    #[test]
    fn test_stack_scrubbing_backwards_compatible_selector() {
        // Some users already use this bare selector, that's all we care about for backwards
        // compatibility.
        let scrubber = TestScrubber::new(
            "linux.dmp",
            include_bytes!("../../../tests/fixtures/linux.dmp"),
            serde_json::json!(
                {
                    "applications": {
                        "$stack_memory": ["@anything:mask"],
                    }
                }
            ),
        );
        for stack in scrubber.stacks(Which::Scrubbed) {
            assert!(stack.iter().all(|b| *b == b'*'));
        }
    }

    #[test]
    fn test_stack_scrubbing_path_item_selector() {
        let scrubber = TestScrubber::new(
            "linux.dmp",
            include_bytes!("../../../tests/fixtures/linux.dmp"),
            serde_json::json!(
                {
                    "applications": {
                        "$minidump.stack_memory": ["@anything:mask"],
                    }
                }
            ),
        );
        for stack in scrubber.stacks(Which::Scrubbed) {
            assert!(stack.iter().all(|b| *b == b'*'));
        }
    }

    #[test]
    #[should_panic]
    fn test_stack_scrubbing_valuetype_selector() {
        // This should work, but is known to fail currently because the selector logic never
        // considers a selector containing $binary as specific.
        let scrubber = TestScrubber::new(
            "linux.dmp",
            include_bytes!("../../../tests/fixtures/linux.dmp"),
            serde_json::json!(
                {
                    "applications": {
                        "$minidump.$binary": ["@anything:mask"],
                    }
                }
            ),
        );
        for stack in scrubber.stacks(Which::Scrubbed) {
            assert!(stack.iter().all(|b| *b == b'*'));
        }
    }

    #[test]
    fn test_stack_scrubbing_valuetype_not_fully_qualified() {
        // Not fully qualified valuetype should not touch the stack
        let scrubber = TestScrubber::new(
            "linux.dmp",
            include_bytes!("../../../tests/fixtures/linux.dmp"),
            serde_json::json!(
                {
                    "applications": {
                        "$binary": ["@anything:mask"],
                    }
                }
            ),
        );
        for (scrubbed_stack, original_stack) in scrubber
            .stacks(Which::Scrubbed)
            .iter()
            .zip(scrubber.stacks(Which::Original).iter())
        {
            assert_eq!(scrubbed_stack, original_stack);
        }
    }

    #[test]
    #[should_panic]
    fn test_stack_scrubbing_wildcard() {
        // Wildcard should not touch the stack.  However currently wildcards are considered
        // specific selectors so they do.  This is a known issue.
        let scrubber = TestScrubber::new(
            "linux.dmp",
            include_bytes!("../../../tests/fixtures/linux.dmp"),
            serde_json::json!(
                {
                    "applications": {
                        "$minidump.*": ["@anything:mask"],
                    }
                }
            ),
        );
        for (scrubbed_stack, original_stack) in scrubber
            .stacks(Which::Scrubbed)
            .iter()
            .zip(scrubber.stacks(Which::Original).iter())
        {
            assert_eq!(scrubbed_stack, original_stack);
        }
    }

    #[test]
    fn test_stack_scrubbing_deep_wildcard() {
        // Wildcard should not touch the stack
        let scrubber = TestScrubber::new(
            "linux.dmp",
            include_bytes!("../../../tests/fixtures/linux.dmp"),
            serde_json::json!(
                {
                    "applications": {
                        "$attachments.**": ["@anything:mask"],
                    }
                }
            ),
        );
        for (scrubbed_stack, original_stack) in scrubber
            .stacks(Which::Scrubbed)
            .iter()
            .zip(scrubber.stacks(Which::Original).iter())
        {
            assert_eq!(scrubbed_stack, original_stack);
        }
    }

    #[test]
    fn test_stack_scrubbing_binary_not_stack() {
        let scrubber = TestScrubber::new(
            "linux.dmp",
            include_bytes!("../../../tests/fixtures/linux.dmp"),
            serde_json::json!(
                {
                    "applications": {
                        "$binary && !stack_memory": ["@anything:mask"],
                    }
                }
            ),
        );
        for (scrubbed_stack, original_stack) in scrubber
            .stacks(Which::Scrubbed)
            .iter()
            .zip(scrubber.stacks(Which::Original).iter())
        {
            assert_eq!(scrubbed_stack, original_stack);
        }
        for heap in scrubber.heaps(Which::Scrubbed) {
            assert!(heap.iter().all(|b| *b == b'*'));
        }
    }

    #[test]
    fn test_linux_environ_valuetype() {
        // The linux environ should be scrubbed for any $binary
        let scrubber = TestScrubber::new(
            "linux.dmp",
            include_bytes!("../../../tests/fixtures/linux.dmp"),
            serde_json::json!(
                {
                    "applications": {
                        "$binary": ["@anything:mask"],
                    }
                }
            ),
        );
        let environ = scrubber.environ(Which::Scrubbed);
        assert!(environ.iter().all(|b| *b == b'*'));
    }
}
