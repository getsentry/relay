use relay_protocol::{Annotated, Array, Empty, FromValue, IntoValue, Object, Value};

use crate::processor::ProcessValue;

// Some stability report fields are measured in 4K blocks, so we define a constant for that.
const _4KIB: u64 = 4096;

/// The state of a process.
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
pub struct ProcessStateContext {
    /// The identifier of the process.
    pub process_id: Annotated<u64>,
    pub memory_state: Annotated<process_state::MemoryStateContext>,
    pub file_system_state: Annotated<process_state::FileSystemStateContext>,
}

/// Nested message and enum types in `ProcessState`.
pub mod process_state {
    use super::*;

    /// Records the state of process memory at the time of crash.
    /// Next id: 6
    #[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
    pub struct MemoryStateContext {
        pub windows_memory: Annotated<memory_state::WindowsMemoryContext>,
    }
    /// Nested message and enum types in `MemoryState`.
    pub mod memory_state {
        use super::*;

        #[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
        pub struct WindowsMemoryContext {
            /// The private byte usage of the process.
            pub process_private_usage: Annotated<u64>,
            /// The peak working set usage of the process.
            pub process_peak_workingset_size: Annotated<u64>,
            /// The peak pagefile usage of the process.
            pub process_peak_pagefile_usage: Annotated<u64>,
            /// The allocation request that caused OOM bytes.
            pub process_allocation_attempt: Annotated<u64>,
        }
    }
    /// Records the state of the file system at the time of crash.
    #[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
    pub struct FileSystemStateContext {
        pub posix_file_system_state: Annotated<file_system_state::PosixFileSystemStateContext>,
        pub windows_file_system_state: Annotated<file_system_state::WindowsFileSystemStateContext>,
    }
    /// Nested message and enum types in `FileSystemState`.
    pub mod file_system_state {
        use super::*;

        #[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
        pub struct PosixFileSystemStateContext {
            /// The number of open file descriptors in the crashing process.
            pub open_file_descriptors: Annotated<u64>,
        }
        #[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
        pub struct WindowsFileSystemStateContext {
            /// The number of open handles in the process.
            pub process_handle_count: Annotated<u64>,
        }
    }
}
/// Records the state of system memory at the time of crash.
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
pub struct SystemMemoryStateContext {
    pub windows_memory: Annotated<system_memory_state::WindowsMemoryContext>,
}
/// Nested message and enum types in `SystemMemoryState`.
pub mod system_memory_state {
    use super::*;

    #[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]

    pub struct WindowsMemoryContext {
        /// The system commit limit.
        pub system_commit_limit: Annotated<u64>,
        /// The amount of system commit remaining.
        pub system_commit_remaining: Annotated<u64>,
        /// The current number of open handles.
        pub system_handle_count: Annotated<u64>,
    }
}

/// A stability report contains information pertaining to the execution of a
/// single logical instance of a "chrome browser". It is comprised of information
/// about the system state and about the chrome browser's processes.

#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
pub struct StabilityReportContext {
    /// State pertaining to Chrome's processes.
    pub process_states: Annotated<Array<ProcessStateContext>>,
    /// System-wide resource usage.
    pub system_memory_state: Annotated<SystemMemoryStateContext>,
}

impl super::DefaultContext for StabilityReportContext {
    fn default_key() -> &'static str {
        "chromium_stability_report"
    }

    fn from_context(context: super::Context) -> Option<Self> {
        match context {
            super::Context::ChromiumStabilityReport(c) => Some(*c),
            _ => None,
        }
    }

    fn cast(context: &super::Context) -> Option<&Self> {
        match context {
            super::Context::ChromiumStabilityReport(c) => Some(c),
            _ => None,
        }
    }

    fn cast_mut(context: &mut super::Context) -> Option<&mut Self> {
        match context {
            super::Context::ChromiumStabilityReport(c) => Some(c),
            _ => None,
        }
    }

    fn into_context(self) -> super::Context {
        super::Context::ChromiumStabilityReport(Box::new(self))
    }
}

// From implementations for converting from minidump types
impl From<minidump::StabilityReport> for StabilityReportContext {
    fn from(report: minidump::StabilityReport) -> Self {
        Self {
            process_states: Annotated::from(
                report
                    .process_states
                    .into_iter()
                    .map(|ps| Annotated::from(ProcessStateContext::from(ps)))
                    .collect::<Vec<_>>(),
            ),
            system_memory_state: report.system_memory_state.map(Into::into).into(),
        }
    }
}

impl From<minidump::ProcessState> for ProcessStateContext {
    fn from(state: minidump::ProcessState) -> Self {
        Self {
            process_id: state.process_id.map(|id| id as u64).into(),
            memory_state: state.memory_state.map(Into::into).into(),
            file_system_state: state.file_system_state.map(Into::into).into(),
        }
    }
}

impl From<minidump::SystemMemoryState> for SystemMemoryStateContext {
    fn from(state: minidump::SystemMemoryState) -> Self {
        Self {
            windows_memory: state.windows_memory.map(Into::into).into(),
        }
    }
}

impl From<minidump::system_memory_state::WindowsMemory>
    for system_memory_state::WindowsMemoryContext
{
    fn from(memory: minidump::system_memory_state::WindowsMemory) -> Self {
        Self {
            system_commit_limit: memory
                .system_commit_limit
                .map(|v| (v as u64) * _4KIB)
                .into(),
            system_commit_remaining: memory
                .system_commit_remaining
                .map(|v| (v as u64) * _4KIB)
                .into(),
            system_handle_count: memory.system_handle_count.map(|v| v as u64).into(),
        }
    }
}

impl From<minidump::process_state::MemoryState> for process_state::MemoryStateContext {
    fn from(state: minidump::process_state::MemoryState) -> Self {
        Self {
            windows_memory: state.windows_memory.map(Into::into).into(),
        }
    }
}

impl From<minidump::process_state::memory_state::WindowsMemory>
    for process_state::memory_state::WindowsMemoryContext
{
    fn from(memory: minidump::process_state::memory_state::WindowsMemory) -> Self {
        Self {
            process_private_usage: memory
                .process_private_usage
                .map(|v| (v as u64) * _4KIB)
                .into(),
            process_peak_workingset_size: memory
                .process_peak_workingset_size
                .map(|v| (v as u64) * _4KIB)
                .into(),
            process_peak_pagefile_usage: memory
                .process_peak_pagefile_usage
                .map(|v| (v as u64) * _4KIB)
                .into(),
            process_allocation_attempt: memory.process_allocation_attempt.map(|v| v as u64).into(),
        }
    }
}

impl From<minidump::process_state::FileSystemState> for process_state::FileSystemStateContext {
    fn from(state: minidump::process_state::FileSystemState) -> Self {
        Self {
            posix_file_system_state: state.posix_file_system_state.map(Into::into).into(),
            windows_file_system_state: state.windows_file_system_state.map(Into::into).into(),
        }
    }
}

impl From<minidump::process_state::file_system_state::PosixFileSystemState>
    for process_state::file_system_state::PosixFileSystemStateContext
{
    fn from(state: minidump::process_state::file_system_state::PosixFileSystemState) -> Self {
        Self {
            open_file_descriptors: state.open_file_descriptors.map(|v| v as u64).into(),
        }
    }
}

impl From<minidump::process_state::file_system_state::WindowsFileSystemState>
    for process_state::file_system_state::WindowsFileSystemStateContext
{
    fn from(state: minidump::process_state::file_system_state::WindowsFileSystemState) -> Self {
        Self {
            process_handle_count: state.process_handle_count.map(|v| v as u64).into(),
        }
    }
}
