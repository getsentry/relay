use std::path::PathBuf;

use anyhow::{Context, Result};
use failure::Fail;
use scroll::Pread;
use structopt::clap::AppSettings;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(setting = AppSettings::ColoredHelp)]
struct CliArgs {
    /// Path to the minidump to rewrite.
    input: PathBuf,
    /// Path to where to write the re-written minidump
    output: PathBuf,
}

#[paw::main]
fn main(argv: CliArgs) -> Result<()> {
    let mut data = std::fs::read(argv.input)?;
    let dump = minidump::Minidump::read(data.as_slice())
        .map_err(|e| e.compat())
        .context("Failed to read minidump")?;

    let thread_list: minidump::MinidumpThreadList = dump
        .get_stream()
        .map_err(|e| e.compat())
        .context("Failed to parse thread information from minidump")?;

    // The Relative Virtual Addresses (offsets into the minidump) of the start of memory
    // regions referenced by threads.  These identify memory regions that are stack memory.
    let stack_rvas: Vec<minidump::format::RVA> = thread_list
        .threads
        .iter()
        .map(|t| t.raw.stack.memory.rva)
        .collect();

    // First we need to collect all the locations we would like to mutate.  The Minidump are
    // its related types borrow the data immutably, however they also have "descriptor"
    // structs which point at locations inside the minidup without borrowing any data from
    // them.  So we first collect all those descriptors using the normal API before starting
    // the mutate the data using the descriptors.

    // Collect the descriptors for mutating memory regions.
    let mem_list: minidump::MinidumpMemoryList = dump
        .get_stream()
        .map_err(|e| e.compat())
        .context("Failed to parse memory regions from minidump")?;
    let mem_descriptors: Vec<minidump::format::MINIDUMP_MEMORY_DESCRIPTOR> =
        mem_list.iter().map(|mem| mem.desc).collect();

    // Collect the descriptors to mutate the filenames in referenced modules.
    let mod_list: minidump::MinidumpModuleList = dump
        .get_stream()
        .map_err(|e| e.compat())
        .context("Failed to parse modules from minidump")?;
    let mut file_name_descriptors: Vec<UTF16Descriptor> = Vec::new();
    for module in mod_list.iter() {
        let name_size: u32 = data.pread_with(module.raw.module_name_rva as usize, dump.endian)?;
        file_name_descriptors.push(UTF16Descriptor {
            rva: module.raw.module_name_rva + std::mem::size_of::<u32>() as u32,
            size: name_size,
        });
        // TODO: add debug names
        // TODO: deduplicate descriptors.  E.g. a code and debug name could both point to
        // the same memory.
    }

    // Time to modify things!

    for mem_desc in mem_descriptors {
        let kind = if stack_rvas.contains(&mem_desc.memory.rva) {
            MemoryKind::Stack
        } else {
            MemoryKind::NonStack
        };
        let range = std::ops::Range {
            start: mem_desc.memory.rva as usize,
            end: (mem_desc.memory.rva + mem_desc.memory.data_size) as usize,
        };
        let dest = &mut data[range];
        scrub_memory_region(kind, dest);
    }

    for desc in file_name_descriptors {
        let range = std::ops::Range {
            start: desc.rva as usize,
            end: (desc.rva + desc.size) as usize,
        };
        scrub_code_name(&mut data[range]);
    }

    std::fs::write(argv.output, data)?;

    Ok(())
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
struct UTF16Descriptor {
    rva: u32,
    size: u32,
}

enum MemoryKind {
    /// Stack memory.
    Stack,
    /// All other memory, likely heap memory.
    NonStack,
}

fn scrub_memory_region(kind: MemoryKind, data: &mut [u8]) {
    match kind {
        MemoryKind::Stack => (),
        MemoryKind::NonStack => null_data(data),
    }
}

/// Modify a module's code name
///
/// The `data` is a UTF16LE encoded string.  Any modifications must keep it valid and can
/// not change the size of it.
// TODO: should really type-enforce this.
fn scrub_code_name(data: &mut [u8]) {
    assert!(data.len() % 2 == 0);
    let letter = b"a\x00"; // 'a' in UTF16LE
    let mut new: Vec<u8> = Vec::new();
    for _ in data.chunks(2) {
        new.push(letter[0]);
        new.push(letter[1]);
    }
    assert_eq!(new.len(), data.len());
    data.copy_from_slice(new.as_slice());
}

/// Zero-out all data in the slice.
fn null_data(data: &mut [u8]) {
    let nulls = vec![0 as u8; data.len()];
    data.copy_from_slice(nulls.as_slice());
}
