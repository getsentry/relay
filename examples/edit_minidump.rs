use std::path::PathBuf;

use anyhow::{Context, Result};
use failure::Fail;
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

    // We would like to modify `data` directly, `mem_list` however refers to data
    // non-mutably so we can not directly iterate over the `mem_list` and collect the memory
    // descriptors instead.
    let mem_list: minidump::MinidumpMemoryList = dump
        .get_stream()
        .map_err(|e| e.compat())
        .context("Failed to parse memory regions from minidump")?;
    let mem_descriptors: Vec<minidump::format::MINIDUMP_MEMORY_DESCRIPTOR> =
        mem_list.iter().map(|mem| mem.desc).collect();

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

    std::fs::write(argv.output, data)?;

    Ok(())
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

/// Zero-out all data in the slice.
fn null_data(data: &mut [u8]) {
    let nulls = vec![0 as u8; data.len()];
    data.copy_from_slice(nulls.as_slice());
}
