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
    println!("{:?}", dump.header.version);

    let thread_list: minidump::MinidumpThreadList = dump.get_stream().map_err(|e| e.compat())?;
    let stack_rvas: Vec<minidump::format::RVA> = thread_list
        .threads
        .iter()
        .map(|t| t.raw.stack.memory.rva)
        .collect();

    let mem_list: minidump::MinidumpMemoryList = dump.get_stream().map_err(|e| e.compat())?;
    let mem_descriptors: Vec<minidump::format::MINIDUMP_MEMORY_DESCRIPTOR> = mem_list
        .iter()
        .filter(|mem| !stack_rvas.contains(&mem.desc.memory.rva))
        .map(|mem| mem.desc)
        .collect();
    let mut count = 0;
    for mem_desc in mem_descriptors {
        count += 1;
        let nulls = vec![0 as u8; mem_desc.memory.data_size as usize];
        let range: std::ops::Range<usize> = std::ops::Range {
            start: mem_desc.memory.rva as usize,
            end: (mem_desc.memory.rva + mem_desc.memory.data_size) as usize,
        };
        let dest = &mut data[range];
        dest.copy_from_slice(nulls.as_slice());
    }
    println!("Modified {} memory regions", count);

    std::fs::write(argv.output, data)?;

    Ok(())
}
