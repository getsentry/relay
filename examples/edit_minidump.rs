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
    println!("{:?}", dump.header.version);

    let mem_list_raw: &[u8] = dump
        .get_raw_stream(minidump::format::MINIDUMP_STREAM_TYPE::MemoryListStream)
        .map_err(|e| e.compat())?;
    println!("{:?}", mem_list_raw);

    let mut offset = 0;
    let descriptors: Vec<minidump::format::MINIDUMP_MEMORY_DESCRIPTOR> =
        read_stream_list(&mut offset, mem_list_raw, dump.endian).map_err(|e| e.compat())?;
    // println!("{:?}", descriptors);

    // Time to modify the data
    for location in descriptors {
        let nulls = vec![0 as u8; location.memory.data_size as usize];
        let range: std::ops::Range<usize> = std::ops::Range {
            start: location.memory.rva as usize,
            end: (location.memory.rva + location.memory.data_size) as usize,
        };
        let dest = &mut data[range];
        dest.copy_from_slice(nulls.as_slice());
    }

    std::fs::write(argv.output, data)?;

    Ok(())
}

fn read_stream_list<'a, T>(
    offset: &mut usize,
    bytes: &'a [u8],
    endian: scroll::Endian,
) -> Result<Vec<T>, minidump::Error>
where
    T: scroll::ctx::TryFromCtx<'a, scroll::Endian, [u8], Error = scroll::Error, Size = usize>,
    T: scroll::ctx::SizeWith<scroll::Endian, Units = usize>,
{
    let u: u32 = bytes
        .gread_with(offset, endian)
        .or(Err(minidump::Error::StreamReadFailure))?;
    let count = u as usize;
    let counted_size = match count
        .checked_mul(<T>::size_with(&endian))
        .and_then(|v| v.checked_add(std::mem::size_of::<u32>()))
    {
        Some(s) => s,
        None => return Err(minidump::Error::StreamReadFailure),
    };
    if bytes.len() < counted_size {
        return Err(minidump::Error::StreamSizeMismatch {
            expected: counted_size,
            actual: bytes.len(),
        });
    }
    match bytes.len() - counted_size {
        0 => {}
        4 => {
            // 4 bytes of padding.
            *offset += 4;
        }
        _ => {
            return Err(minidump::Error::StreamSizeMismatch {
                expected: counted_size,
                actual: bytes.len(),
            })
        }
    };
    // read count T raw stream entries
    let mut raw_entries = Vec::with_capacity(count);
    for _ in 0..count {
        let raw: T = bytes
            .gread_with(offset, endian)
            .or(Err(minidump::Error::StreamReadFailure))?;
        raw_entries.push(raw);
    }
    Ok(raw_entries)
}
