use std::path::PathBuf;

use anyhow::{Context, Result};
use failure::Fail;
use scroll::Pread;
use structopt::clap::AppSettings;
use structopt::StructOpt;

use relay_general::pii::{AttachmentBytesType, PiiAttachmentsProcessor, PiiConfig};

/// Apply data scrubbing (PII) rules on a minidump file.
///
///     edit_minidump --pii-config config.json input.dmp output.dmp
///
/// Remove all non-stack memory:
///
///     {"applications": {"$memory": ["@anything:remove"]}}
///
/// Remove stack + non-stack memory:
///
///     {"applications": {"$stackmemory": ["@anything:remove"]}}
///
/// Remove creditcards from non-stack memory:
///
///     {"applications": {"$memory": ["@creditcard:remove"]}}
///
/// For more information on how to scrub IP addresses, user file paths and how to define custom
/// regexes see https://getsentry.github.io/relay/pii-config/
#[derive(Debug, StructOpt)]
#[structopt(setting = AppSettings::ColoredHelp)]
#[structopt(verbatim_doc_comment)]
struct CliArgs {
    /// Path to where to read the PII config from
    #[structopt(long = "pii-config", value_name = "PATH")]
    pii_config: PathBuf,
    /// Path to the minidump to rewrite.
    input: PathBuf,
    /// Path to where to write the re-written minidump
    output: PathBuf,
}

#[paw::main]
fn main(argv: CliArgs) -> Result<()> {
    let pii_config_raw =
        std::fs::read_to_string(argv.pii_config).context("failed to read PII config")?;
    let pii_config = PiiConfig::from_json(&pii_config_raw).context("failed to parse PII config")?;
    let pii_config_compiled = pii_config.compiled();
    let pii_processor = PiiAttachmentsProcessor::new(&pii_config_compiled);

    let mut data = std::fs::read(&argv.input).context("failed to read minidump")?;
    let filename = argv.input.to_string_lossy();

    let dump = minidump::Minidump::read(data.as_slice())
        .map_err(|e| e.compat())
        .context("Failed to parse minidump")?;

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

    let mut changed = false;

    for mem_desc in mem_descriptors {
        let bytes_type = if stack_rvas.contains(&mem_desc.memory.rva) {
            AttachmentBytesType::MinidumpStack
        } else {
            AttachmentBytesType::MinidumpHeap
        };
        let range = std::ops::Range {
            start: mem_desc.memory.rva as usize,
            end: (mem_desc.memory.rva + mem_desc.memory.data_size) as usize,
        };
        let dest = &mut data[range];
        assert!(dest.len() > 0);

        changed |= pii_processor.scrub_attachment_bytes(&filename, dest, bytes_type);
    }

    //for desc in file_name_descriptors {
    //let range = std::ops::Range {
    //start: desc.rva as usize,
    //end: (desc.rva + desc.size) as usize,
    //};

    //// TODO: modify `changed`
    //scrub_code_name(&mut data[range]);
    /*}*/

    if changed {
        std::fs::write(&argv.output, data)?;
        println!("Minidump changed, wrote new file to {:?}", &argv.output);
    } else {
        println!("Did not change minidump.");
    }

    Ok(())
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
struct UTF16Descriptor {
    rva: u32,
    size: u32,
}

///// Modify a module's code name
/////
///// The `data` is a UTF16LE encoded string.  Any modifications must keep it valid and can
///// not change the size of it.
//// TODO: should really type-enforce this.
//fn scrub_code_name(data: &mut [u8]) {
//assert!(data.len() % 2 == 0);
//let letter = b"a\x00"; // 'a' in UTF16LE
//let mut new: Vec<u8> = Vec::new();
//for _ in data.chunks(2) {
//new.push(letter[0]);
//new.push(letter[1]);
//}
//assert_eq!(new.len(), data.len());
//data.copy_from_slice(new.as_slice());
/*}*/
