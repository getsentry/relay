#![doc(
    html_logo_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png",
    html_favicon_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png"
)]

use std::fs;
use std::path::PathBuf;

use anyhow::{Context, Result, format_err};
use clap::Parser;
use relay_pii::{PiiAttachmentsProcessor, PiiConfig};

/// Apply data scrubbing (PII) rules on a minidump file.
///
/// Remove all heap memory:
///
///     {"applications": {"$heap_memory": ["@anything:remove"]}}
///
/// Remove all memory regions:
///
///     {"applications": {"$stack_memory || $heap_memory": ["@anything:remove"]}}
///
/// Remove credit cards from heap memory:
///
///     {"applications": {"$heap_memory": ["@creditcard:remove"]}}
///
/// For more information on how to scrub IP addresses, user file paths and how to define custom
/// regexes see <https://getsentry.github.io/relay/pii-config/>
#[derive(Debug, Parser)]
#[structopt(verbatim_doc_comment)]
struct Cli {
    /// Path to a PII config JSON file.
    #[arg(short, long)]
    config: PathBuf,

    /// Path to the minidump to rewrite.
    minidump: PathBuf,

    /// Optional output path. By default, the minidump file is overwritten.
    #[arg(short, long)]
    output: Option<PathBuf>,
}

impl Cli {
    fn load_pii_config(&self) -> Result<PiiConfig> {
        let json = fs::read_to_string(&self.config).with_context(|| "failed to read PII config")?;
        let config = serde_json::from_str(&json).with_context(|| "failed to parse PII config")?;
        Ok(config)
    }

    fn load_minidump(&self) -> Result<Vec<u8>> {
        let buf = fs::read(&self.minidump).with_context(|| "failed to open minidump")?;
        Ok(buf)
    }

    fn minidump_name(&self) -> &str {
        self.minidump
            .file_name()
            .and_then(|os_str| os_str.to_str())
            .unwrap_or_default()
    }

    fn write_output(&self, data: &[u8]) -> Result<()> {
        let path = match self.output {
            Some(ref output) => output,
            None => &self.minidump,
        };

        fs::write(path, data)
            .with_context(|| format!("failed to write minidump to {}", path.display()))?;

        println!("output written to {}", path.display());

        Ok(())
    }

    pub fn run(self) -> Result<()> {
        let config = self.load_pii_config()?;
        let processor = PiiAttachmentsProcessor::new(config.compiled());

        let mut data = self.load_minidump()?;
        let changed = processor
            .scrub_minidump(self.minidump_name(), &mut data)
            .map_err(|e| format_err!("{e}"))?; // does not implement std::error::Error

        if changed {
            self.write_output(&data)?;
        } else {
            println!("nothing changed.");
        }

        Ok(())
    }
}

fn print_error(error: &anyhow::Error) {
    eprintln!("Error: {error}");

    let mut cause = error.source();
    while let Some(ref e) = cause {
        eprintln!("  caused by: {e}");
        cause = e.source();
    }
}

fn main() {
    let cli = Cli::parse();

    match cli.run() {
        Ok(()) => (),
        Err(error) => {
            print_error(&error);
            std::process::exit(1);
        }
    }
}
