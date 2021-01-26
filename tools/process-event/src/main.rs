use std::fs;
use std::io::{self, Read};
use std::path::PathBuf;

use relay_general::pii::{PiiConfig, PiiProcessor};
use relay_general::processor::{process_value, ProcessingState};
use relay_general::protocol::Event;
use relay_general::store::{StoreConfig, StoreProcessor};
use relay_general::types::Annotated;

use anyhow::{format_err, Context, Result};
use structopt::clap::AppSettings;
use structopt::StructOpt;

/// Processes a Sentry event payload.
///
/// This command takes a JSON event payload on stdin and write the processed event payload to
/// stdout. Optionally, an additional PII config can be supplied.
#[derive(Debug, StructOpt)]
#[structopt(verbatim_doc_comment, setting = AppSettings::ColoredHelp)]
struct Cli {
    /// Path to a PII processing config JSON file.
    #[structopt(short = "c", long)]
    pii_config: Option<PathBuf>,

    /// Path to an event payload JSON file (defaults to stdin).
    #[structopt(short, long)]
    event: Option<PathBuf>,

    /// Apply full store normalization.
    #[structopt(long)]
    store: bool,

    /// Pretty print the output JSON.
    #[structopt(long, conflicts_with = "debug")]
    pretty: bool,

    /// Debug print the internal structure.
    #[structopt(long)]
    debug: bool,
}

impl Cli {
    fn load_pii_config(&self) -> Result<Option<PiiConfig>> {
        let path = match self.pii_config {
            Some(ref path) => path,
            None => return Ok(None),
        };

        let json = fs::read_to_string(path).with_context(|| "failed to read PII config")?;
        let config = PiiConfig::from_json(&json).with_context(|| "failed to parse PII config")?;
        Ok(Some(config))
    }

    fn load_event(&self) -> Result<Annotated<Event>> {
        let json = match self.event {
            Some(ref path) => fs::read_to_string(path).with_context(|| "failed to read event")?,
            None => {
                let mut json = String::new();
                io::stdin()
                    .read_to_string(&mut json)
                    .with_context(|| "failed to read event")?;
                json
            }
        };

        let event = Annotated::from_json(&json).with_context(|| "failed to parse event")?;
        Ok(event)
    }

    pub fn run(self) -> Result<()> {
        let mut event = self.load_event()?;

        if let Some(pii_config) = self.load_pii_config()? {
            let compiled = pii_config.compiled();
            let mut processor = PiiProcessor::new(&compiled);
            process_value(&mut event, &mut processor, ProcessingState::root())
                .map_err(|e| format_err!("{}", e))?;
        }

        if self.store {
            let mut processor = StoreProcessor::new(StoreConfig::default(), None);
            process_value(&mut event, &mut processor, ProcessingState::root())
                .map_err(|e| format_err!("{}", e))
                .with_context(|| "failed to store process event")?;
        }

        if self.debug {
            println!("{:#?}", event);
        } else if self.pretty {
            println!("{}", event.to_json_pretty()?);
        } else {
            println!("{}", event.to_json()?);
        }

        Ok(())
    }
}

fn print_error(error: &anyhow::Error) {
    eprintln!("Error: {}", error);

    let mut cause = error.source();
    while let Some(ref e) = cause {
        eprintln!("  caused by: {}", e);
        cause = e.source();
    }
}

#[paw::main]
fn main(cli: Cli) {
    match cli.run() {
        Ok(()) => (),
        Err(error) => {
            print_error(&error);
            std::process::exit(1);
        }
    }
}
