#![doc(
    html_logo_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png",
    html_favicon_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png"
)]

use std::fs;
use std::io::{self, Read};
use std::path::PathBuf;

use anyhow::{Context, Result, format_err};
use clap::Parser;
use relay_event_normalization::{
    EventValidationConfig, NormalizationConfig, normalize_event, validate_event,
};
use relay_event_schema::processor::{ProcessingState, process_value};
use relay_event_schema::protocol::Event;
use relay_pii::{ENCRYPTED_PII_KEY, EncryptProcessor, PiiConfig, PiiProcessor, generate_keypair};
use relay_protocol::{Annotated, Value};

/// Processes a Sentry event payload.
///
/// This command takes a JSON event payload on stdin and write the processed event payload to
/// stdout. Optionally, an additional PII config can be supplied.
#[derive(Debug, Parser)]
#[structopt(verbatim_doc_comment)]
struct Cli {
    /// Path to a PII processing config JSON file.
    #[arg(short = 'c', long)]
    pii_config: Option<PathBuf>,

    /// Path to an event payload JSON file (defaults to stdin).
    #[arg(short, long)]
    event: Option<PathBuf>,

    /// Apply full store normalization.
    #[arg(long)]
    store: bool,

    /// Pretty print the output JSON.
    #[arg(long, conflicts_with = "debug")]
    pretty: bool,

    /// Debug print the internal structure.
    #[arg(long)]
    debug: bool,

    /// Generate an X25519 keypair for `encrypt` PII rules and exit.
    ///
    /// The public half goes into the PII config under `vars.publicKey`; keep the secret half to
    /// decrypt with.
    #[arg(long)]
    keygen: bool,
}

impl Cli {
    fn load_pii_config(&self) -> Result<Option<PiiConfig>> {
        let path = match self.pii_config {
            Some(ref path) => path,
            None => return Ok(None),
        };

        let json = fs::read_to_string(path).with_context(|| "failed to read PII config")?;
        let config = serde_json::from_str(&json).with_context(|| "failed to parse PII config")?;
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
        if self.keygen {
            let (public_key, secret_key) = generate_keypair();
            eprintln!("secret key (keep private, needed to decrypt): {secret_key}");
            println!("{}", serde_json::json!({"vars": {"publicKey": public_key}}));
            return Ok(());
        }

        let mut event = self.load_event()?;

        if let Some(pii_config) = self.load_pii_config()? {
            let compiled = pii_config.compiled();

            // Mirror the server: collect the originals first, scrub, then attach the sealed payload
            // so no rule can strip it.
            let sealed = if EncryptProcessor::is_enabled(compiled) {
                let mut processor = EncryptProcessor::new(compiled);
                process_value(&mut event, &mut processor, ProcessingState::root())
                    .map_err(|e| format_err!("{e}"))?;
                processor.seal().context("failed to encrypt PII values")?
            } else {
                None
            };

            let mut processor = PiiProcessor::new(compiled);
            process_value(&mut event, &mut processor, ProcessingState::root())
                .map_err(|e| format_err!("{e}"))?;

            if let (Some(sealed), Some(event)) = (sealed, event.value_mut()) {
                event.other.insert(
                    ENCRYPTED_PII_KEY.to_owned(),
                    Annotated::new(Value::String(sealed)),
                );
            }
        }

        if self.store {
            validate_event(&mut event, &EventValidationConfig::default())
                .map_err(|e| format_err!("{e}"))?;
            normalize_event(&mut event, &NormalizationConfig::default());
        }

        if self.debug {
            println!("{event:#?}");
        } else if self.pretty {
            println!("{}", event.to_json_pretty()?);
        } else {
            println!("{}", event.to_json()?);
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
