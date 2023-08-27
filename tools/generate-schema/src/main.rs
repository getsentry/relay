#![doc(
    html_logo_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png",
    html_favicon_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png"
)]

use std::fs::File;
use std::io;
use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, ValueEnum};

#[derive(Clone, Copy, Debug, ValueEnum)]
#[value(rename_all = "lowercase")]
enum SchemaFormat {
    JsonSchema,
}

/// Prints the event protocol schema.
#[derive(Debug, Parser)]
#[command(verbatim_doc_comment)]
struct Cli {
    /// The format to output the schema in.
    #[arg(value_enum, short, long, default_value = "jsonschema")]
    format: SchemaFormat,

    /// Optional output path. By default, the schema is printed on stdout.
    #[arg(short, long)]
    output: Option<PathBuf>,
}

impl Cli {
    pub fn run(self) -> Result<()> {
        match self.format {
            SchemaFormat::JsonSchema => {
                let schema = relay_event_schema::protocol::event_json_schema();

                match self.output {
                    Some(path) => serde_json::to_writer_pretty(File::create(path)?, &schema)?,
                    None => serde_json::to_writer_pretty(io::stdout(), &schema)?,
                }
            }
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
