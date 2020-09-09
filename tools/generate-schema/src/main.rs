use std::fmt;
use std::fs::File;
use std::io;
use std::path::PathBuf;

use anyhow::Result;
use structopt::clap::AppSettings;
use structopt::StructOpt;

#[derive(Clone, Copy, Debug)]
enum SchemaFormat {
    JsonSchema,
}

impl fmt::Display for SchemaFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::JsonSchema => write!(f, "jsonschema"),
        }
    }
}

#[derive(Debug)]
struct ParseSchemaFormatError;

impl fmt::Display for ParseSchemaFormatError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid schema format")
    }
}

impl std::error::Error for ParseSchemaFormatError {}

impl std::str::FromStr for SchemaFormat {
    type Err = ParseSchemaFormatError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "jsonschema" => Ok(Self::JsonSchema),
            _ => Err(ParseSchemaFormatError),
        }
    }
}

/// Prints the event protocol schema.
#[derive(Debug, StructOpt)]
#[structopt(verbatim_doc_comment, setting = AppSettings::ColoredHelp)]
struct Cli {
    /// The format to output the schema in.
    #[structopt(short, long, default_value = "jsonschema")]
    format: SchemaFormat,

    /// Optional output path. By default, the schema is printed on stdout.
    #[structopt(short, long, value_name = "PATH")]
    output: Option<PathBuf>,
}

impl Cli {
    pub fn run(self) -> Result<()> {
        let schema = relay_general::protocol::event_json_schema();

        match self.output {
            Some(path) => serde_json::to_writer_pretty(File::create(path)?, &schema)?,
            None => serde_json::to_writer_pretty(io::stdout(), &schema)?,
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
