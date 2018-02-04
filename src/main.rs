extern crate clap;
extern crate failure;
extern crate pretty_env_logger;
extern crate smith_config;
extern crate smith_server;

use std::env;
use failure::Error;
use clap::{App, AppSettings, Arg};

use smith_config::Config;

pub const VERSION: &'static str = env!("CARGO_PKG_VERSION");
pub const ABOUT: &'static str = "Runs a sentry-agent (fancy proxy server)";

fn init_logging(config: &Config) {
    if env::var("RUST_LOG").is_err() {
        env::set_var("RUST_LOG", config.log_level_filter().to_string());
    }
    pretty_env_logger::init();
}

pub fn execute() -> Result<(), Error> {
    let app = App::new("sentry-agent")
        .setting(AppSettings::UnifiedHelpMessage)
        .help_message("Print this help message.")
        .version(VERSION)
        .version_message("Print version information.")
        .about(ABOUT)
        .arg(
            Arg::with_name("config")
                .value_name("CONFIG")
                .long("config")
                .short("c")
                .required(true)
                .help("The path to the config file."),
        );

    let matches = app.get_matches();

    let mut config = Config::open(matches.value_of("config").unwrap())?;
    init_logging(&config);

    // upon loading the config can be initialized.  In that case it will be
    // modified and we want to write it back automatically for now.
    if config.changed() {
        config.save()?;
    }

    smith_server::run(&config)?;
    Ok(())
}

pub fn main() {
    if let Err(err) = execute() {
        println!("error: {}", err);
        for cause in err.causes().skip(1) {
            println!("  caused by: {}", cause);
        }
        match env::var("RUST_BACKTRACE").as_ref().map(|x| x.as_str()) {
            Ok("1") | Ok("full") => {
                let bt = err.backtrace();
                println!("");
                println!("{}", bt);
            }
            _ => if cfg!(debug_assertions) {
                println!("");
                println!("hint: you can set RUST_BACKTRACE=1 to get the entire backtrace.");
            },
        }
    }
}
