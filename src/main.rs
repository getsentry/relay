extern crate clap;
extern crate ctrlc;
extern crate failure;
extern crate futures;
#[macro_use]
extern crate log;
extern crate parking_lot;
extern crate pretty_env_logger;
extern crate sentry;

extern crate smith_config;
extern crate smith_server;

use std::env;

use log::LevelFilter;
use failure::Error;
use clap::{App, AppSettings, Arg};

use smith_config::Config;

pub const VERSION: &'static str = env!("CARGO_PKG_VERSION");
pub const ABOUT: &'static str = "Runs a sentry-relay (fancy proxy server)";

fn init_logging(config: &Config) {
    if config.enable_backtraces() {
        env::set_var("RUST_BACKTRACE", "1");
    }

    if env::var("RUST_LOG").is_err() {
        env::set_var(
            "RUST_LOG",
            match config.log_level_filter() {
                LevelFilter::Off => "",
                LevelFilter::Error => "ERROR",
                LevelFilter::Warn => "WARN",
                LevelFilter::Info => "INFO",
                LevelFilter::Debug => {
                    "INFO,\
                     actix_web::pipeline=DEBUG,\
                     smith_common=DEBUG,\
                     smith_aorta=DEBUG,\
                     smith_config=DEBUG,\
                     smith_common=DEBUG,\
                     smith_server=DEBUG,\
                     smith_trove=DEBUG,\
                     sentry_relay=DEBUG"
                }
                LevelFilter::Trace => "TRACE",
            },
        );
    }

    let mut log_builder = pretty_env_logger::formatted_builder().unwrap();
    match env::var("RUST_LOG") {
        Ok(rust_log) => log_builder.parse(&rust_log),
        Err(_) => log_builder.filter_level(config.log_level_filter()),
    };

    sentry::integrations::log::init(Some(Box::new(log_builder.build())), Default::default());
}

fn dump_spawn_infos(config: &Config) {
    info!(
        "launching relay with config {}",
        config.filename().display()
    );
    info!("  relay id: {}", config.relay_id());
    info!("  public key: {}", config.public_key());
    info!("  listening on http://{}/", config.listen_addr());
    info!("  log level: {}", config.log_level_filter());
}

pub fn execute() -> Result<(), Error> {
    let app = App::new("sentry-relay")
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
    sentry::init(config.sentry_dsn());

    init_logging(&config);

    // upon loading the config can be initialized.  In that case it will be
    // modified and we want to write it back automatically for now.
    if config.changed() {
        config.save()?;
    }

    dump_spawn_infos(&config);
    smith_server::run(config)?;

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
