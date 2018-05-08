use failure::Error;
use clap::{App, AppSettings, Arg};

use smith_server;
use smith_config::Config;

use setup;

pub const VERSION: &'static str = env!("CARGO_PKG_VERSION");
pub const ABOUT: &'static str = "Runs a sentry-relay (fancy proxy server)";

fn make_app<'a, 'b>() -> App<'a, 'b> {
    App::new("sentry-relay")
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
                .help("The path to the config folder."),
        )
        .subcommand(App::new("run").about("Run the relay"))
        .subcommand(
            App::new("generate-auth").about("Write new authentication info into the config"),
        )
        .subcommand(App::new("init-config").about("Initialize a new agent config"))
}

/// Runs the command line application.
pub fn execute() -> Result<(), Error> {
    let app = make_app();
    let matches = app.get_matches();
    let mut config = Config::from_path(&matches.value_of("config").unwrap())?;

    if !config.has_credentials() {
        config.regenerate_credentials()?;
    }

    run(config)
}

pub fn run(config: Config) -> Result<(), Error> {
    setup::init_logging(&config);
    setup::dump_spawn_infos(&config);
    setup::init_metrics(&config)?;

    smith_server::run(config)?;

    Ok(())
}
