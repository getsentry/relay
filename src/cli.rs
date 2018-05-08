use failure::{err_msg, Error};
use clap::{App, AppSettings, Arg, ArgMatches};

use smith_server;
use smith_config::Config;

use setup;

pub const VERSION: &'static str = env!("CARGO_PKG_VERSION");
pub const ABOUT: &'static str = "Runs a sentry-relay (fancy proxy server)";

fn make_app<'a, 'b>() -> App<'a, 'b> {
    App::new("sentry-relay")
        .setting(AppSettings::UnifiedHelpMessage)
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .help_message("Print this help message.")
        .version(VERSION)
        .version_message("Print version information.")
        .about(ABOUT)
        .arg(
            Arg::with_name("config")
                .value_name("CONFIG")
                .long("config")
                .short("c")
                .global(true)
                .help("The path to the config folder."),
        )
        .subcommand(App::new("run").about("Run the relay"))
        .subcommand(
            App::new("generate-auth")
                .about("Write new authentication info into the config")
                .arg(
                    Arg::with_name("overwrite")
                        .long("overwrite")
                        .help("Overwrite already existing credentials instead of failing"),
                ),
        )
        .subcommand(App::new("init").about("Initialize a new relay config"))
}

/// Runs the command line application.
pub fn execute() -> Result<(), Error> {
    let app = make_app();
    let matches = app.get_matches();
    let config = Config::from_path(&matches.value_of("config").unwrap_or(".sentry-relay"))?;

    setup::init_logging(&config);

    if let Some(matches) = matches.subcommand_matches("generate-auth") {
        generate_auth(config, &matches)
    } else if let Some(matches) = matches.subcommand_matches("init") {
        init(config, &matches)
    } else if let Some(matches) = matches.subcommand_matches("run") {
        run(config, &matches)
    } else {
        unreachable!();
    }
}

pub fn generate_auth<'a>(mut config: Config, matches: &ArgMatches<'a>) -> Result<(), Error> {
    if config.has_credentials() && !matches.is_present("overwrite") {
        return Err(err_msg(
            "aborting because credentials already exist. Pass --overwrite to force.",
        ));
    }
    config.regenerate_credentials()?;
    setup::dump_credentials(&config);
    Ok(())
}

pub fn init<'a>(_config: Config, _matches: &ArgMatches<'a>) -> Result<(), Error> {
    Ok(())
}

pub fn run<'a>(mut config: Config, _matches: &ArgMatches<'a>) -> Result<(), Error> {
    // in all other cases we make new credentials if there are none yet
    // and spawn the server (this is both `run`
    if !config.has_credentials() {
        config.regenerate_credentials()?;
    }

    setup::dump_spawn_infos(&config);
    setup::init_metrics(&config)?;

    smith_server::run(config)?;

    Ok(())
}
