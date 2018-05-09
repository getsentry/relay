use std::env;
use std::path::{Path, PathBuf};

use failure::{err_msg, Error};
use clap::{App, AppSettings, Arg, ArgMatches};
use dialoguer::{Confirmation, Select};

use semaphore_server;
use semaphore_config::{Config, MinimalConfig};

use setup;
use utils;

pub const VERSION: &'static str = env!("CARGO_PKG_VERSION");
pub const ABOUT: &'static str = "Semaphore is an implementation of the relay system for Sentry.";

fn make_app<'a, 'b>() -> App<'a, 'b> {
    App::new("semaphore")
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
        .subcommand(
            App::new("dump-config")
                .about("Dump the entire config out for debugging purposes")
                .arg(
                    Arg::with_name("format")
                        .short("f")
                        .long("format")
                        .possible_values(&["debug", "json", "yaml"])
                        .default_value("debug")
                        .help("The output format"),
                ),
        )
}

/// Runs the command line application.
pub fn execute() -> Result<(), Error> {
    let app = make_app();
    let matches = app.get_matches();
    let config_path = matches.value_of("config").unwrap_or(".semaphore");

    // init is special because it does not yet have a config.
    if let Some(matches) = matches.subcommand_matches("init") {
        return init(&config_path, &matches);
    }

    let config = Config::from_path(&config_path)?;
    setup::init_logging(&config);
    if let Some(matches) = matches.subcommand_matches("generate-auth") {
        generate_auth(config, &matches)
    } else if let Some(matches) = matches.subcommand_matches("run") {
        run(config, &matches)
    } else if let Some(matches) = matches.subcommand_matches("dump-config") {
        dump_config(config, &matches)
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

pub fn init<'a, P: AsRef<Path>>(config_path: P, _matches: &ArgMatches<'a>) -> Result<(), Error> {
    let mut done_something = false;
    let config_path = env::current_dir()?.join(config_path.as_ref());
    println!("Initializing relay in {}", config_path.display());

    if !Config::config_exists(&config_path) {
        println!("There is no relay config yet. Do you want to create one?");
        let item = Select::new()
            .default(0)
            .item("yes, create default config")
            .item("yes, create custom config")
            .item("no, abort")
            .interact()?;

        let with_prompts = match item {
            0 => false,
            1 => true,
            2 => return Ok(()),
            _ => unreachable!(),
        };

        let mut mincfg: MinimalConfig = Default::default();
        if with_prompts {
            utils::prompt_value("upstream", &mut mincfg.relay.upstream)?;
            utils::prompt_value("listen interface", &mut mincfg.relay.host)?;
            utils::prompt_value("listen port", &mut mincfg.relay.port)?;

            if Confirmation::new("do you want to configure TLS").interact()? {
                let mut port = mincfg.relay.port.saturating_add(443);
                utils::prompt_value("tls port", &mut port)?;
                mincfg.relay.tls_port = Some(port);
                let mut path = None::<String>;
                utils::prompt_value_no_default("tls private key path", &mut path)?;
                mincfg.relay.tls_private_key = Some(PathBuf::from(path.unwrap()));
                let mut path = None::<String>;
                utils::prompt_value_no_default("tls certificate path", &mut path)?;
                mincfg.relay.tls_cert = Some(PathBuf::from(path.unwrap()));
            }
        }

        mincfg.save_in_folder(&config_path)?;
        done_something = true;
    }

    let mut config = Config::from_path(&config_path)?;
    if !config.has_credentials() {
        println!("Generating credentials ...");
        config.regenerate_credentials()?;
        done_something = true;
    }

    if done_something {
        println!("All done!");
    } else {
        println!("Nothing to do.");
    }

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

    semaphore_server::run(config)?;

    Ok(())
}

pub fn dump_config<'a>(config: Config, matches: &ArgMatches<'a>) -> Result<(), Error> {
    match matches.value_of("format").unwrap() {
        "debug" => println!("{:#?}", &config),
        "json" => println!("{}", config.to_json_string()?),
        "yaml" => println!("{}", config.to_yaml_string()?),
        _ => unreachable!(),
    }
    Ok(())
}
