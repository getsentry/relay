use std::env;
use std::path::{Path, PathBuf};

use clap::{App, AppSettings, Arg, ArgMatches};
use dialoguer::{Confirmation, Select};
use failure::{err_msg, Error};
use uuid::Uuid;

use semaphore_config::{Config, Credentials, MinimalConfig};
use semaphore_server;

use setup;
use utils;

pub const VERSION: &'static str = env!("CARGO_PKG_VERSION");
pub const ABOUT: &'static str = "Semaphore is an implementation of the relay system for Sentry.";

fn make_app<'a, 'b>() -> App<'a, 'b> {
    App::new("semaphore")
        .global_setting(AppSettings::UnifiedHelpMessage)
        .global_setting(AppSettings::DisableHelpSubcommand)
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .setting(AppSettings::GlobalVersion)
        .max_term_width(79)
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
            App::new("credentials")
                .setting(AppSettings::SubcommandRequiredElseHelp)
                .about("Manage the relay credentials")
                .after_help(
                    "This command can be used to manage the stored credentials of \
                     the relay.  These credentials are used to authenticate with the \
                     upstream sentry.  A sentry organization trusts a certain public \
                     key and each relay is identified with a unique relay ID.\n\
                     \n\
                     Multiple relays can share the same public/secret key pair for as \
                     long as they use different relay IDs.  Once a relay (as identified \
                     by the ID) has signed in with a certain key it cannot be changed \
                     any more.",
                )
                .subcommand(
                    App::new("generate")
                        .about("Generate new credentials")
                        .after_help(
                            "This generates new credentials for the relay and stores \
                             them.  In case the relay already has credentials stored \
                             this command will error unless the '--overwrite' option \
                             has been passed.",
                        )
                        .arg(
                            Arg::with_name("overwrite")
                                .long("overwrite")
                                .help("Overwrite already existing credentials instead of failing"),
                        ),
                )
                .subcommand(
                    App::new("remove")
                        .about("Remove credentials")
                        .after_help(
                            "This command removes already stored credentials from the \
                             relay.",
                        )
                        .arg(
                            Arg::with_name("yes")
                                .long("yes")
                                .help("Do not prompt for confirmation"),
                        ),
                )
                .subcommand(
                    App::new("show")
                        .about("Show currently stored credentials.")
                        .after_help("This prints out the agent ID and public key."),
                )
                .subcommand(
                    App::new("set")
                        .about("Set new credentials")
                        .after_help(
                            "Credentials can be stored by providing them on the command \
                             line.  If just an agent id (or secret/public key pair) is \
                             provided that part of the credentials are overwritten.  If \
                             no credentials are stored yet at all and no parameters are \
                             supplied the command will prompt for the appropriate values.",
                        )
                        .arg(
                            Arg::with_name("secret_key")
                                .long("secret-key")
                                .short("s")
                                .value_name("KEY")
                                .requires("public_key")
                                .help("The secret key to set"),
                        )
                        .arg(
                            Arg::with_name("public_key")
                                .long("public-key")
                                .short("p")
                                .value_name("KEY")
                                .requires("secret_key")
                                .help("The public key to set"),
                        )
                        .arg(
                            Arg::with_name("id")
                                .long("id")
                                .short("i")
                                .value_name("RELAY_ID")
                                .help("The relay ID to set"),
                        ),
                ),
        )
        .subcommand(
            App::new("config")
                .about("Manage the relay config")
                .after_help(
                    "This command provides basic config management.  It can be \
                     used primarily to initialize a new relay config and to \
                     print out the current config.",
                )
                .setting(AppSettings::SubcommandRequiredElseHelp)
                .subcommand(
                    App::new("init")
                        .about("Initialize a new relay config")
                        .after_help(
                            "For new relay installations this will guide through \
                             the initial config process and create the necessary \
                             files.  It will create an initial config as well as \
                             set of credentials.",
                        ),
                )
                .subcommand(
                    App::new("show")
                        .about("Show the entire config out for debugging purposes")
                        .after_help(
                            "This dumps out the entire config including the values \
                             which are not in the config file but filled in from \
                             defaults.  The default output format is YAML but \
                             a debug format can also be specific which is useful \
                             to understand how the relay interprets the individual \
                             values.",
                        )
                        .arg(
                            Arg::with_name("format")
                                .short("f")
                                .long("format")
                                .possible_values(&["debug", "yaml"])
                                .default_value("yaml")
                                .help("The output format"),
                        ),
                ),
        )
}

/// Runs the command line application.
pub fn execute() -> Result<(), Error> {
    let app = make_app();
    let matches = app.get_matches();
    let config_path = matches.value_of("config").unwrap_or(".semaphore");

    // config init is special because it does not yet have a config.
    if let Some(matches) = matches.subcommand_matches("config") {
        if let Some(matches) = matches.subcommand_matches("init") {
            return init_config(&config_path, &matches);
        }
    }

    let config = Config::from_path(&config_path)?;
    setup::init_logging(&config);
    if let Some(matches) = matches.subcommand_matches("config") {
        manage_config(config, &matches)
    } else if let Some(matches) = matches.subcommand_matches("credentials") {
        manage_credentials(config, &matches)
    } else if let Some(matches) = matches.subcommand_matches("run") {
        run(config, &matches)
    } else {
        unreachable!();
    }
}

pub fn manage_credentials<'a>(mut config: Config, matches: &ArgMatches<'a>) -> Result<(), Error> {
    // generate completely new credentials
    if let Some(matches) = matches.subcommand_matches("generate") {
        if config.has_credentials() && !matches.is_present("overwrite") {
            return Err(err_msg(
                "aborting because credentials already exist. Pass --overwrite to force.",
            ));
        }
        config.regenerate_credentials()?;
        println!("Generated new credentials");
        setup::dump_credentials(&config);
    } else if let Some(matches) = matches.subcommand_matches("set") {
        let mut prompted = false;
        let secret_key = match matches.value_of("secret_key") {
            Some(value) => Some(value
                .parse()
                .map_err(|_| err_msg("invalid secret key supplied"))?),
            None => config.credentials().map(|x| x.secret_key.clone()),
        };
        let public_key = match matches.value_of("secret_key") {
            Some(value) => Some(value
                .parse()
                .map_err(|_| err_msg("invalid public key supplied"))?),
            None => config.credentials().map(|x| x.public_key.clone()),
        };
        let id = match matches.value_of("id") {
            Some("random") => Some(Uuid::new_v4()),
            Some(value) => Some(value
                .parse()
                .map_err(|_| err_msg("invalid relay id supplied"))?),
            None => config.credentials().map(|x| x.id.clone()),
        };
        let changed = config.replace_credentials(Some(Credentials {
            secret_key: match secret_key {
                Some(value) => value,
                None => {
                    prompted = true;
                    utils::prompt_value_no_default("secret key")?
                }
            },
            public_key: match public_key {
                Some(value) => value,
                None => {
                    prompted = true;
                    utils::prompt_value_no_default("public key")?
                }
            },
            id: match id {
                Some(value) => value,
                None => {
                    prompted = true;
                    if Confirmation::new("do you want to generate a random relay id").interact()? {
                        Uuid::new_v4()
                    } else {
                        utils::prompt_value_no_default("relay id")?
                    }
                }
            },
        }))?;
        if !changed {
            println!("Nothing was changed");
            if !prompted {
                println!(
                    "Run `semaphore credentials remove` first to remove all stored credentials."
                );
            }
        } else {
            println!("Stored updated credentials:");
            setup::dump_credentials(&config);
        }
    } else if let Some(matches) = matches.subcommand_matches("remove") {
        if config.has_credentials() {
            if matches.is_present("yes")
                || Confirmation::new("Remove stored credentials?").interact()?
            {
                config.replace_credentials(None)?;
                println!("Credentials removed");
            }
        } else {
            println!("No credentials");
        }
    } else if let Some(..) = matches.subcommand_matches("show") {
        if !config.has_credentials() {
            return Err(err_msg("no stored credentials"));
        } else {
            println!("Stored credentials:");
            setup::dump_credentials(&config);
        }
    } else {
        unreachable!();
    }

    Ok(())
}

pub fn manage_config<'a>(config: Config, matches: &ArgMatches<'a>) -> Result<(), Error> {
    if let Some(matches) = matches.subcommand_matches("init") {
        return init_config(config.path(), &matches);
    } else if let Some(matches) = matches.subcommand_matches("show") {
        match matches.value_of("format").unwrap() {
            "debug" => println!("{:#?}", &config),
            "yaml" => println!("{}", config.to_yaml_string()?),
            _ => unreachable!(),
        }
        Ok(())
    } else {
        unreachable!();
    }
}

pub fn init_config<'a, P: AsRef<Path>>(
    config_path: P,
    _matches: &ArgMatches<'a>,
) -> Result<(), Error> {
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
                mincfg.relay.tls_private_key = Some(PathBuf::from(
                    utils::prompt_value_no_default::<String>("tls private key path")?,
                ));
                mincfg.relay.tls_cert = Some(PathBuf::from(utils::prompt_value_no_default::<
                    String,
                >("tls certificate path")?));
            }
        }

        mincfg.save_in_folder(&config_path)?;
        done_something = true;
    }

    let mut config = Config::from_path(&config_path)?;
    if !config.has_credentials() {
        config.regenerate_credentials()?;
        println!("Generated new credentials");
        setup::dump_credentials(&config);
        done_something = true;
    }

    if done_something {
        println!("All done!");
    } else {
        println!("Nothing to do.");
    }

    Ok(())
}

pub fn run<'a>(config: Config, _matches: &ArgMatches<'a>) -> Result<(), Error> {
    if !config.has_credentials() {
        return Err(err_msg(
            "relay has no stored credentials. Generate some \
             with \"semaphore credentials generate\" first.",
        ));
    }
    setup::dump_spawn_infos(&config);
    setup::init_metrics(&config)?;
    semaphore_server::run(config)?;
    Ok(())
}
