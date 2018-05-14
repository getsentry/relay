use std::env;
use std::io;
use std::path::{Path, PathBuf};

use clap::{ArgMatches, Shell};
use dialoguer::{Confirmation, Select};
use failure::{err_msg, Error};
use uuid::Uuid;

use semaphore_config::{Config, Credentials, MinimalConfig};
use semaphore_server;

use cliapp::make_app;
use setup;
use utils;

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
    // likewise completions generation does not need the config.
    } else if let Some(matches) = matches.subcommand_matches("generate-completions") {
        return generate_completions(&matches);
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

pub fn generate_completions<'a>(matches: &ArgMatches<'a>) -> Result<(), Error> {
    let shell = match matches
        .value_of("format")
        .map(|x| x.parse::<Shell>().unwrap())
    {
        None => match env::var("SHELL")
            .ok()
            .as_ref()
            .and_then(|x| x.rsplit('/').next())
        {
            Some("bash") => Shell::Bash,
            Some("zsh") => Shell::Zsh,
            Some("fish") => Shell::Fish,
            _ => {
                #[cfg(windows)]
                {
                    Shell::PowerShell
                }
                #[cfg(not(windows))]
                {
                    Shell::Bash
                }
            }
        },
        Some(shell) => shell,
    };
    make_app().gen_completions_to("semaphore", shell, &mut io::stdout());
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
