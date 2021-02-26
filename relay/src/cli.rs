use std::env;
use std::io;
use std::path::{Path, PathBuf};

use clap::{ArgMatches, Shell};
use dialoguer::{Confirmation, Select};
use failure::{err_msg, Error};

use relay_common::Uuid;
use relay_config::{Config, Credentials, MinimalConfig, OverridableConfig, RelayMode};

use crate::cliapp::make_app;
use crate::setup;
use crate::utils;
use crate::utils::get_theme;

/// Runs the command line application.
pub fn execute() -> Result<(), Error> {
    let app = make_app();
    let matches = app.get_matches();
    let config_path = matches.value_of("config").unwrap_or(".relay");

    // Commands that do not need to load the config:
    if let Some(matches) = matches.subcommand_matches("config") {
        if let Some(matches) = matches.subcommand_matches("init") {
            return init_config(&config_path, &matches);
        }
    } else if let Some(matches) = matches.subcommand_matches("generate-completions") {
        return generate_completions(&matches);
    }

    // Commands that need a loaded config:
    let mut config = Config::from_path(&config_path)?;
    // override file config with environment variables
    let env_config = extract_config_env_vars();
    config.apply_override(env_config)?;

    relay_log::init(config.logging(), config.sentry());
    if let Some(matches) = matches.subcommand_matches("config") {
        manage_config(&config, &matches)
    } else if let Some(matches) = matches.subcommand_matches("credentials") {
        manage_credentials(config, &matches)
    } else if let Some(matches) = matches.subcommand_matches("run") {
        // override config with run command args
        let arg_config = extract_config_args(matches);
        config.apply_override(arg_config)?;
        run(config, &matches)
    } else {
        unreachable!();
    }
}

/// Extract config arguments from a parsed command line arguments object
pub fn extract_config_args(matches: &ArgMatches) -> OverridableConfig {
    let processing = if matches.is_present("processing") {
        Some("true".to_owned())
    } else if matches.is_present("no_processing") {
        Some("false".to_owned())
    } else {
        None
    };

    OverridableConfig {
        upstream: matches.value_of("upstream").map(str::to_owned),
        host: matches.value_of("host").map(str::to_owned),
        port: matches.value_of("redis_url").map(str::to_owned),
        processing,
        kafka_url: matches.value_of("kafka_broker_url").map(str::to_owned),
        redis_url: matches.value_of("redis_url").map(str::to_owned),
        id: matches.value_of("id").map(str::to_owned),
        public_key: matches.value_of("public_key").map(str::to_owned),
        secret_key: matches.value_of("secret_key").map(str::to_owned),
        outcome_source: matches.value_of("source_id").map(str::to_owned),
    }
}

/// Extract config arguments from environment variables
pub fn extract_config_env_vars() -> OverridableConfig {
    OverridableConfig {
        upstream: env::var("RELAY_UPSTREAM_URL").ok(),
        host: env::var("RELAY_HOST").ok(),
        port: env::var("RELAY_PORT").ok(),
        processing: env::var("RELAY_PROCESSING_ENABLED").ok(),
        kafka_url: env::var("RELAY_KAFKA_BROKER_URL").ok(),
        redis_url: env::var("RELAY_REDIS_URL").ok(),
        id: env::var("RELAY_ID").ok(),
        public_key: env::var("RELAY_PUBLIC_KEY").ok(),
        secret_key: env::var("RELAY_SECRET_KEY").ok(),
        outcome_source: None, //already extracted in params
    }
}

pub fn manage_credentials(mut config: Config, matches: &ArgMatches) -> Result<(), Error> {
    // generate completely new credentials
    if let Some(matches) = matches.subcommand_matches("generate") {
        if config.has_credentials() && !matches.is_present("overwrite") {
            return Err(err_msg(
                "aborting because credentials already exist. Pass --overwrite to force.",
            ));
        }
        let credentials = Credentials::generate();
        if matches.is_present("stdout") {
            println!("{}", credentials.to_json_string()?);
        } else {
            config.replace_credentials(Some(credentials))?;
            println!("Generated new credentials");
            setup::dump_credentials(&config);
        }
    } else if let Some(matches) = matches.subcommand_matches("set") {
        let mut prompted = false;
        let secret_key = match matches.value_of("secret_key") {
            Some(value) => Some(
                value
                    .parse()
                    .map_err(|_| err_msg("invalid secret key supplied"))?,
            ),
            None => config.credentials().map(|x| x.secret_key.clone()),
        };
        let public_key = match matches.value_of("secret_key") {
            Some(value) => Some(
                value
                    .parse()
                    .map_err(|_| err_msg("invalid public key supplied"))?,
            ),
            None => config.credentials().map(|x| x.public_key.clone()),
        };
        let id = match matches.value_of("id") {
            Some("random") => Some(Uuid::new_v4()),
            Some(value) => Some(
                value
                    .parse()
                    .map_err(|_| err_msg("invalid relay id supplied"))?,
            ),
            None => config.credentials().map(|x| x.id),
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
                    if Confirmation::with_theme(get_theme())
                        .with_text("do you want to generate a random relay id")
                        .interact()?
                    {
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
                println!("Run `relay credentials remove` first to remove all stored credentials.");
            }
        } else {
            println!("Stored updated credentials:");
            setup::dump_credentials(&config);
        }
    } else if let Some(matches) = matches.subcommand_matches("remove") {
        if config.has_credentials() {
            if matches.is_present("yes")
                || Confirmation::with_theme(get_theme())
                    .with_text("Remove stored credentials?")
                    .interact()?
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
            println!("Credentials:");
            setup::dump_credentials(&config);
        }
    } else {
        unreachable!();
    }

    Ok(())
}

pub fn manage_config<'a>(config: &Config, matches: &ArgMatches<'a>) -> Result<(), Error> {
    if let Some(matches) = matches.subcommand_matches("init") {
        init_config(config.path(), &matches)
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

pub fn init_config<P: AsRef<Path>>(config_path: P, _matches: &ArgMatches) -> Result<(), Error> {
    let mut done_something = false;
    let config_path = env::current_dir()?.join(config_path.as_ref());
    println!("Initializing relay in {}", config_path.display());

    if !Config::config_exists(&config_path) {
        let item = Select::with_theme(get_theme())
            .with_prompt("Do you want to create a new config?")
            .default(0)
            .item("Yes, create default config")
            .item("Yes, create custom config")
            .item("No, abort")
            .interact()?;

        let with_prompts = match item {
            0 => false,
            1 => true,
            2 => return Ok(()),
            _ => unreachable!(),
        };

        let mut mincfg = MinimalConfig::default();
        if with_prompts {
            let mode = Select::with_theme(get_theme())
                .with_prompt("How should this relay operate?")
                .default(0)
                .item("Managed through upstream")
                .item("Statically configured")
                .item("Proxy for all events")
                .interact()?;

            mincfg.relay.mode = match mode {
                0 => RelayMode::Managed,
                1 => RelayMode::Static,
                2 => RelayMode::Proxy,
                _ => unreachable!(),
            };

            utils::prompt_value("upstream", &mut mincfg.relay.upstream)?;
            utils::prompt_value("listen interface", &mut mincfg.relay.host)?;
            utils::prompt_value("listen port", &mut mincfg.relay.port)?;

            if Confirmation::with_theme(get_theme())
                .with_text("do you want listen to TLS")
                .interact()?
            {
                let mut port = mincfg.relay.port.saturating_add(443);
                utils::prompt_value("tls port", &mut port)?;
                mincfg.relay.tls_port = Some(port);
                mincfg.relay.tls_identity_path =
                    Some(PathBuf::from(utils::prompt_value_no_default::<String>(
                        "path to your DER-encoded PKCS #12 archive",
                    )?));
                mincfg.relay.tls_identity_password = Some(
                    utils::prompt_value_no_default::<String>("password for your PKCS #12 archive")?,
                );
            }
        }

        // TODO: Enable this once logging to Sentry is more useful.
        // mincfg.sentry.enabled = Select::with_theme(get_theme())
        //     .with_prompt("Do you want to enable internal crash reporting?")
        //     .default(0)
        //     .item("Yes, share relay internal crash reports with sentry.io")
        //     .item("No, do not share crash reports")
        //     .interact()?
        //     == 0;

        mincfg.save_in_folder(&config_path)?;
        done_something = true;
    }

    let mut config = Config::from_path(&config_path)?;
    if config.relay_mode() == RelayMode::Managed && !config.has_credentials() {
        let credentials = Credentials::generate();
        config.replace_credentials(Some(credentials))?;
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

pub fn generate_completions(matches: &ArgMatches) -> Result<(), Error> {
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
    make_app().gen_completions_to("relay", shell, &mut io::stdout());
    Ok(())
}

pub fn run(config: Config, _matches: &ArgMatches) -> Result<(), Error> {
    setup::dump_spawn_infos(&config);
    setup::check_config(&config)?;
    setup::init_metrics(&config)?;
    relay_server::run(config)?;
    Ok(())
}
