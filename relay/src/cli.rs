use std::path::{Path, PathBuf};
use std::{env, io};

use anyhow::{anyhow, bail, Result};
use clap::ArgMatches;
use clap_complete::Shell;
use dialoguer::{Confirm, Select};
use relay_config::{
    Config, ConfigError, ConfigErrorKind, Credentials, MinimalConfig, OverridableConfig, RelayMode,
};
use uuid::Uuid;

use crate::cliapp::make_app;
use crate::utils::get_theme;
use crate::{setup, utils};

fn load_config(path: impl AsRef<Path>, require: bool) -> Result<Config> {
    match Config::from_path(path) {
        Ok(config) => Ok(config),
        Err(error) => {
            if let Some(config_error) = error.downcast_ref::<ConfigError>() {
                if !require && config_error.kind() == ConfigErrorKind::CouldNotOpenFile {
                    return Ok(Config::default());
                }
            }

            Err(error)
        }
    }
}

/// Runs the command line application.
pub fn execute() -> Result<()> {
    let app = make_app();
    let matches = app.get_matches();
    let config_path = matches
        .get_one::<PathBuf>("config")
        .map_or(Path::new(".relay"), PathBuf::as_path);

    // Commands that do not need to load the config:
    if let Some(matches) = matches.subcommand_matches("config") {
        if let Some(matches) = matches.subcommand_matches("init") {
            return init_config(config_path, matches);
        }
    } else if let Some(matches) = matches.subcommand_matches("generate-completions") {
        return generate_completions(matches);
    }

    // Commands that need a loaded config:
    let mut config = load_config(config_path, matches.contains_id("config"))?;
    // override file config with environment variables
    let env_config = extract_config_env_vars();
    config.apply_override(env_config)?;

    relay_log::init(config.logging(), config.sentry());

    if let Some(matches) = matches.subcommand_matches("config") {
        manage_config(&config, matches)
    } else if let Some(matches) = matches.subcommand_matches("credentials") {
        manage_credentials(config, matches)
    } else if let Some(matches) = matches.subcommand_matches("run") {
        // override config with run command args
        let arg_config = extract_config_args(matches);
        config.apply_override(arg_config)?;
        run(config, matches)
    } else {
        unreachable!();
    }
}

/// Extract config arguments from a parsed command line arguments object
pub fn extract_config_args(matches: &ArgMatches) -> OverridableConfig {
    let processing = if matches.get_flag("processing") {
        Some("true".to_owned())
    } else if matches.get_flag("no_processing") {
        Some("false".to_owned())
    } else {
        None
    };

    OverridableConfig {
        mode: matches.get_one("mode").cloned(),
        upstream: matches.get_one("upstream").cloned(),
        upstream_dsn: matches.get_one("upstream_dsn").cloned(),
        host: matches.get_one("host").cloned(),
        port: matches.get_one("port").cloned(),
        processing,
        kafka_url: matches.get_one("kafka_broker_url").cloned(),
        redis_url: matches.get_one("redis_url").cloned(),
        id: matches.get_one("id").cloned(),
        public_key: matches.get_one("public_key").cloned(),
        secret_key: matches.get_one("secret_key").cloned(),
        outcome_source: matches.get_one("source_id").cloned(),
        shutdown_timeout: matches.get_one("shutdown_timeout").cloned(),
        aws_runtime_api: matches.get_one("aws_runtime_api").cloned(),
    }
}

/// Extract config arguments from environment variables
pub fn extract_config_env_vars() -> OverridableConfig {
    OverridableConfig {
        mode: env::var("RELAY_MODE").ok(),
        upstream: env::var("RELAY_UPSTREAM_URL").ok(),
        upstream_dsn: env::var("RELAY_UPSTREAM_DSN").ok(),
        host: env::var("RELAY_HOST").ok(),
        port: env::var("RELAY_PORT").ok(),
        processing: env::var("RELAY_PROCESSING_ENABLED").ok(),
        kafka_url: env::var("RELAY_KAFKA_BROKER_URL").ok(),
        redis_url: env::var("RELAY_REDIS_URL").ok(),
        id: env::var("RELAY_ID").ok(),
        public_key: env::var("RELAY_PUBLIC_KEY").ok(),
        secret_key: env::var("RELAY_SECRET_KEY").ok(),
        outcome_source: None, //already extracted in params
        shutdown_timeout: env::var("SHUTDOWN_TIMEOUT").ok(),
        aws_runtime_api: None,
    }
}

pub fn manage_credentials(mut config: Config, matches: &ArgMatches) -> Result<()> {
    // generate completely new credentials
    if let Some(matches) = matches.subcommand_matches("generate") {
        if config.has_credentials() && !matches.get_flag("overwrite") {
            bail!("aborting because credentials already exist. Pass --overwrite to force.");
        }
        let credentials = Credentials::generate();
        if matches.get_flag("stdout") {
            println!("{}", credentials.to_json_string()?);
        } else {
            config.replace_credentials(Some(credentials))?;
            println!("Generated new credentials");
            setup::dump_credentials(&config);
        }
    } else if let Some(matches) = matches.subcommand_matches("set") {
        let mut prompted = false;
        let secret_key = match matches.get_one::<String>("secret_key") {
            Some(value) => Some(
                value
                    .parse()
                    .map_err(|_| anyhow!("invalid secret key supplied"))?,
            ),
            None => config.credentials().map(|x| x.secret_key.clone()),
        };
        let public_key = match matches.get_one::<String>("public_key") {
            Some(value) => Some(
                value
                    .parse()
                    .map_err(|_| anyhow!("invalid public key supplied"))?,
            ),
            None => config.credentials().map(|x| x.public_key.clone()),
        };
        let id = match matches.get_one::<String>("id").map(String::as_str) {
            Some("random") => Some(Uuid::new_v4()),
            Some(value) => Some(
                value
                    .parse()
                    .map_err(|_| anyhow!("invalid relay id supplied"))?,
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
                    if Confirm::with_theme(get_theme())
                        .with_prompt("do you want to generate a random relay id")
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
            if matches.get_flag("yes")
                || Confirm::with_theme(get_theme())
                    .with_prompt("Remove stored credentials?")
                    .interact()?
            {
                config.replace_credentials(None)?;
                println!("Credentials removed");
            }
        } else {
            println!("No credentials");
        }
    } else if matches.subcommand_matches("show").is_some() {
        if !config.has_credentials() {
            bail!("no stored credentials");
        } else {
            println!("Credentials:");
            setup::dump_credentials(&config);
        }
    } else {
        unreachable!();
    }

    Ok(())
}

pub fn manage_config(config: &Config, matches: &ArgMatches) -> Result<()> {
    if let Some(matches) = matches.subcommand_matches("init") {
        init_config(config.path(), matches)
    } else if let Some(matches) = matches.subcommand_matches("show") {
        match matches.get_one("format").map(String::as_str).unwrap() {
            "debug" => println!("{config:#?}"),
            "yaml" => println!("{}", config.to_yaml_string()?),
            _ => unreachable!(),
        }
        Ok(())
    } else {
        unreachable!();
    }
}

pub fn init_config<P: AsRef<Path>>(config_path: P, _matches: &ArgMatches) -> Result<()> {
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

pub fn generate_completions(matches: &ArgMatches) -> Result<()> {
    let shell = match matches.get_one::<Shell>("format") {
        Some(shell) => *shell,
        None => match env::var("SHELL")
            .ok()
            .as_ref()
            .and_then(|x| x.rsplit('/').next())
        {
            Some("bash") => Shell::Bash,
            Some("zsh") => Shell::Zsh,
            Some("fish") => Shell::Fish,
            #[cfg(windows)]
            _ => Shell::PowerShell,
            #[cfg(not(windows))]
            _ => Shell::Bash,
        },
    };

    let mut app = make_app();
    let name = app.get_name().to_string();
    clap_complete::generate(shell, &mut app, name, &mut io::stdout());

    Ok(())
}

pub fn run(config: Config, _matches: &ArgMatches) -> Result<()> {
    setup::dump_spawn_infos(&config);
    setup::check_config(&config)?;
    setup::init_metrics(&config)?;
    relay_server::run(config)?;
    Ok(())
}
