//! This module implements the definition of the command line app.

use clap::builder::ValueParser;
use clap::{Arg, ArgAction, ArgGroup, Command, ValueHint};
use clap_complete::Shell;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
pub const ABOUT: &str = "The official Sentry Relay.";

pub fn make_app() -> Command {
    Command::new("relay")
        .disable_help_subcommand(true)
        .subcommand_required(true)
        .propagate_version(true)
        .max_term_width(79)
        .version(VERSION)
        .about(ABOUT)
        .arg(
            Arg::new("config")
                .long("config")
                .short('c')
                .global(true)
                .value_hint(ValueHint::DirPath)
                .value_parser(ValueParser::path_buf())
                .help("The path to the config folder."),
        )
        .subcommand(
            Command::new("run")
                .about("Run the relay")
                .after_help(
                    "This runs the relay in the foreground until it's shut down.  It will bind \
                    to the port and network interface configured in the config file.",
                )
                .arg(
                    Arg::new("mode")
                        .long("mode")
                        .help("The relay mode to set")
                        .value_parser(["managed", "proxy", "static"]),
                )
                .arg(
                    Arg::new("log_level")
                        .long("log-level")
                        .help("The relay log level")
                        .value_parser(["info", "warn", "error", "debug", "trace"]),
                )
                .arg(
                    Arg::new("secret_key")
                        .long("secret-key")
                        .short('s')
                        .requires("public_key")
                        .help("The secret key to set"),
                )
                .arg(
                    Arg::new("public_key")
                        .long("public-key")
                        .short('p')
                        .requires("secret_key")
                        .help("The public key to set"),
                )
                .arg(
                    Arg::new("id")
                        .long("id")
                        .short('i')
                        .help("The relay ID to set"),
                )
                .arg(
                    Arg::new("upstream")
                        .value_name("url")
                        .value_hint(ValueHint::Url)
                        .short('u')
                        .long("upstream")
                        .help("The upstream server URL."),
                )
                .arg(
                    Arg::new("upstream_dsn")
                        .value_name("dsn")
                        .long("upstream-dsn")
                        .conflicts_with("upstream")
                        .help(
                            "Alternate upstream provided through a Sentry DSN, compatible with the \
                            SENTRY_DSN environment variable. Key and project of the DSN will be \
                            ignored.",
                        ),
                )
                .arg(
                    Arg::new("host")
                        .value_name("HOST")
                        .short('H')
                        .long("host")
                        .help("The host dns name."),
                )
                .arg(
                    Arg::new("port")
                        .value_name("PORT")
                        .short('P')
                        .long("port")
                        .help("The server port."),
                )
                .arg(
                    Arg::new("processing")
                        .long("processing")
                        .help("Enable processing.")
                        .action(ArgAction::SetTrue),
                )
                .arg(
                    Arg::new("no_processing")
                        .long("no-processing")
                        .help("Disable processing.")
                        .action(ArgAction::SetTrue),
                )
                .group(
                    ArgGroup::new("processing_group")
                        .args(["processing", "no_processing"])
                        .multiple(false),
                )
                .arg(
                    Arg::new("kafka_broker_url")
                        .value_name("url")
                        .value_hint(ValueHint::Url)
                        .long("kafka-broker-url")
                        .help("Kafka broker URL."),
                )
                .arg(
                    Arg::new("redis_url")
                        .value_name("url")
                        .value_hint(ValueHint::Url)
                        .long("redis-url")
                        .help("Redis server URL."),
                )
                .arg(
                    Arg::new("source_id")
                        .long("source-id")
                        .env("RELAY_SOURCE_ID")
                        .help("Names the current relay in the outcome source."),
                )
                .arg(
                    Arg::new("shutdown_timeout")
                        .value_name("seconds")
                        .long("shutdown-timeout")
                        .help(
                            "Maximum number of seconds to wait for pending envelopes on shutdown.",
                        ),
                )
                .arg(
                    Arg::new("aws_runtime_api")
                        .value_name("address")
                        .long("aws-runtime-api")
                        .help(
                            "Host and port of the AWS lambda extensions API, usually provided in \
                            the AWS_LAMBDA_RUNTIME_API environment variable. This integrates Relay \
                            with the lambda execution environment lifecycle.",
                        ),
                ),
        )
        .subcommand(
            Command::new("credentials")
                .subcommand_required(true)
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
                    Command::new("generate")
                        .about("Generate new credentials")
                        .after_help(
                            "This generates new credentials for the relay and stores \
                             them.  In case the relay already has credentials stored \
                             this command will error unless the '--overwrite' option \
                             has been passed.",
                        )
                        .arg(
                            Arg::new("overwrite")
                                .long("overwrite")
                                .action(ArgAction::SetTrue)
                                .help("Overwrite already existing credentials instead of failing"),
                        )
                        .arg(
                            Arg::new("stdout")
                                .long("stdout")
                                .action(ArgAction::SetTrue)
                                .help("Write credentials to stdout instead of credentials.json"),
                        ),
                )
                .subcommand(
                    Command::new("remove")
                        .about("Remove credentials")
                        .after_help(
                            "This command removes already stored credentials from the \
                             relay.",
                        )
                        .arg(
                            Arg::new("yes")
                                .long("yes")
                                .action(ArgAction::SetTrue)
                                .help("Do not prompt for confirmation"),
                        ),
                )
                .subcommand(
                    Command::new("show")
                        .about("Show currently stored credentials.")
                        .after_help("This prints out the agent ID and public key."),
                )
                .subcommand(
                    Command::new("set")
                        .about("Set new credentials")
                        .after_help(
                            "Credentials can be stored by providing them on the command \
                             line.  If just an agent id (or secret/public key pair) is \
                             provided that part of the credentials are overwritten.  If \
                             no credentials are stored yet at all and no parameters are \
                             supplied the command will prompt for the appropriate values.",
                        )
                        .arg(
                            Arg::new("mode")
                                .long("mode")
                                .help("The relay mode to set")
                                .value_parser(["managed", "proxy", "static"]),
                        )
                        .arg(
                            Arg::new("secret_key")
                                .long("secret-key")
                                .short('s')
                                .requires("public_key")
                                .help("The secret key to set"),
                        )
                        .arg(
                            Arg::new("public_key")
                                .long("public-key")
                                .short('p')
                                .requires("secret_key")
                                .help("The public key to set"),
                        )
                        .arg(
                            Arg::new("id")
                                .long("id")
                                .short('i')
                                .help("The relay ID to set"),
                        ),
                ),
        )
        .subcommand(
            Command::new("config")
                .about("Manage the relay config")
                .after_help(
                    "This command provides basic config management.  It can be \
                     used primarily to initialize a new relay config and to \
                     print out the current config.",
                )
                .subcommand_required(true)
                .subcommand(
                    Command::new("init")
                        .about("Initialize a new relay config")
                        .after_help(
                            "For new relay installations this will guide through \
                             the initial config process and create the necessary \
                             files.  It will create an initial config as well as \
                             set of credentials.",
                        ),
                )
                .subcommand(
                    Command::new("show")
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
                            Arg::new("format")
                                .short('f')
                                .long("format")
                                .value_parser(["debug", "yaml"])
                                .default_value("yaml")
                                .help("The output format"),
                        ),
                ),
        )
        .subcommand(
            Command::new("generate-completions")
                .about("Generate shell completion file")
                .after_help(
                    "This generates a completions file for the shell of choice. \
                     The default selection will be an educated guess for the currently \
                     running shell.",
                )
                .arg(
                    Arg::new("format")
                        .short('f')
                        .long("format")
                        .value_name("SHELL")
                        .value_parser(clap::value_parser!(Shell))
                        .help(
                            "Explicitly pick the shell to generate a completion file \
                             for. The default is autodetection.",
                        ),
                ),
        )
}
