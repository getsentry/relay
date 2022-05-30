// This module implements the definition of the command line app.
//
// It must not have any other imports as also the build.rs file to
// automatically generate the completion scripts.
use clap::{App, AppSettings, Arg, ArgGroup, Shell};

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
pub const ABOUT: &str = "The official Sentry Relay.";

pub fn make_app() -> App<'static, 'static> {
    App::new("relay")
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
        .subcommand(
            App::new("run")
                .about("Run the relay")
                .after_help(
                    "This runs the relay in the foreground until it's shut down.  It will bind \
             to the port and network interface configured in the config file.",
                )
                .arg(
                    Arg::with_name("mode")
                        .long("mode")
                        .help("The relay mode to set")
                        .value_name("MODE")
                        .possible_values(&["managed", "proxy", "static"]),
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
                )
                .arg(
                    Arg::with_name("upstream")
                        .value_name("UPSTREAM_URL")
                        .takes_value(true)
                        .short("u")
                        .long("upstream")
                        .help("The upstream server URL."),
                )
                .arg(
                    Arg::with_name("host")
                        .value_name("HOST")
                        .takes_value(true)
                        .short("H")
                        .long("host")
                        .help("The host dns name."),
                )
                .arg(
                    Arg::with_name("port")
                        .value_name("PORT")
                        .takes_value(true)
                        .short("P")
                        .long("port")
                        .help("The server port."),
                )
                .arg(
                    Arg::with_name("processing")
                        .long("processing")
                        .help("Enable processing."),
                )
                .arg(
                    Arg::with_name("no_processing")
                        .long("no-processing")
                        .help("Disable processing."),
                )
                .group(
                    ArgGroup::with_name("processing_group")
                        .args(&["processing", "no_processing"])
                        .multiple(false),
                )
                .arg(
                    Arg::with_name("kafka_broker_url")
                        .value_name("KAFKA_BROKER_URL")
                        .takes_value(true)
                        .long("kafka-broker-url")
                        .help("Kafka broker URL."),
                )
                .arg(
                    Arg::with_name("redis_url")
                        .value_name("REDIS_URL")
                        .takes_value(true)
                        .long("redis-url")
                        .help("Redis server URL."),
                )
                .arg(
                    Arg::with_name("source_id")
                        .value_name("SOURCE_ID")
                        .takes_value(true)
                        .long("source-id")
                        .env("RELAY_SOURCE_ID")
                        .help("Names the current relay in the outcome source."),
                )
                .arg(
                    Arg::with_name("shutdown_timeout")
                        .value_name("SHUTDOWN_TIMEOUT")
                        .takes_value(true)
                        .long("shutdown-timeout")
                        .help(
                            "Maximum number of seconds to wait for pending envelopes on shutdown.",
                        ),
                )
                .arg(
                    Arg::with_name("aws_runtime_api")
                        .value_name("$AWS_LAMBDA_RUNTIME_API")
                        .takes_value(true)
                        .long("aws-runtime-api")
                        .help(
                            "Host and port of the AWS lambda extensions API, usually provided in \
                            the AWS_LAMBDA_RUNTIME_API environment variable. This integrates Relay \
                            with the lambda execution environment lifecycle.",
                        ),
                ),
        )
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
                        )
                        .arg(
                            Arg::with_name("stdout")
                                .long("stdout")
                                .help("Write credentials to stdout instead of credentials.json"),
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
                            Arg::with_name("mode")
                                .long("mode")
                                .help("The relay mode to set")
                                .value_name("MODE")
                                .possible_values(&["managed", "proxy", "static"]),
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
        .subcommand(
            App::new("generate-completions")
                .about("Generate shell completion file")
                .after_help(
                    "This generates a completions file for the shell of choice. \
                     The default selection will be an educated guess for the currently \
                     running shell.",
                )
                .arg(
                    Arg::with_name("format")
                        .short("f")
                        .long("format")
                        .value_name("FORMAT")
                        .possible_values(&Shell::variants()[..])
                        .help(
                            "Explicitly pick the shell to generate a completion file \
                             for.  The default is autodetection",
                        ),
                ),
        )
}
