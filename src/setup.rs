use std::env;

use failure::Error;
use log::LevelFilter;
use pretty_env_logger;
use sentry;
use sentry::integrations::log as sentry_log;

use semaphore_common::metrics;
use semaphore_config::Config;

/// Print spawn infos to the log.
pub fn dump_spawn_infos(config: &Config) {
    info!(
        "launching relay from config folder {}",
        config.path().display()
    );
    info!("  relay id: {}", config.relay_id());
    info!("  public key: {}", config.public_key());
    info!("  log level: {}", config.log_level_filter());
}

/// Dumps out credential info.
pub fn dump_credentials(config: &Config) {
    println!("  relay id: {}", config.relay_id());
    println!("  public key: {}", config.public_key());
}

/// Initialize the logging system.
pub fn init_logging(config: &Config) {
    sentry::init((
        config.sentry_dsn(),
        sentry::ClientOptions {
            in_app_include: vec![
                "semaphore_common::",
                "semaphore_aorta::",
                "semaphore_config::",
                "semaphore_common::",
                "semaphore_server::",
                "semaphore_trove::",
                "sentry_relay::",
            ],
            ..Default::default()
        },
    ));

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
                     semaphore_common=DEBUG,\
                     semaphore_aorta=DEBUG,\
                     semaphore_config=DEBUG,\
                     semaphore_common=DEBUG,\
                     semaphore_server=DEBUG,\
                     semaphore_trove=DEBUG,\
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

    let log = Box::new(log_builder.build());
    let global_filter = log.filter();

    sentry_log::init(
        Some(log),
        sentry_log::LoggerOptions {
            global_filter: Some(global_filter),
            ..Default::default()
        },
    );
}

/// Initialize the metric system.
pub fn init_metrics(config: &Config) -> Result<(), Error> {
    let addrs = config.statsd_addrs()?;
    if !addrs.is_empty() {
        metrics::configure_statsd(config.metrics_prefix(), &addrs[..]);
    }
    Ok(())
}
