use failure::{err_msg, Error};

use relay_common::metrics;
use relay_config::{Config, RelayMode};

pub fn check_config(config: &Config) -> Result<(), Error> {
    if config.relay_mode() == RelayMode::Managed && config.credentials().is_none() {
        return Err(err_msg(
            "relay has no credentials, which are required in managed mode. \
             Generate some with \"relay credentials generate\" first.",
        ));
    }

    Ok(())
}

/// Print spawn infos to the log.
pub fn dump_spawn_infos(config: &Config) {
    relay_log::info!(
        "launching relay from config folder {}",
        config.path().display()
    );
    relay_log::info!("  relay mode: {}", config.relay_mode());

    match config.relay_id() {
        Some(id) => relay_log::info!("  relay id: {}", id),
        None => relay_log::info!("  relay id: -"),
    };
    match config.public_key() {
        Some(key) => relay_log::info!("  public key: {}", key),
        None => relay_log::info!("  public key: -"),
    };
    relay_log::info!("  log level: {}", config.logging().level);
}

/// Dumps out credential info.
pub fn dump_credentials(config: &Config) {
    match config.relay_id() {
        Some(id) => println!("  relay id: {}", id),
        None => println!("  relay id: -"),
    };
    match config.public_key() {
        Some(key) => println!("  public key: {}", key),
        None => println!("  public key: -"),
    };
}

/// Initialize the metric system.
pub fn init_metrics(config: &Config) -> Result<(), Error> {
    let addrs = config.statsd_addrs()?;
    if addrs.is_empty() {
        return Ok(());
    }

    let mut default_tags = config.metrics_default_tags().clone();
    if let Some(hostname_tag) = config.metrics_hostname_tag() {
        if let Some(hostname) = hostname::get().ok().and_then(|s| s.into_string().ok()) {
            default_tags.insert(hostname_tag.to_owned(), hostname);
        }
    }
    metrics::configure_statsd(
        config.metrics_prefix(),
        &addrs[..],
        default_tags,
        config.metrics_buffering(),
        config.metrics_sample_rate(),
    );

    Ok(())
}
