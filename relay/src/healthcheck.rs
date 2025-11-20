use std::net::SocketAddr;
use std::time::Duration;

use anyhow::{Result, format_err};
use clap::ArgMatches;
use relay_config::Config;
use reqwest::blocking::Client;

pub fn healthcheck(config: &Config, matches: &ArgMatches) -> Result<()> {
    let mode = matches
        .get_one::<String>("mode")
        .expect("`mode` is required");

    let timeout = matches
        .get_one::<u64>("timeout")
        .expect("`timeout` is required");

    let addr = matches
        .get_one::<SocketAddr>("addr")
        .copied()
        .or_else(|| config.listen_addr_internal())
        .unwrap_or(config.listen_addr());

    let client = Client::builder()
        .timeout(Some(Duration::from_secs(*timeout)))
        .build()
        .unwrap_or_default();

    let response = client
        .get(format!("http://{addr}/api/relay/healthcheck/{mode}/"))
        .send();

    match response {
        Ok(response) => {
            if response.status().is_success() {
                Ok(())
            } else {
                relay_log::error!("Relay is unhealthy. Status code: {}", response.status());
                Err(format_err!(
                    "Relay is unhealthy. Status code: {}",
                    response.status()
                ))
            }
        }
        Err(err) => {
            relay_log::error!("Relay is unhealthy. Error: {err}");
            Err(err.into())
        }
    }
}
