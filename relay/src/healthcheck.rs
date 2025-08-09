use std::time::Duration;

use anyhow::{Result, format_err};
use clap::ArgMatches;
use reqwest::blocking::Client;

pub fn healthcheck(matches: &ArgMatches) -> Result<()> {
    let mode = matches.get_one::<String>("mode").unwrap();
    let timeout = matches.get_one::<u64>("timeout").unwrap();
    let host = matches.get_one::<String>("host").unwrap();
    let port = matches.get_one::<u16>("port").unwrap();

    // Make sure `mode` is either `live` or `ready`. Otherwise, return an error.
    if mode != "live" && mode != "ready" {
        return Err(format_err!(
            "Invalid mode. Expected `live` or `ready`, got `{}`.",
            mode
        ));
    }

    let client = Client::builder()
        .timeout(Some(Duration::from_secs(*timeout)))
        .build()
        .unwrap_or_default();
    let response = client
        .get(format!(
            "http://{}:{}/api/relay/healthcheck/{}/",
            host, port, mode
        ))
        .send();

    match response {
        Ok(response) => {
            if response.status().is_success() {
                println!("Relay is healthy.");
                Ok(())
            } else {
                println!("Relay is unhealthy.");
                Err(format_err!(
                    "Relay is unhealthy. Status code: {}",
                    response.status()
                ))
            }
        }
        Err(err) => {
            println!("Relay is unhealthy.");
            println!("Error: {}", err);
            Err(err.into())
        }
    }
}
