//! Implements event filtering based on the client ip address.
//!
//! A project may be configured with blacklisted ip addresses that will
//! be banned from sending events (all events received from banned ip
//! addresses will be filtered).

use ipnetwork::IpNetwork;
use std::net::IpAddr;

/// Should filter event
pub fn should_filter(
    client_ip: &Option<IpAddr>,
    black_listed_ips: &[String],
) -> Result<(), String> {
    if black_listed_ips.is_empty() {
        return Ok(());
    }

    //TODO check if we shouldn't pre parse blacklisted ips when we construct the project config.
    if let Some(client_ip) = client_ip {
        for black_listed_ip_str in black_listed_ips {
            // first try to see if it is an IpAddress
            let black_listed_ip: Result<IpAddr, _> = black_listed_ip_str.as_str().parse();
            if let Ok(black_listed_ip) = black_listed_ip {
                if client_ip == &black_listed_ip {
                    return Err("Client ip filtered".to_string());
                }
            } else {
                //try to see if we can parse it as a network specifier
                let bl_ip_network: Result<IpNetwork, _> = black_listed_ip_str.as_str().parse();
                if let Ok(bl_ip_network) = bl_ip_network {
                    if bl_ip_network.contains(*client_ip) {
                        return Err("Client ip filtered".to_string());
                    }
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_should_filter_black_listed_ips() {}
}
