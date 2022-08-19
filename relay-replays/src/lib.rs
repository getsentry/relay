//! Replay processing and normalization module.
//!
//! Replays are multi-part values sent from Sentry integrations spanning arbitrary time-periods.
//! They are ingested incrementally and are available for viewing after the first two segments
//! (segment 0 and 1) have completed ingestion.
//!
//! # Protocol
//!
//! Relay is expecting a JSON object with some mandatory metadata.  However, environment and user
//! metadata is usually sent in addition to the minimal payload.
//!
//! ```json
//! {
//!     "type": "replay_event",
//!     "replay_id": "d2132d31b39445f1938d7e21b6bf0ec4",
//!     "event_id": "63c5b0f895441a94340183c5f1e74cd4",
//!     "segment_id": 0,
//!     "timestamp": 1597976392.6542819,
//!     "replay_start_timestamp": 1597976392.6542819,
//!     "urls": ["https://sentry.io"],
//!     "error_ids": ["d2132d31b39445f1938d7e21b6bf0ec4"],
//!     "trace_ids": ["63c5b0f895441a94340183c5f1e74cd4"],
//!     "requests": {
//!         "headers": {"User-Agent": "Mozilla/5.0..."}
//!     },
//! }
//! ```
use std::collections::HashMap;
use std::fmt::Write;
use std::net::IpAddr;

use serde::{Deserialize, Serialize};
use serde_json::Error;

use relay_general::user_agent::{parse_device, parse_os, parse_user_agent, Device};

pub fn normalize_replay_event(
    replay_bytes: &[u8],
    detected_ip_address: Option<IpAddr>,
) -> Result<Vec<u8>, Error> {
    let mut replay_input: ReplayInput = serde_json::from_slice(replay_bytes)?;

    // Set user-agent metadata.
    replay_input.set_user_agent_meta();

    // Set user ip-address if needed.
    match detected_ip_address {
        Some(ip_address) => replay_input.set_user_ip_address(ip_address),
        None => (),
    }

    serde_json::to_vec(&replay_input)
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ReplayInput {
    #[serde(rename = "type")]
    ty: String,
    replay_id: String,
    event_id: String,
    segment_id: u8,
    timestamp: f64,
    replay_start_timestamp: Option<f64>,
    #[serde(default)]
    urls: Vec<String>,
    #[serde(default)]
    error_ids: Vec<String>,
    #[serde(default)]
    trace_ids: Vec<String>,
    contexts: Option<Contexts>,
    dist: Option<String>,
    platform: Option<String>,
    environment: Option<String>,
    release: Option<String>,
    tags: Option<HashMap<String, String>>,
    user: Option<User>,
    sdk: Option<VersionedMeta>,
    requests: Requests,
}

impl ReplayInput {
    pub fn set_user_agent_meta(&mut self) {
        let user_agent = &self.requests.headers.user_agent;

        let device = parse_device(user_agent);

        let ua = parse_user_agent(user_agent);
        let browser_struct = VersionedMeta {
            name: ua.family,
            version: get_version(&ua.major, &ua.minor, &ua.patch),
        };

        let os = parse_os(user_agent);
        let os_struct = VersionedMeta {
            name: os.family,
            version: get_version(&os.major, &os.minor, &os.patch),
        };

        self.contexts = Some(Contexts {
            device: Some(device),
            browser: Some(browser_struct),
            os: Some(os_struct),
        })
    }

    pub fn set_user_ip_address(&mut self, ip_address: IpAddr) {
        match &self.user {
            Some(user) => {
                // User was found but no ip-address exists on the object.
                if user.ip_address.is_none() {
                    self.user = Some(User {
                        id: user.id.to_owned(),
                        username: user.username.to_owned(),
                        email: user.email.to_owned(),
                        ip_address: Some(ip_address.to_string()),
                    });
                }
            }
            None => {
                // Anonymous user-data provided.
                self.user = Some(User {
                    id: None,
                    username: None,
                    email: None,
                    ip_address: Some(ip_address.to_string()),
                });
            }
        }
    }
}

fn get_version(
    major: &Option<String>,
    minor: &Option<String>,
    patch: &Option<String>,
) -> Option<String> {
    let mut version = major.clone()?;

    if let Some(minor) = minor {
        write!(version, ".{}", minor).ok();
        if let Some(patch) = patch {
            write!(version, ".{}", patch).ok();
        }
    }

    Some(version)
}

#[derive(Debug, Deserialize, Serialize)]
struct Contexts {
    browser: Option<VersionedMeta>,
    device: Option<Device>,
    os: Option<VersionedMeta>,
}

#[derive(Debug, Deserialize, Serialize)]
struct VersionedMeta {
    name: String,
    version: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
struct User {
    id: Option<String>,
    username: Option<String>,
    email: Option<String>,
    ip_address: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
struct Requests {
    url: Option<String>,
    headers: Headers,
}

#[derive(Debug, Deserialize, Serialize)]
struct Headers {
    #[serde(rename = "User-Agent")]
    user_agent: String,
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};

    use crate::ReplayInput;

    #[test]
    fn test_set_ip_address() {
        let ip_address = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));

        // IP-Address was not set.
        let payload = include_bytes!("../tests/fixtures/replay.json");
        let mut replay_input: ReplayInput = serde_json::from_slice(payload).unwrap();
        replay_input.set_user_ip_address(ip_address);
        assert!("192.168.11.12".to_string() == replay_input.user.unwrap().ip_address.unwrap());

        // IP-Address set.
        let payload = include_bytes!("../tests/fixtures/replay_missing_user.json");
        let mut replay_input: ReplayInput = serde_json::from_slice(payload).unwrap();
        replay_input.set_user_ip_address(ip_address);
        assert!("127.0.0.1".to_string() == replay_input.user.unwrap().ip_address.unwrap());

        // IP-Address set.
        let payload = include_bytes!("../tests/fixtures/replay_missing_user_ip_address.json");
        let mut replay_input: ReplayInput = serde_json::from_slice(payload).unwrap();
        replay_input.set_user_ip_address(ip_address);
        assert!("127.0.0.1".to_string() == replay_input.user.unwrap().ip_address.unwrap());
    }

    #[test]
    fn test_set_user_agent_meta() {
        let payload = include_bytes!("../tests/fixtures/replay.json");
        let mut replay_input: ReplayInput = serde_json::from_slice(payload).unwrap();
        replay_input.set_user_agent_meta();

        match replay_input.contexts {
            Some(contexts) => {
                match contexts.browser {
                    Some(browser) => {
                        assert!(browser.name == "Safari".to_string());
                        assert!(browser.version.unwrap() == "15.5".to_string());
                    }
                    None => assert!(false),
                }
                match contexts.os {
                    Some(os) => {
                        assert!(os.name == "Mac OS X".to_string());
                        assert!(os.version.unwrap() == "10.15.7".to_string());
                    }
                    None => assert!(false),
                }
                match contexts.device {
                    Some(device) => {
                        assert!(device.family == "Mac".to_string());
                        assert!(device.brand.unwrap() == "Apple".to_string());
                        assert!(device.model.unwrap() == "Mac".to_string());
                    }
                    None => assert!(false),
                }
            }
            None => assert!(false),
        }
    }
}
