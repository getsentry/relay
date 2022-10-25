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
//!     "request": {
//!         "headers": {"User-Agent": "Mozilla/5.0..."}
//!     },
//! }
//! ```
use std::fmt::Write;
use std::net::IpAddr;

use serde::{Deserialize, Serialize};
use serde_json::{Error, Value};

use relay_general::user_agent;

/// Validates and accepts a replay-event item as input and returns modified output.
///
/// The output of this function will return a valid replay-event payload with the addition of
/// browser, device, and operating-system metadata.  The output will also contain a default
/// ip-address value for the user if none was provided.
pub fn normalize_replay_event(
    replay_bytes: &[u8],
    detected_ip_address: Option<IpAddr>,
) -> Result<Vec<u8>, Error> {
    let mut replay_input: ReplayInput = serde_json::from_slice(replay_bytes)?;

    // Set user-agent metadata from request object.
    replay_input.set_user_agent_meta();

    // Set user ip-address if needed.
    if let Some(ip_address) = detected_ip_address {
        replay_input.set_user_ip_address(ip_address)
    }

    serde_json::to_vec(&replay_input)
}

#[derive(Debug, Default, Deserialize, Serialize)]
struct ReplayInput {
    #[serde(rename = "type")]
    ty: String,
    replay_id: String,
    event_id: String,
    segment_id: u16,
    timestamp: f64,
    replay_start_timestamp: Option<f64>,
    contexts: Option<Contexts>,
    dist: Option<String>,
    platform: Option<String>,
    environment: Option<String>,
    release: Option<String>,
    tags: Option<Value>,
    sdk: Option<VersionedMeta>,
    #[serde(default)]
    urls: Vec<String>,
    #[serde(default)]
    error_ids: Vec<String>,
    #[serde(default)]
    trace_ids: Vec<String>,
    #[serde(default)]
    user: User,
    #[serde(default)]
    request: Request,
}

impl ReplayInput {
    fn set_user_agent_meta(&mut self) {
        let user_agent = &self.request.headers.user_agent;

        let ua = user_agent::parse_user_agent(user_agent);
        let browser_struct = VersionedMeta {
            name: ua.family,
            version: get_version(&ua.major, &ua.minor, &ua.patch),
        };

        let os = user_agent::parse_os(user_agent);
        let os_struct = VersionedMeta {
            name: os.family,
            version: get_version(&os.major, &os.minor, &os.patch),
        };

        self.contexts = Some(Contexts {
            device: Some(user_agent::parse_device(user_agent)),
            browser: Some(browser_struct),
            os: Some(os_struct),
        })
    }

    fn set_user_ip_address(&mut self, ip_address: IpAddr) {
        self.user
            .ip_address
            .get_or_insert_with(|| ip_address.to_string());
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
    device: Option<user_agent::Device>,
    os: Option<VersionedMeta>,
}

#[derive(Debug, Deserialize, Serialize)]
struct VersionedMeta {
    name: String,
    version: Option<String>,
}

#[derive(Debug, Default, Deserialize, Serialize)]
struct User {
    id: Option<Value>,
    username: Option<Value>,
    email: Option<Value>,
    ip_address: Option<String>,
}

#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(default)]
struct Request {
    url: Option<String>,
    headers: Headers,
}

#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(default)]
struct Headers {
    #[serde(rename = "User-Agent")]
    user_agent: String,
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};

    use crate::{normalize_replay_event, ReplayInput};

    #[test]
    fn test_set_ip_address() {
        let ip_address = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));

        // IP-Address was not set.
        let payload = include_bytes!("../tests/fixtures/replay.json");
        let mut replay_input: ReplayInput = serde_json::from_slice(payload).unwrap();
        replay_input.set_user_ip_address(ip_address);
        assert!(*"192.168.11.12" == replay_input.user.ip_address.unwrap());
    }

    #[test]
    fn test_set_ip_address_missing_user() {
        let ip_address = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));

        // IP-Address set.
        let payload = include_bytes!("../tests/fixtures/replay_missing_user.json");
        let mut replay_input: ReplayInput = serde_json::from_slice(payload).unwrap();
        replay_input.set_user_ip_address(ip_address);
        assert!(*"127.0.0.1" == replay_input.user.ip_address.unwrap());
    }

    #[test]
    fn test_set_ip_address_missing_user_ip_address() {
        let ip_address = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));

        // IP-Address set.
        let payload = include_bytes!("../tests/fixtures/replay_missing_user_ip_address.json");
        let mut replay_input: ReplayInput = serde_json::from_slice(payload).unwrap();
        replay_input.set_user_ip_address(ip_address);
        assert!(*"127.0.0.1" == replay_input.user.ip_address.unwrap());
    }

    #[test]
    fn test_set_user_agent_meta() {
        let payload = include_bytes!("../tests/fixtures/replay.json");
        let mut replay_input: ReplayInput = serde_json::from_slice(payload).unwrap();
        replay_input.set_user_agent_meta();

        let contexts = replay_input.contexts.unwrap();

        let browser = contexts.browser.unwrap();
        assert!(browser.name == *"Safari");
        assert!(browser.version.unwrap() == *"15.5");

        let os = contexts.os.unwrap();
        assert!(os.name == *"Mac OS X");
        assert!(os.version.unwrap() == *"10.15.7");

        let device = contexts.device.unwrap();
        assert!(device.family == *"Mac");
        assert!(device.brand.unwrap() == *"Apple");
        assert!(device.model.unwrap() == *"Mac");
    }

    #[test]
    fn test_set_user_agent_meta_no_request() {
        let payload = include_bytes!("../tests/fixtures/replay_no_requests.json");
        let mut replay_input: ReplayInput = serde_json::from_slice(payload).unwrap();
        replay_input.set_user_agent_meta();

        let contexts = replay_input.contexts.unwrap();

        let browser = contexts.browser.unwrap();
        assert!(browser.name == *"Other");
        assert!(browser.version.is_none());

        let os = contexts.os.unwrap();
        assert!(os.name == *"Other");
        assert!(os.version.is_none());

        let device = contexts.device.unwrap();
        assert!(device.family == *"Other");
        assert!(device.brand.is_none());
        assert!(device.model.is_none());
    }

    #[test]
    fn test_loose_type_requirements() {
        let payload = include_bytes!("../tests/fixtures/replay_failure_22_08_31.json");
        let result = normalize_replay_event(payload, None);
        assert!(result.is_ok());

        // Assert the user payload is just generally unaffected by parsing.
        let replay_output: ReplayInput = serde_json::from_slice(&result.unwrap()).unwrap();
        assert!(replay_output.user.ip_address.unwrap() == *"127.1.1.1");
        assert!(replay_output.user.username.is_none());
        assert!(replay_output.user.email.unwrap() == *"email@sentry.io");
        assert!(replay_output.user.id.unwrap().is_number());
    }
}
