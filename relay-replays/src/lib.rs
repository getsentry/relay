//! Relay processing and normalization module.
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
//!     "sdk": {
//!         "name": "sdk-name",
//!         "version": null
//!     },
//!     "requests": {
//!         "headers": {"User-Agent": "Mozilla/5.0..."}
//!     },
//! }
//! ```
use relay_general::user_agent::{parse_device, parse_os, parse_user_agent, Device};
use serde::{Deserialize, Serialize};
use serde_json::Error;
use std::collections::HashMap;
use std::fmt::Write;

pub fn normalize_replay_event(replay_bytes: &[u8]) -> Result<Vec<u8>, Error> {
    let mut replay_input: ReplayInput = serde_json::from_slice(replay_bytes)?;
    replay_input.set_user_agent_meta();
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
    replay_start_timestamp: f64,
    urls: Vec<String>,
    error_ids: Vec<String>,
    trace_ids: Vec<String>,
    contexts: Option<Contexts>,
    dist: Option<String>,
    platform: Option<String>,
    environment: Option<String>,
    release: Option<String>,
    tags: Option<HashMap<String, String>>,
    user: Option<User>,
    sdk: VersionedMeta,
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
