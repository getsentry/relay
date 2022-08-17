use relay_general::user_agent::{parse_device, parse_os, parse_user_agent};
use serde::{Deserialize, Serialize};
use serde_json::Error;
use std::collections::HashMap;
use std::fmt::Write;

pub fn parse_replay_event(replay_bytes: &[u8]) -> Result<Vec<u8>, Error> {
    let replay_input: Result<ReplayInput, Error> = serde_json::from_slice(&replay_bytes);
    match replay_input {
        Ok(mut replay_in) => {
            replay_in.set_user_agent_meta(); // breaks because of UA parsing :O
            return serde_json::to_vec(&replay_in);
        }
        Err(e) => Err(e),
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ReplayInput {
    #[serde(rename = "type")]
    type_: String,
    replay_id: String,
    event_id: String,
    segment_id: u8,
    timestamp: f64,
    replay_start_timestamp: f64,
    urls: Vec<String>,
    error_ids: Vec<String>,
    trace_ids: Vec<String>,
    contexts: Option<Contexts>,
    dist: String,
    platform: String,
    environment: String,
    release: String,
    tags: HashMap<String, String>,
    user: User,
    sdk: VersionedMeta,
    requests: Requests,
}

impl ReplayInput {
    pub fn set_user_agent_meta(&mut self) {
        let user_agent = &self.requests.headers.user_agent;

        let device = parse_device(&user_agent);
        let device_struct = Device {
            family: device.family,
            brand: device.brand,
            model: device.model,
        };

        let ua = parse_user_agent(&user_agent);
        let browser_struct = VersionedMeta {
            name: ua.family,
            version: get_version(&ua.major, &ua.minor, &ua.patch),
        };

        let os = parse_os(&user_agent);
        let os_struct = VersionedMeta {
            name: os.family,
            version: get_version(&os.major, &os.minor, &os.patch),
        };

        self.contexts = Some(Contexts {
            device: Some(device_struct),
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
struct Device {
    brand: Option<String>,
    model: Option<String>,
    family: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct User {
    id: String,
    username: String,
    email: String,
    ip_address: String,
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
#[test]
fn test_deserialize_replay() {
    let json = br#"{
    "type": "replay_event",
    "replay_id": "123",
    "segment_id": 1,
    "timestamp": 1.23,
    "replay_start_timestamp": 1.20,
    "urls": ["sentry.io"],
    "error_ids": ["1", "2"],
    "trace_ids": ["3", "4"],
    "dist": "dist",
    "platform": "platform",
    "environment": "environment",
    "release": "release",
    "contexts": {"test": "testing"},
    "tags": {
        "a": "b",
        "c": "d"
    },
    "user": {
        "id": "1",
        "username": "user",
        "email": "user@site.com",
        "ip_address": "127.0.0.1"
    },
    "sdk": {
        "name": "sdk-name",
        "version": "12.0.1"
    },
    "requests": {
        "url": null,
        "headers": {"User-Agent": "Firefox"}
    }
}"#;
    // Convert the JSON string back to a Point.
    let parsed_replay = parse_replay_event(&json.to_vec());
    println!("serialized = {:?}", parsed_replay);
    assert_eq!(false, true);
    // let output: serde_json::Value = serde_json::from_str(&parsed_replay).unwrap();
    // let serialized = serde_json::to_string(&deserialized).unwrap();
    // println!("json = {:?}", json);
}
