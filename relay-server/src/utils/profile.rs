use bytes::Bytes;
use failure::Fail;

use android_trace_log::AndroidTraceLog;
use serde::{Deserialize, Serialize};

use crate::envelope::{ContentType, Item};

pub const ANDROID_SDK_NAME: &str = "sentry.java.android";

#[derive(Serialize, Deserialize)]
struct AndroidProfile {
    stacktrace: Bytes,
    android_trace: AndroidTraceLog,
}

#[derive(Debug, Fail)]
pub enum ProfileError {
    #[fail(display = "invalid json in profile")]
    InvalidJson(#[cause] serde_json::Error),
    #[fail(display = "invalid stack trace in profile")]
    InvalidStackTrace,
    #[fail(display = "cannot serialize payload")]
    CannotSerializePayload,
}

pub fn expand_profile_envelope(item: &mut Item) -> Result<(), ProfileError> {
    let payload = item.payload();
    let profile: AndroidProfile = match serde_json::from_slice(&payload) {
        Ok(profile) => profile,
        Err(err) => return Err(ProfileError::InvalidJson(err)),
    };

    let trace = match android_trace_log::parse(&profile.stacktrace) {
        Ok(trace) => trace,
        Err(_) => return Err(ProfileError::InvalidStackTrace),
    };

    match serde_json::to_vec(&AndroidProfile {
        stacktrace: profile.stacktrace,
        android_trace: trace,
    }) {
        Ok(payload) => item.set_payload(ContentType::Json, &payload[..]),
        Err(_) => return Err(ProfileError::CannotSerializePayload),
    };

    Ok(())
}
