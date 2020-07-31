use std::fmt;

use failure::Fail;
use serde::{Deserialize, Serialize};

/// An error response from an api.
#[derive(Serialize, Deserialize, Default, Debug, Fail)]
pub struct ApiErrorResponse {
    #[serde(default)]
    detail: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    causes: Vec<String>,
}

impl ApiErrorResponse {
    /// Creates an error response with a detail message
    pub fn with_detail<S: AsRef<str>>(s: S) -> ApiErrorResponse {
        ApiErrorResponse {
            detail: Some(s.as_ref().to_string()),
            causes: Vec::new(),
        }
    }

    /// Creates an error response from a fail.
    pub fn from_fail<F: Fail>(fail: &F) -> ApiErrorResponse {
        let mut messages = vec![];

        for cause in Fail::iter_chain(fail) {
            let msg = cause.to_string();
            if !messages.contains(&msg) {
                messages.push(msg);
            }
        }

        ApiErrorResponse {
            detail: Some(messages.remove(0)),
            causes: messages,
        }
    }
}

impl fmt::Display for ApiErrorResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(ref detail) = self.detail {
            write!(f, "{}", detail)
        } else {
            write!(f, "no error details")
        }
    }
}
