use std::borrow::Cow;
use std::fmt;

use failure::Fail;
use hyper::Method;
use serde::de::DeserializeOwned;
use serde::ser::Serialize;

/// For well known types this can automatically define the API request to issue.
pub trait ApiRequest: Serialize {
    /// The response type.
    type Response: DeserializeOwned + 'static;

    /// Returns the target URL and method.
    fn get_aorta_request_target(&self) -> (Method, Cow<str>);
}

/// An error response from an api.
#[derive(Serialize, Deserialize, Default, Debug)]
pub struct ApiErrorResponse {
    detail: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    causes: Option<Vec<String>>,
}

impl ApiErrorResponse {
    /// Creates an error response with a detail message
    pub fn with_detail<S: AsRef<str>>(s: S) -> ApiErrorResponse {
        ApiErrorResponse {
            detail: Some(s.as_ref().to_string()),
            causes: None,
        }
    }

    /// Creates an error response from a fail.
    pub fn from_fail<F: Fail>(fail: &F) -> ApiErrorResponse {
        let mut messages = vec![];

        for cause in Fail::causes(fail) {
            let msg = cause.to_string();
            if !messages.contains(&msg) {
                messages.push(msg);
            }
        }

        ApiErrorResponse {
            detail: Some(messages.remove(0)),
            causes: if messages.is_empty() {
                None
            } else {
                Some(messages)
            },
        }
    }
}

impl fmt::Display for ApiErrorResponse {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Some(ref detail) = self.detail {
            write!(f, "{}", detail)
        } else {
            write!(f, "no error details")
        }
    }
}
