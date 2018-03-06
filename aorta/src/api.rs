use std::fmt;
use std::borrow::Cow;

use hyper::Method;
use serde::ser::Serialize;
use serde::de::DeserializeOwned;

/// For well known types this can automatically define the API request to issue.
pub trait ApiRequest: Serialize {
    /// The response type.
    type Response: DeserializeOwned + 'static;

    /// Returns the target URL and method.
    fn get_aorta_request_target<'a>(&'a self) -> (Method, Cow<'a, str>);
}

/// An error response from an api.
#[derive(Serialize, Deserialize, Default, Debug)]
pub struct ApiErrorResponse {
    detail: Option<String>,
}

impl ApiErrorResponse {
    /// Creates an error response with a detail message
    pub fn with_detail<S: AsRef<str>>(s: S) -> ApiErrorResponse {
        ApiErrorResponse {
            detail: Some(s.as_ref().to_string()),
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
