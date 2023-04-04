use std::error::Error;
use std::fmt;

use axum::response::IntoResponse;
use serde::{Deserialize, Serialize};

/// Represents an action requested by the Upstream sent in an error message.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub enum RelayErrorAction {
    Stop,
    #[serde(other)]
    #[default]
    None,
}

impl RelayErrorAction {
    fn is_none(&self) -> bool {
        *self == Self::None
    }
}

/// An error response from an api.
#[derive(Serialize, Deserialize, Default, Debug)]
pub struct ApiErrorResponse {
    #[serde(default)]
    detail: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    causes: Vec<String>,
    #[serde(default, skip_serializing_if = "RelayErrorAction::is_none")]
    relay: RelayErrorAction,
}

impl ApiErrorResponse {
    /// Creates an error response with a detail message
    pub fn with_detail<S: AsRef<str>>(s: S) -> ApiErrorResponse {
        ApiErrorResponse {
            detail: Some(s.as_ref().to_string()),
            causes: Vec::new(),
            relay: RelayErrorAction::None,
        }
    }

    pub fn from_error<E: Error + ?Sized>(error: &E) -> Self {
        let detail = Some(error.to_string());

        let mut causes = Vec::new();
        let mut source = error.source();
        while let Some(s) = source {
            causes.push(s.to_string());
            source = s.source();
        }

        Self {
            detail,
            causes,
            relay: RelayErrorAction::None,
        }
    }

    pub fn relay_action(&self) -> RelayErrorAction {
        self.relay
    }
}

impl fmt::Display for ApiErrorResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(ref detail) = self.detail {
            write!(f, "{detail}")
        } else {
            write!(f, "no error details")
        }
    }
}

impl Error for ApiErrorResponse {}

impl IntoResponse for ApiErrorResponse {
    fn into_response(self) -> axum::response::Response {
        axum::Json(self).into_response()
    }
}
