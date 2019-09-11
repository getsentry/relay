//! Contains the

use serde::Serialize;

use crate::filter::FilterStatKey;

/// Taken from Sentry's outcome reasons for invalid outcomes.
/// Unlike FilterStatKey these are serialized with snake_case (e.g. project_id)
#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize, Hash)]
#[serde(rename_all = "snake_case")]
pub enum OutcomeInvalidReason {
    Duplicate,
    ProjectId,
    AuthVersion,
    AuthClient,
    NoData,
    TooLarge,
    DisallowedMethod,
    ContentType,
    MultiProjectId,
    MissingMinidumpUpload,
    InvalidMinidump,
    SecurityReportType,
    SecurityReport,
    Cors,
}

/// Unified reason type
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Hash)]
#[serde(untagged)]
pub enum OutcomeReason {
    FilterReason(FilterStatKey),
    InvalidReason(OutcomeInvalidReason),
    RateLimited(String),
}

impl OutcomeReason {
    pub fn name(&self) -> &'static str {
        "Hello"
    }
}
