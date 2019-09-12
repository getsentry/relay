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

impl OutcomeInvalidReason {
    pub fn name(self) -> &'static str {
        match self {
            OutcomeInvalidReason::Duplicate => "duplicate",
            OutcomeInvalidReason::ProjectId => "project_id",
            OutcomeInvalidReason::AuthVersion => "auth_version",
            OutcomeInvalidReason::AuthClient => "auth_client",
            OutcomeInvalidReason::NoData => "no_data",
            OutcomeInvalidReason::TooLarge => "too_large",
            OutcomeInvalidReason::DisallowedMethod => "disallowed_method",
            OutcomeInvalidReason::ContentType => "content_type",
            OutcomeInvalidReason::MultiProjectId => "multi_project_id",
            OutcomeInvalidReason::MissingMinidumpUpload => "missing_minidump_upload",
            OutcomeInvalidReason::InvalidMinidump => "invalid_minidump",
            OutcomeInvalidReason::SecurityReportType => "security_report_type",
            OutcomeInvalidReason::SecurityReport => "security_report",
            OutcomeInvalidReason::Cors => "cors",
        }
    }
}

/// Unified reason type
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Hash)]
#[serde(untagged)]
pub enum OutcomeReason {
    FilterReason(FilterStatKey),
    InvalidReason(OutcomeInvalidReason),
    RateLimited(String),
}

impl From<OutcomeInvalidReason> for OutcomeReason {
    fn from(reason: OutcomeInvalidReason) -> OutcomeReason {
        OutcomeReason::InvalidReason(reason)
    }
}

impl From<FilterStatKey> for OutcomeReason {
    fn from(reason: FilterStatKey) -> OutcomeReason {
        OutcomeReason::FilterReason(reason)
    }
}

impl From<&str> for OutcomeReason {
    fn from(reason: &str) -> OutcomeReason {
        OutcomeReason::RateLimited(reason.to_string())
    }
}

impl From<&String> for OutcomeReason {
    fn from(reason: &String) -> OutcomeReason {
        OutcomeReason::RateLimited(reason.clone())
    }
}

impl OutcomeReason {
    pub fn name(&self) -> &str {
        match self {
            OutcomeReason::FilterReason(stat) => stat.name(),
            OutcomeReason::InvalidReason(inv_reason) => inv_reason.name(),
            OutcomeReason::RateLimited(val) => val.as_str(),
        }
    }
}
