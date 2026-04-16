use std::sync::Arc;

use relay_config::UpstreamDescriptor;
use serde::{Deserialize, Serialize};

use crate::services::projects::project::{LimitedProjectInfo, ProjectInfo};

/// Project state as used in de-serialization.
///
/// Use [`SerializeProjectState`] to serialize a project state.
#[derive(Debug, Clone, Deserialize)]
pub struct ParsedProjectState {
    /// Whether the project state is disabled.
    #[serde(default)]
    pub disabled: bool,
    /// Project info.
    ///
    /// This contains no information when `disabled` is `true`.
    #[serde(flatten)]
    pub info: Arc<ProjectInfo>,
}

/// Project state as used in serialization.
///
/// Use [`ParsedProjectState`] to de-serialize a project state.
#[derive(Debug, Clone, Serialize)]
pub struct SerializeProjectState {
    /// Whether the project state is disabled.
    #[serde(default)]
    pub disabled: bool,
    /// Project info.
    ///
    /// This contains no information when `disabled` is `true`.
    #[serde(flatten)]
    pub info: Arc<ProjectInfo>,
    /// Upstream override to use by the downstream Relay receiving the project state.
    ///
    /// See also: [`ProjectInfo::upstream`].
    #[serde(skip_serializing_if = "Option::is_none")]
    pub upstream: Option<UpstreamDescriptor>,
}

/// Limited project state for external Relays.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase", remote = "SerializeProjectState")]
pub struct LimitedSerializeProjectState {
    /// Whether the project state is disabled.
    pub disabled: bool,
    /// Limited project info for external Relays.
    ///
    /// This contains no information when `disabled` is `true`.
    #[serde(with = "LimitedProjectInfo")]
    #[serde(flatten)]
    pub info: Arc<ProjectInfo>,
    /// Upstream override to use by the downstream Relay receiving the project state.
    ///
    /// See also: [`ProjectInfo::upstream`].
    #[serde(skip_serializing_if = "Option::is_none")]
    pub upstream: Option<UpstreamDescriptor>,
}

#[cfg(test)]
mod tests {
    use insta::assert_json_snapshot;

    use super::*;

    #[derive(Debug, Serialize)]
    #[serde(transparent)]
    struct Limited(#[serde(with = "LimitedSerializeProjectState")] SerializeProjectState);

    #[test]
    fn test_serialize_full_no_upstream() {
        let s = SerializeProjectState {
            disabled: false,
            info: Default::default(),
            upstream: None,
        };

        assert_json_snapshot!(s, @r#"
        {
          "disabled": false,
          "projectId": null,
          "lastChange": null,
          "rev": null,
          "publicKeys": [],
          "slug": null,
          "config": {
            "allowedDomains": [
              "*"
            ],
            "trustedRelays": [],
            "piiConfig": null
          },
          "organizationId": null
        }
        "#);

        assert_json_snapshot!(Limited(s), @r#"
        {
          "disabled": false,
          "projectId": null,
          "lastChange": null,
          "rev": null,
          "publicKeys": [],
          "slug": null,
          "config": {
            "allowedDomains": [
              "*"
            ],
            "trustedRelays": [],
            "piiConfig": null
          },
          "organizationId": null
        }
        "#);
    }

    #[test]
    fn test_serialize_full_project_info_upstream_ignored() {
        let s = SerializeProjectState {
            disabled: false,
            info: Arc::new(ProjectInfo {
                upstream: Some("https://sentry.io".parse().unwrap()),
                ..Default::default()
            }),
            upstream: None,
        };

        assert_json_snapshot!(s, @r#"
        {
          "disabled": false,
          "projectId": null,
          "lastChange": null,
          "rev": null,
          "publicKeys": [],
          "slug": null,
          "config": {
            "allowedDomains": [
              "*"
            ],
            "trustedRelays": [],
            "piiConfig": null
          },
          "organizationId": null
        }
        "#);

        assert_json_snapshot!(Limited(s), @r#"
        {
          "disabled": false,
          "projectId": null,
          "lastChange": null,
          "rev": null,
          "publicKeys": [],
          "slug": null,
          "config": {
            "allowedDomains": [
              "*"
            ],
            "trustedRelays": [],
            "piiConfig": null
          },
          "organizationId": null
        }
        "#);
    }

    #[test]
    fn test_serialize_full_correct_upstream_used() {
        let s = SerializeProjectState {
            disabled: false,
            info: Arc::new(ProjectInfo {
                upstream: Some("https://sentry.io".parse().unwrap()),
                ..Default::default()
            }),
            upstream: Some("https://us.sentry.io".parse().unwrap()),
        };

        assert_json_snapshot!(s, @r#"
        {
          "disabled": false,
          "projectId": null,
          "lastChange": null,
          "rev": null,
          "publicKeys": [],
          "slug": null,
          "config": {
            "allowedDomains": [
              "*"
            ],
            "trustedRelays": [],
            "piiConfig": null
          },
          "organizationId": null,
          "upstream": "https://us.sentry.io/"
        }
        "#);

        assert_json_snapshot!(Limited(s), @r#"
        {
          "disabled": false,
          "projectId": null,
          "lastChange": null,
          "rev": null,
          "publicKeys": [],
          "slug": null,
          "config": {
            "allowedDomains": [
              "*"
            ],
            "trustedRelays": [],
            "piiConfig": null
          },
          "organizationId": null,
          "upstream": "https://us.sentry.io/"
        }
        "#);
    }

    #[test]
    fn test_serialize_full_upstream_used() {
        let s = SerializeProjectState {
            disabled: false,
            info: Default::default(),
            upstream: Some("https://us.sentry.io".parse().unwrap()),
        };

        assert_json_snapshot!(s, @r#"
        {
          "disabled": false,
          "projectId": null,
          "lastChange": null,
          "rev": null,
          "publicKeys": [],
          "slug": null,
          "config": {
            "allowedDomains": [
              "*"
            ],
            "trustedRelays": [],
            "piiConfig": null
          },
          "organizationId": null,
          "upstream": "https://us.sentry.io/"
        }
        "#);

        assert_json_snapshot!(Limited(s), @r#"
        {
          "disabled": false,
          "projectId": null,
          "lastChange": null,
          "rev": null,
          "publicKeys": [],
          "slug": null,
          "config": {
            "allowedDomains": [
              "*"
            ],
            "trustedRelays": [],
            "piiConfig": null
          },
          "organizationId": null,
          "upstream": "https://us.sentry.io/"
        }
        "#);
    }
}
