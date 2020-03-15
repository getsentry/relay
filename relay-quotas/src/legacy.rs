//! Conversions for the legacy quota schema.
//!
//! Requires the `legacy` feature.

// Required due to remote
#![allow(missing_docs)]

use serde::{ser::SerializeSeq, Deserialize, Deserializer, Serialize, Serializer};
use smallvec::smallvec;

use crate::types::{DataCategory, Quota, QuotaScope, ReasonCode};

/// Legacy format of the `Quota` type.
#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", remote = "Quota")]
pub struct LegacyQuota {
    #[serde(default, rename = "prefix", skip_serializing_if = "Option::is_none")]
    id: Option<String>,

    #[serde(default, rename = "subscope", skip_serializing_if = "Option::is_none")]
    scope_id: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    limit: Option<u32>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    window: Option<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    reason_code: Option<ReasonCode>,

    // Workaround to force the remote deserializer to use `From<LegacyQuota>`.
    #[serde(skip, getter = "Default::default")]
    _force_from: (),
}

impl From<LegacyQuota> for Quota {
    fn from(legacy: LegacyQuota) -> Self {
        let categories = smallvec![
            DataCategory::Default,
            DataCategory::Error,
            DataCategory::Security,
            // NB: For now, we allow transactions until Sentry emits those explicitly.
            DataCategory::Transaction,
        ];

        // Legacy quotas did not have a scope. However, the ids were all well-known to map to
        // certain scopes.
        let scope = match legacy.id.as_deref() {
            Some("k") => QuotaScope::Key,
            Some("p") => QuotaScope::Project,
            _ => QuotaScope::Organization,
        };

        Self {
            id: legacy.id,
            categories,
            scope,
            scope_id: legacy.scope_id,
            limit: legacy.limit,
            window: legacy.window,
            reason_code: legacy.reason_code,
        }
    }
}

/// Deserializes a list of `Quota` objects using `LegacyQuota`.
pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<Quota>, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    struct Wrapper(#[serde(with = "LegacyQuota")] Quota);

    let v = Vec::deserialize(deserializer)?;
    Ok(v.into_iter().map(|Wrapper(q)| q).collect())
}

/// Serializes a list of `Quota` objects using `LegacyQuota`.
pub fn serialize<S>(quotas: &[Quota], serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    #[derive(Serialize)]
    struct Wrapper<'a>(#[serde(with = "LegacyQuota")] &'a Quota);

    let mut seq = serializer.serialize_seq(Some(quotas.len()))?;
    for quota in quotas {
        seq.serialize_element(&Wrapper(quota))?;
    }
    seq.end()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_quota_reject_all() {
        let json = r#"{
            "limit": 0,
            "reasonCode": "not_yet"
        }"#;

        let de = &mut serde_json::Deserializer::from_str(json);
        let quota: Quota = LegacyQuota::deserialize(de).expect("parse quota");

        insta::assert_ron_snapshot!(quota, @r###"
        Quota(
          id: None,
          categories: [
            default,
            error,
            security,
            transaction,
          ],
          scope: organization,
          limit: Some(0),
          reasonCode: Some(ReasonCode("not_yet")),
        )
        "###);
    }

    #[test]
    fn test_parse_quota_limited() {
        let json = r#"{
            "prefix": "o",
            "limit": 4711,
            "window": 42,
            "reasonCode": "not_so_fast"
        }"#;

        let de = &mut serde_json::Deserializer::from_str(json);
        let quota: Quota = LegacyQuota::deserialize(de).expect("parse quota");

        insta::assert_ron_snapshot!(quota, @r###"
        Quota(
          id: Some("o"),
          categories: [
            default,
            error,
            security,
            transaction,
          ],
          scope: organization,
          limit: Some(4711),
          window: Some(42),
          reasonCode: Some(ReasonCode("not_so_fast")),
        )
        "###);
    }

    #[test]
    fn test_parse_quota_project() {
        let json = r#"{
            "prefix": "p",
            "subscope": "1",
            "limit": 4711,
            "window": 42,
            "reasonCode": "not_so_fast"
        }"#;

        let de = &mut serde_json::Deserializer::from_str(json);
        let quota: Quota = LegacyQuota::deserialize(de).expect("parse quota");

        insta::assert_ron_snapshot!(quota, @r###"
        Quota(
          id: Some("p"),
          categories: [
            default,
            error,
            security,
            transaction,
          ],
          scope: project,
          scopeId: Some("1"),
          limit: Some(4711),
          window: Some(42),
          reasonCode: Some(ReasonCode("not_so_fast")),
        )
        "###);
    }

    #[test]
    fn test_parse_quota_key() {
        let json = r#"{
            "prefix": "k",
            "subscope": "1",
            "limit": 4711,
            "window": 42,
            "reasonCode": "not_so_fast"
        }"#;

        let de = &mut serde_json::Deserializer::from_str(json);
        let quota: Quota = LegacyQuota::deserialize(de).expect("parse quota");

        insta::assert_ron_snapshot!(quota, @r###"
        Quota(
          id: Some("k"),
          categories: [
            default,
            error,
            security,
            transaction,
          ],
          scope: key,
          scopeId: Some("1"),
          limit: Some(4711),
          window: Some(42),
          reasonCode: Some(ReasonCode("not_so_fast")),
        )
        "###);
    }

    #[test]
    fn test_parse_quota_unlimited() {
        let json = r#"{
            "prefix": "o",
            "window": 42
        }"#;

        let de = &mut serde_json::Deserializer::from_str(json);
        let quota: Quota = LegacyQuota::deserialize(de).expect("parse quota");

        insta::assert_ron_snapshot!(quota, @r###"
        Quota(
          id: Some("o"),
          categories: [
            default,
            error,
            security,
            transaction,
          ],
          scope: organization,
          limit: None,
          window: Some(42),
        )
        "###);
    }
}
