use serde::{ser::SerializeSeq, Deserialize, Deserializer, Serialize, Serializer};

use crate::quotas::{DataCategory, Quota, QuotaScope};

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
    reason_code: Option<String>,

    // Workaround to force the remote deserializer to use `From<LegacyQuota>`.
    #[serde(skip, getter = "Default::default")]
    _force_from: (),
}

impl From<LegacyQuota> for Quota {
    fn from(legacy: LegacyQuota) -> Self {
        let categories = vec![
            DataCategory::Default,
            DataCategory::Error,
            DataCategory::Security,
            // TODO(ja): Attachments should probably not be part of this list. This requires Sentry
            // to emit attachment limits, however.
            DataCategory::Attachment,
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

pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<Quota>, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    struct Wrapper(#[serde(with = "LegacyQuota")] Quota);

    let v = Vec::deserialize(deserializer)?;
    Ok(v.into_iter().map(|Wrapper(q)| q).collect())
}

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
