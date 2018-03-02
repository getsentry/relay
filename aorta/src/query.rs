use smith_common::ProjectId;

/// Represents the queries that can be issued over the aorta protocol.
#[derive(Serialize, Deserialize, Debug, PartialEq)]
#[serde(tag="type", content="data", rename_all="snake_case")]
pub enum Query {
    /// Requests the current project config.
    #[serde(rename_all="camelCase")]
    GetProjectConfig {
        project_id: ProjectId,
    }
}
