use serde::ser::Serialize;
use serde::de::DeserializeOwned;
use serde_json;

use smith_common::ProjectId;

use projectstate::ProjectStateSnapshot;

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct PackedQuery {
    #[serde(rename = "type")] pub ty: String,
    pub data: serde_json::Value,
}

pub trait AortaQuery: Serialize {
    type Response: DeserializeOwned + 'static;
    fn aorta_query_type(&self) -> &str;
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct GetProjectConfigQuery {
    pub project_id: ProjectId,
}

impl GetProjectConfigQuery {
    pub fn new(project_id: ProjectId) -> GetProjectConfigQuery {
        GetProjectConfigQuery {
            project_id: project_id,
        }
    }
}

impl AortaQuery for GetProjectConfigQuery {
    type Response = ProjectStateSnapshot;
    fn aorta_query_type(&self) -> &str {
        "get_project_config"
    }
}
