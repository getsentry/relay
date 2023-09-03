use serde::{Deserialize, Serialize};

/// A dynamic configuration for all Relays passed down from Sentry.
///
/// Values shared across all projects may also be included here, to keep
/// [`ProjectConfig`](crate::ProjectConfig)s small.
#[derive(Default, Clone, Debug, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct GlobalConfig {}

impl GlobalConfig {
    /// load it from a file
    pub fn from_file() -> anyhow::Result<Option<GlobalConfig>> {
        let path = std::env::current_dir()?
            .join(".relay")
            .join("global_config.json");

        match path.exists() {
            true => {
                let file_contents = std::fs::read_to_string(path)?;
                let global_config = serde_json::from_str::<GlobalConfig>(file_contents.as_str())?;

                Ok(Some(global_config))
            }
            false => Ok(None),
        }
    }
}
