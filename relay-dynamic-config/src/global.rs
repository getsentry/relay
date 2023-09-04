use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

/// A dynamic configuration for all Relays passed down from Sentry.
///
/// Values shared across all projects may also be included here, to keep
/// [`ProjectConfig`](crate::ProjectConfig)s small.
#[derive(Default, Clone, Debug, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct GlobalConfig {}

impl GlobalConfig {
    /// The full filename of the global config file, including the file extension.
    fn path(base: &Path) -> PathBuf {
        base.join("global_config.json")
    }

    /// Loads the [`GlobalConfig`] from a file if it's provided.
    pub fn load(base: &Path) -> anyhow::Result<Option<Self>> {
        let path = Self::path(base);

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
