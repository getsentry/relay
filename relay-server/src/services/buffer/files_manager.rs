use crate::services::buffer::common::ProjectKeyPair;
use hashbrown::{HashMap, HashSet};
use relay_base_schema::project::{ParseProjectKeyError, ProjectKey};
use relay_config::Config;
use std::path::{Path, PathBuf};
use std::time::Instant;
use tokio::fs::{read_dir, DirBuilder, File, OpenOptions};
use tokio::io;

const FILE_EXTENSION: &str = "spool";

/// An error returned when doing an operation on [`FilesManager`].
#[derive(Debug, thiserror::Error)]
pub enum FilesManagerError {
    #[error("failed work with a file: {0}")]
    FileError(#[from] io::Error),

    #[error("failed to create the spool file: {0}")]
    FileSetupError(io::Error),

    #[error("no file path for the spool was provided")]
    NoFilePath,

    #[error("failed to parse project key: {0}")]
    ProjectKeyParseError(#[from] ParseProjectKeyError),
}

#[derive(Debug)]
pub struct FilesManager {
    base_path: PathBuf,
    max_opened_files: usize,
    cache: HashMap<ProjectKeyPair, CacheEntry>,
}

#[derive(Debug)]
struct CacheEntry {
    file: File,
    last_access: Instant,
}

impl FilesManager {
    pub async fn new(config: &Config) -> Result<Self, FilesManagerError> {
        // If no path is provided, we can't do disk spooling.
        let Some(base_path) = config.spool_envelopes_path() else {
            return Err(FilesManagerError::NoFilePath);
        };

        Ok(FilesManager {
            base_path,
            max_opened_files: config.spool_envelopes_max_opened_files(),
            cache: HashMap::new(),
        })
    }

    pub async fn get_file(
        &mut self,
        project_key_pair: ProjectKeyPair,
    ) -> Result<&mut File, FilesManagerError> {
        if !self.cache.contains_key(&project_key_pair) {
            let file = Self::load_or_create_file(self.base_path.clone(), &project_key_pair).await?;
            self.insert_into_cache(project_key_pair, file);
        }

        let cache_entry = self
            .cache
            .get_mut(&project_key_pair)
            .expect("file to be in the cache");

        Ok(&mut cache_entry.file)
    }

    pub async fn list_project_key_pairs(
        &self,
    ) -> Result<HashSet<ProjectKeyPair>, FilesManagerError> {
        let mut project_key_pairs = HashSet::new();

        let mut dir = read_dir(&self.base_path).await?;
        while let Some(entry) = dir.next_entry().await? {
            let path = entry.path();

            if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some(FILE_EXTENSION) {
                if let Some(file_name) = path.file_stem().and_then(|s| s.to_str()) {
                    if let Some((own_key, sampling_key)) = file_name.split_once('-') {
                        project_key_pairs.insert(ProjectKeyPair {
                            own_key: ProjectKey::parse(own_key)?,
                            sampling_key: ProjectKey::parse(sampling_key)?,
                        });
                    }
                }
            }
        }

        Ok(project_key_pairs)
    }

    async fn load_or_create_file(
        base_path: PathBuf,
        key_pair: &ProjectKeyPair,
    ) -> Result<File, FilesManagerError> {
        let filename = format!(
            "{}-{}.{}",
            key_pair.own_key, key_pair.sampling_key, FILE_EXTENSION
        );

        let filepath = base_path.join(filename);
        Self::create_spool_directory(&filepath).await?;

        OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(filepath)
            .await
            .map_err(FilesManagerError::FileError)
    }

    /// Creates the directories for the spool file.
    async fn create_spool_directory(path: &Path) -> Result<(), FilesManagerError> {
        let Some(parent) = path.parent() else {
            return Ok(());
        };

        if !parent.as_os_str().is_empty() && !parent.exists() {
            relay_log::debug!("creating directory for spooling file: {}", parent.display());
            DirBuilder::new()
                .recursive(true)
                .create(&parent)
                .await
                .map_err(FilesManagerError::FileSetupError)?;
        }

        Ok(())
    }

    fn insert_into_cache(&mut self, key_pair: ProjectKeyPair, file: File) {
        if self.cache.len() >= self.max_opened_files {
            self.evict_lru();
        }

        self.cache.insert(
            key_pair,
            CacheEntry {
                file,
                last_access: Instant::now(),
            },
        );
    }

    fn evict_lru(&mut self) {
        if let Some(lru_project_key_pair) = self
            .cache
            .iter()
            .min_by_key(|(_, entry)| entry.last_access)
            .map(|(&key, _)| key)
        {
            self.cache.remove(&lru_project_key_pair);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::prelude::MetadataExt;
    use std::sync::Arc;
    use uuid::Uuid;

    fn mock_config(path: &str, max_opened_files: usize) -> Arc<Config> {
        Config::from_json_value(serde_json::json!({
            "spool": {
                "envelopes": {
                    "path": path,
                    "max_opened_files": max_opened_files
                }
            }
        }))
        .unwrap()
        .into()
    }

    async fn setup_files_manager(max_opened_files: usize) -> FilesManager {
        let path = std::env::temp_dir()
            .join(Uuid::new_v4().to_string())
            .into_os_string()
            .into_string()
            .unwrap();
        let config = mock_config(&path, max_opened_files);
        FilesManager::new(&config)
            .await
            .expect("Failed to create FilesManager")
    }

    #[tokio::test]
    async fn test_create_evict_load() {
        let mut files_manager = setup_files_manager(5).await;
        let project_key_pair = ProjectKeyPair {
            own_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            sampling_key: ProjectKey::parse("b94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
        };

        // First call should create the file
        let file_ino = files_manager
            .get_file(project_key_pair)
            .await
            .unwrap()
            .metadata()
            .await
            .unwrap()
            .ino();

        // We evict the file to see if re-opening gives the same ino.
        files_manager.evict_lru();
        assert!(files_manager.cache.is_empty());

        // Second call should load the file from disk since it was evicted
        let cached_file_ino = files_manager
            .get_file(project_key_pair)
            .await
            .unwrap()
            .metadata()
            .await
            .unwrap()
            .ino();
        assert_eq!(file_ino, cached_file_ino);
    }

    #[tokio::test]
    async fn test_list_project_key_pairs() {
        let mut files_manager = setup_files_manager(5).await;
        let project_key_pair1 = ProjectKeyPair {
            own_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            sampling_key: ProjectKey::parse("b94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
        };
        let project_key_pair2 = ProjectKeyPair {
            own_key: ProjectKey::parse("c94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            sampling_key: ProjectKey::parse("d94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
        };

        // Create two files
        files_manager.get_file(project_key_pair1).await.unwrap();
        files_manager.get_file(project_key_pair2).await.unwrap();

        // List project key pairs
        let key_pairs = files_manager.list_project_key_pairs().await.unwrap();
        assert_eq!(key_pairs.len(), 2);
        assert!(key_pairs.contains(&project_key_pair1));
        assert!(key_pairs.contains(&project_key_pair2));
    }

    #[tokio::test]
    async fn test_cache_eviction() {
        let mut files_manager = setup_files_manager(5).await;

        // Create 6 files (max_size is 5)
        for i in 0..6 {
            let project_key_pair = ProjectKeyPair {
                own_key: ProjectKey::parse(&format!("c{}4ae32be2584e0bbd7a4cbb95971fee", i))
                    .unwrap(),
                sampling_key: ProjectKey::parse(&format!("c{}4ae32be2584e0bbd7a4cbb95971fee", i))
                    .unwrap(),
            };
            files_manager.get_file(project_key_pair).await.unwrap();
        }

        // Check that the cache size is still 5
        assert_eq!(files_manager.cache.len(), 5);

        // The first file should have been evicted
        let first_key_pair = ProjectKeyPair {
            own_key: ProjectKey::parse("c04ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            sampling_key: ProjectKey::parse("c04ae32be2584e0bbd7a4cbb95971fee").unwrap(),
        };
        assert!(!files_manager.cache.contains_key(&first_key_pair));
    }
}
