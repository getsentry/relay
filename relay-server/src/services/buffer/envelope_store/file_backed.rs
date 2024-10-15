use crate::services::buffer::common::ProjectKeyPair;
use hashbrown::{HashMap, HashSet};
use relay_base_schema::project::{ParseProjectKeyError, ProjectKey};
use relay_config::Config;
use std::path::{Path, PathBuf};
use std::time::Instant;
use tokio::fs::{read_dir, remove_file, DirBuilder, File, OpenOptions};
use tokio::io;

const FILE_EXTENSION: &str = "spool";

/// An error returned when doing an operation on [`FileBackedEnvelopeStore`].
#[derive(Debug, thiserror::Error)]
pub enum FileBackedEnvelopeStoreError {
    #[error("failed work with a file: {0}")]
    FileError(#[from] io::Error),

    #[error("failed to create the spool file: {0}")]
    FileSetupError(io::Error),

    #[error("no file path for the spool was provided")]
    NoFilePath,

    #[error("failed to parse project key: {0}")]
    ProjectKeyParseError(#[from] ParseProjectKeyError),
}

/// A file-backed envelope store that manages envelope files on disk.
#[derive(Debug)]
pub struct FileBackedEnvelopeStore {
    base_path: PathBuf,
    files_cache: EnvelopesFilesCache,
}

/// A cache for managing open file handles which contain envelopes.
#[derive(Debug)]
struct EnvelopesFilesCache {
    max_opened_files: usize,
    cache: HashMap<ProjectKeyPair, CacheEntry>,
}

#[derive(Debug)]
struct CacheEntry {
    file: File,
    last_access: Instant,
}

impl FileBackedEnvelopeStore {
    /// Creates a new `FileBackedEnvelopeStore` instance.
    pub async fn new(config: &Config) -> Result<Self, FileBackedEnvelopeStoreError> {
        let Some(base_path) = config.spool_envelopes_path() else {
            return Err(FileBackedEnvelopeStoreError::NoFilePath);
        };

        Ok(FileBackedEnvelopeStore {
            base_path,
            files_cache: EnvelopesFilesCache::new(config.spool_envelopes_max_opened_files()),
        })
    }

    /// Retrieves or creates an envelope file for the given project key pair.
    pub async fn get_envelopes_file(
        &mut self,
        project_key_pair: ProjectKeyPair,
    ) -> Result<&mut File, FileBackedEnvelopeStoreError> {
        self.files_cache
            .get_file(project_key_pair, &self.base_path)
            .await
    }

    /// Lists all project key pairs that have envelope files on disk.
    pub async fn list_project_key_pairs(
        &self,
    ) -> Result<HashSet<ProjectKeyPair>, FileBackedEnvelopeStoreError> {
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

    /// Removes the envelope file associated with the given project key pair.
    pub async fn remove_file(
        &mut self,
        project_key_pair: &ProjectKeyPair,
    ) -> Result<(), FileBackedEnvelopeStoreError> {
        self.files_cache
            .remove(project_key_pair, &self.base_path)
            .await
    }
}

impl EnvelopesFilesCache {
    fn new(max_opened_files: usize) -> Self {
        EnvelopesFilesCache {
            max_opened_files,
            cache: HashMap::new(),
        }
    }

    async fn get_file(
        &mut self,
        project_key_pair: ProjectKeyPair,
        base_path: &Path,
    ) -> Result<&mut File, FileBackedEnvelopeStoreError> {
        if !self.cache.contains_key(&project_key_pair) {
            let file =
                Self::load_or_create_file(base_path.to_path_buf(), &project_key_pair).await?;
            self.insert_into_cache(project_key_pair, file);
        }

        let cache_entry = self
            .cache
            .get_mut(&project_key_pair)
            .expect("file to be in the cache");

        cache_entry.last_access = Instant::now();
        Ok(&mut cache_entry.file)
    }

    /// Removes the file from the cache and disk.
    async fn remove(
        &mut self,
        project_key_pair: &ProjectKeyPair,
        base_path: &Path,
    ) -> Result<(), FileBackedEnvelopeStoreError> {
        // Remove from cache
        self.cache.remove(project_key_pair);

        // Construct the file path
        let filename = Self::filename(project_key_pair);
        let filepath = base_path.join(filename);

        // Remove the file from disk
        match remove_file(&filepath).await {
            Ok(_) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(FileBackedEnvelopeStoreError::FileError(e)),
        }
    }

    /// Loads an existing envelope file or creates a new one if it doesn't exist.
    async fn load_or_create_file(
        base_path: PathBuf,
        project_key_pair: &ProjectKeyPair,
    ) -> Result<File, FileBackedEnvelopeStoreError> {
        let filename = Self::filename(project_key_pair);

        let filepath = base_path.join(filename);
        Self::create_spool_directory(&filepath).await?;

        OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(filepath)
            .await
            .map_err(FileBackedEnvelopeStoreError::FileError)
    }

    /// Creates the directory structure for the spool file if it doesn't exist.
    async fn create_spool_directory(path: &Path) -> Result<(), FileBackedEnvelopeStoreError> {
        let Some(parent) = path.parent() else {
            return Ok(());
        };

        if !parent.as_os_str().is_empty() && !parent.exists() {
            relay_log::debug!("creating directory for spooling file: {}", parent.display());
            DirBuilder::new()
                .recursive(true)
                .create(&parent)
                .await
                .map_err(FileBackedEnvelopeStoreError::FileSetupError)?;
        }

        Ok(())
    }

    /// Inserts a new file into the cache, evicting the least recently used entry if necessary.
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

    /// Evicts the least recently used entry from the cache.
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

    /// Generates a filename for the given project key pair.
    fn filename(project_key_pair: &ProjectKeyPair) -> String {
        format!(
            "{}-{}.{}",
            project_key_pair.own_key, project_key_pair.sampling_key, FILE_EXTENSION
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(unix)]
    use std::os::unix::fs::MetadataExt;
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

    async fn setup_envelope_store(max_opened_files: usize) -> FileBackedEnvelopeStore {
        let path = std::env::temp_dir()
            .join(Uuid::new_v4().to_string())
            .into_os_string()
            .into_string()
            .unwrap();
        let config = mock_config(&path, max_opened_files);
        FileBackedEnvelopeStore::new(&config).await.unwrap()
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn test_create_evict_load() {
        let mut store = setup_envelope_store(5).await;
        let project_key_pair = ProjectKeyPair {
            own_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            sampling_key: ProjectKey::parse("b94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
        };

        // First call should create the file
        let file_ino = store
            .get_envelopes_file(project_key_pair)
            .await
            .unwrap()
            .metadata()
            .await
            .unwrap()
            .ino();

        // We evict the file to see if re-opening gives the same ino.
        store.files_cache.evict_lru();
        assert!(store.files_cache.cache.is_empty());

        // Second call should load the file from disk since it was evicted
        let cached_file_ino = store
            .get_envelopes_file(project_key_pair)
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
        let mut store = setup_envelope_store(5).await;
        let project_key_pair1 = ProjectKeyPair {
            own_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            sampling_key: ProjectKey::parse("b94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
        };
        let project_key_pair2 = ProjectKeyPair {
            own_key: ProjectKey::parse("c94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            sampling_key: ProjectKey::parse("d94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
        };

        // Create two files
        store.get_envelopes_file(project_key_pair1).await.unwrap();
        store.get_envelopes_file(project_key_pair2).await.unwrap();

        // List project key pairs
        let key_pairs = store.list_project_key_pairs().await.unwrap();
        assert_eq!(key_pairs.len(), 2);
        assert!(key_pairs.contains(&project_key_pair1));
        assert!(key_pairs.contains(&project_key_pair2));
    }

    #[tokio::test]
    async fn test_cache_eviction() {
        let mut store = setup_envelope_store(5).await;

        // Create 6 files (max_size is 5)
        for i in 0..6 {
            let project_key_pair = ProjectKeyPair {
                own_key: ProjectKey::parse(&format!("c{}4ae32be2584e0bbd7a4cbb95971fee", i))
                    .unwrap(),
                sampling_key: ProjectKey::parse(&format!("c{}4ae32be2584e0bbd7a4cbb95971fee", i))
                    .unwrap(),
            };
            store.get_envelopes_file(project_key_pair).await.unwrap();
        }

        // Check that the cache size is still 5
        assert_eq!(store.files_cache.cache.len(), 5);

        // The first file should have been evicted
        let first_key_pair = ProjectKeyPair {
            own_key: ProjectKey::parse("c04ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            sampling_key: ProjectKey::parse("c04ae32be2584e0bbd7a4cbb95971fee").unwrap(),
        };
        assert!(!store.files_cache.cache.contains_key(&first_key_pair));
    }

    #[tokio::test]
    async fn test_remove_file() {
        let mut store = setup_envelope_store(5).await;
        let project_key_pair = ProjectKeyPair {
            own_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            sampling_key: ProjectKey::parse("b94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
        };

        // Create the file
        store.get_envelopes_file(project_key_pair).await.unwrap();

        // Verify the file exists
        assert!(store.files_cache.cache.contains_key(&project_key_pair));
        let file_path = store
            .base_path
            .join(EnvelopesFilesCache::filename(&project_key_pair));
        assert!(file_path.exists());

        // Remove the file
        store.remove_file(&project_key_pair).await.unwrap();

        // Verify the file no longer exists in cache or on disk
        assert!(!store.files_cache.cache.contains_key(&project_key_pair));
        assert!(!file_path.exists());

        // Removing a non-existent file should not error
        store.remove_file(&project_key_pair).await.unwrap();
    }
}
