use crate::services::buffer::common::ProjectKeyPair;
use crate::services::buffer::envelope_stack::file_backed::get_total_count;
use crate::statsd::RelayGauges;
use hashbrown::HashMap;
use relay_base_schema::project::{ParseProjectKeyError, ProjectKey};
use relay_config::Config;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::fs::{create_dir_all, read_dir, remove_file, File, OpenOptions};
use tokio::io;
use tokio::time::sleep;

/// File extension for the files that are written by Relay and used for spooling purposes.
const FILE_EXTENSION: &str = "spool";
/// The delimiter used in the file name for the envelopes file for a given project key pair.
const PROJECT_KEYS_DELIMITER: &str = "_";

/// Generates a file name for the given project key pair.
fn get_envelopes_file_file_name(project_key_pair: &ProjectKeyPair) -> String {
    format!(
        "{}{}{}.{}",
        project_key_pair.own_key,
        PROJECT_KEYS_DELIMITER,
        project_key_pair.sampling_key,
        FILE_EXTENSION
    )
}

/// Parses a filename of the envelopes file and returns the project key pair if valid.
fn parse_envelopes_file_file_name(file_name: &str) -> Option<ProjectKeyPair> {
    let (stem, _) = file_name.strip_suffix(FILE_EXTENSION)?.split_once(".")?;
    let (own_key, sampling_key) = stem.split_once(PROJECT_KEYS_DELIMITER)?;

    Some(ProjectKeyPair {
        own_key: ProjectKey::parse(own_key).ok()?,
        sampling_key: ProjectKey::parse(sampling_key).ok()?,
    })
}

/// An error returned when doing an operation on [`FileBackedEnvelopeStore`].
#[derive(Debug, thiserror::Error)]
pub enum FileBackedEnvelopeStoreError {
    #[error("failed work with a file: {0}")]
    FileError(#[from] io::Error),

    #[error("no file path for the spool was provided")]
    NoFilePath,

    #[error("failed to parse project key: {0}")]
    ProjectKeyParseError(#[from] ParseProjectKeyError),
}

/// A file-backed envelope store that manages envelope files on disk.
///
/// This struct provides functionality to store and manage envelopes in files on the local filesystem.
/// It uses a caching mechanism to optimize file access and tracks the total size of the envelope files.
#[derive(Debug)]
pub struct FileBackedEnvelopeStore {
    base_path: PathBuf,
    files_cache: EnvelopesFilesCache,
    folder_size_tracker: FolderSizeTracker,
}

impl FileBackedEnvelopeStore {
    /// Creates a new `FileBackedEnvelopeStore` instance.
    ///
    /// This method initializes the envelope store with the provided configuration,
    /// setting up the base path for envelope files and initializing the folder size tracker.
    /// It also creates the spool directory if it doesn't exist.
    pub async fn new(config: &Config) -> Result<Self, FileBackedEnvelopeStoreError> {
        let Some(base_path) = config.spool_envelopes_path() else {
            return Err(FileBackedEnvelopeStoreError::NoFilePath);
        };

        // Create the spool directory if it doesn't exist
        create_dir_all(&base_path).await?;

        let folder_size_tracker = FolderSizeTracker::prepare(
            base_path.clone(),
            config.spool_disk_usage_refresh_frequency_ms(),
        )
        .await?;

        Ok(FileBackedEnvelopeStore {
            base_path: base_path.clone(),
            files_cache: EnvelopesFilesCache::new(config.spool_envelopes_max_opened_files()),
            folder_size_tracker,
        })
    }

    /// Retrieves or creates an envelope file for the given project key pair.
    ///
    /// If the file doesn't exist, it will be created. If it exists, it will be opened.
    /// This method uses a caching mechanism to optimize file access.
    pub async fn get_envelopes_file(
        &mut self,
        project_key_pair: ProjectKeyPair,
    ) -> Result<&mut File, FileBackedEnvelopeStoreError> {
        self.files_cache
            .get_file(project_key_pair, &self.base_path)
            .await
    }

    /// Lists all project key pairs that have envelope files on disk and their total counts.
    ///
    /// This method scans the base directory and returns a map of project key pairs to their
    /// respective envelope counts.
    pub async fn project_key_pairs_with_counts(
        &mut self,
    ) -> Result<HashMap<ProjectKeyPair, u32>, FileBackedEnvelopeStoreError> {
        let mut project_key_pairs = HashMap::new();

        let mut dir = read_dir(&self.base_path).await?;
        while let Some(entry) = dir.next_entry().await? {
            let path = entry.path();

            if path.is_file() {
                if let Some(file_name) = path.file_name().and_then(|s| s.to_str()) {
                    if let Some(project_key_pair) = parse_envelopes_file_file_name(file_name) {
                        let file = self.get_envelopes_file(project_key_pair).await?;
                        let total_count = get_total_count(file).await.unwrap_or(0);

                        project_key_pairs.insert(project_key_pair, total_count);
                    }
                }
            }
        }

        Ok(project_key_pairs)
    }

    /// Removes the envelope file associated with the given project key pair.
    ///
    /// This method removes the file both from the cache and from the disk.
    pub async fn remove_file(
        &mut self,
        project_key_pair: &ProjectKeyPair,
    ) -> Result<(), FileBackedEnvelopeStoreError> {
        self.files_cache
            .remove(project_key_pair, &self.base_path)
            .await
    }

    /// Returns the current estimated size of the folder containing all envelope files.
    ///
    /// This method provides a quick way to get the total size of all envelope files
    /// without having to scan the directory each time.
    pub fn usage(&self) -> u64 {
        self.folder_size_tracker.size()
    }
}

/// A cache for managing open file handles which contain envelopes.
#[derive(Debug)]
struct EnvelopesFilesCache {
    max_opened_files: usize,
    cache: HashMap<ProjectKeyPair, CacheEntry>,
}

/// A cache entry for a file containing envelopes.
#[derive(Debug)]
struct CacheEntry {
    file: File,
    last_access: Instant,
}

impl EnvelopesFilesCache {
    /// Creates a new instance of [`EnvelopesFilesCache`].
    fn new(max_opened_files: usize) -> Self {
        EnvelopesFilesCache {
            max_opened_files,
            cache: HashMap::new(),
        }
    }

    /// Returns the [`File`] from the cache, otherwise it will be loaded from the file system.
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
        let file_name = get_envelopes_file_file_name(project_key_pair);
        let file_path = base_path.join(file_name);

        // Remove the file from disk
        match remove_file(&file_path).await {
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
        let file_name = get_envelopes_file_file_name(project_key_pair);
        let file_path = base_path.join(file_name);

        OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(file_path)
            .await
            .map_err(FileBackedEnvelopeStoreError::FileError)
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
}

/// A background task that tracks the size of the folder containing all envelope files.
#[derive(Debug, Clone)]
struct FolderSizeTracker {
    base_path: PathBuf,
    last_known_size: Arc<AtomicU64>,
    refresh_frequency: Duration,
}

impl FolderSizeTracker {
    fn new(base_path: PathBuf, refresh_frequency: Duration) -> Self {
        Self {
            base_path,
            last_known_size: Arc::new(AtomicU64::new(0)),
            refresh_frequency,
        }
    }

    pub async fn prepare(
        base_path: PathBuf,
        refresh_frequency: Duration,
    ) -> Result<Self, FileBackedEnvelopeStoreError> {
        let size = Self::estimate_folder_size(&base_path).await?;

        let tracker = Self::new(base_path, refresh_frequency);
        tracker.last_known_size.store(size, Ordering::Relaxed);
        tracker.start_background_refresh();

        Ok(tracker)
    }

    /// Returns the last known folder size.
    fn size(&self) -> u64 {
        self.last_known_size.load(Ordering::Relaxed)
    }

    /// Starts a background tokio task to update the folder usage.
    fn start_background_refresh(&self) {
        let base_path = self.base_path.clone();
        // We get a weak reference, to make sure that if `FolderSizeTracker` is dropped, the reference can't
        // be upgraded, causing the loop in the tokio task to exit.
        let last_known_size_weak = Arc::downgrade(&self.last_known_size);
        let refresh_frequency = self.refresh_frequency;

        tokio::spawn(async move {
            loop {
                // When our `Weak` reference can't be upgraded to an `Arc`, it means that the value
                // is not referenced anymore by self, meaning that `FolderSizeTracker` was dropped.
                let Some(last_known_size) = last_known_size_weak.upgrade() else {
                    break;
                };

                match Self::estimate_folder_size(&base_path).await {
                    Ok(size) => {
                        let current = last_known_size.load(Ordering::Relaxed);
                        if last_known_size
                            .compare_exchange_weak(
                                current,
                                size,
                                Ordering::Relaxed,
                                Ordering::Relaxed,
                            )
                            .is_err()
                        {
                            relay_log::error!("failed to update the folder size asynchronously");
                        }
                    }
                    Err(e) => {
                        relay_log::error!("failed to estimate folder size: {}", e);
                    }
                }

                sleep(refresh_frequency).await;
            }
        });
    }

    /// Estimates the total size of the folder containing all envelope files.
    async fn estimate_folder_size(base_path: &Path) -> Result<u64, FileBackedEnvelopeStoreError> {
        let mut total_size = 0;
        let mut dir = read_dir(base_path).await?;

        while let Some(entry) = dir.next_entry().await? {
            let path = entry.path();
            if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some(FILE_EXTENSION) {
                if let Ok(metadata) = entry.metadata().await {
                    total_size += metadata.len();
                }
            }
        }

        relay_statsd::metric!(gauge(RelayGauges::BufferDiskUsed) = total_size);

        Ok(total_size)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::buffer::envelope_stack::file_backed::append_envelope;
    use crate::services::buffer::testutils::utils::mock_envelopes;
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

        // Create mock envelopes
        let envelopes1 = mock_envelopes(2);
        let envelopes2 = mock_envelopes(3);

        // Append envelopes to files
        for envelope in envelopes1 {
            let file = store.get_envelopes_file(project_key_pair1).await.unwrap();
            append_envelope(file, &envelope).await.unwrap();
        }
        for envelope in envelopes2 {
            let file = store.get_envelopes_file(project_key_pair2).await.unwrap();
            append_envelope(file, &envelope).await.unwrap();
        }

        // List project key pairs with counts
        let project_key_pairs_with_counts = store.project_key_pairs_with_counts().await.unwrap();
        assert_eq!(project_key_pairs_with_counts.len(), 2);
        assert_eq!(
            project_key_pairs_with_counts.get(&project_key_pair1),
            Some(&2)
        );
        assert_eq!(
            project_key_pairs_with_counts.get(&project_key_pair2),
            Some(&3)
        );
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
            .join(get_envelopes_file_file_name(&project_key_pair));
        assert!(file_path.exists());

        // Remove the file
        store.remove_file(&project_key_pair).await.unwrap();

        // Verify the file no longer exists in cache or on disk
        assert!(!store.files_cache.cache.contains_key(&project_key_pair));
        assert!(!file_path.exists());

        // Removing a non-existent file should not error
        store.remove_file(&project_key_pair).await.unwrap();
    }

    #[tokio::test]
    async fn test_folder_size_tracker() {
        let mut store = setup_envelope_store(5).await;

        // Create some files
        for i in 0..3 {
            let project_key_pair = ProjectKeyPair {
                own_key: ProjectKey::parse(&format!("a{}4ae32be2584e0bbd7a4cbb95971fee", i))
                    .unwrap(),
                sampling_key: ProjectKey::parse(&format!("b{}4ae32be2584e0bbd7a4cbb95971fee", i))
                    .unwrap(),
            };
            let file = store.get_envelopes_file(project_key_pair).await.unwrap();
            file.set_len(1000 * (i + 1) as u64).await.unwrap(); // Set different file sizes
        }

        // Wait for the background task to update the folder size
        sleep(Duration::from_millis(200)).await;

        // Check the folder size
        let folder_size = store.usage();

        // The total size should be 1000 + 2000 + 3000 = 6000 bytes
        assert_eq!(folder_size, 6000);
    }

    #[test]
    fn test_parse_filename() {
        let valid_filename =
            "a94ae32be2584e0bbd7a4cbb95971fee_b94ae32be2584e0bbd7a4cbb95971fee.spool";
        let invalid_filename = "invalid_filename.txt";

        assert!(parse_envelopes_file_file_name(valid_filename).is_some());
        assert!(parse_envelopes_file_file_name(invalid_filename).is_none());

        if let Some(project_key_pair) = parse_envelopes_file_file_name(valid_filename) {
            assert_eq!(
                project_key_pair.own_key.to_string(),
                "a94ae32be2584e0bbd7a4cbb95971fee"
            );
            assert_eq!(
                project_key_pair.sampling_key.to_string(),
                "b94ae32be2584e0bbd7a4cbb95971fee"
            );
        }
    }
}
