use crate::envelope::{Envelope, EnvelopeError};
use crate::services::buffer::common::ProjectKeyPair;
use crate::services::buffer::envelope_stack::EnvelopeStack;
use crate::services::buffer::envelope_store::file_backed::{
    FileBackedEnvelopeStore, FileBackedEnvelopeStoreError,
};
use crate::statsd::{RelayCounters, RelayTimers};
use hashbrown::HashSet;
use std::error::Error;
use std::io;
use std::io::SeekFrom;
use std::sync::{Arc, LazyLock};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::Mutex;

/// Set of all error kinds that are allowed when dealing with a file.
static ALLOWED_ERROR_KINDS: LazyLock<HashSet<io::ErrorKind>> = LazyLock::new(|| {
    let mut error_kinds = HashSet::new();
    error_kinds.insert(io::ErrorKind::InvalidInput);
    #[cfg(windows)]
    error_kinds.insert(io::ErrorKind::Uncategorized);

    error_kinds
});

/// The version of the envelope stack file format.
const CURRENT_FILE_VERSION: u8 = 1;

/// The size of the version field in bytes.
const VERSION_FIELD_BYTES: u64 = 1;
/// The size of the total count field in bytes.
const TOTAL_COUNT_FIELD_BYTES: u64 = 4;
/// The size of the envelope size field in bytes.
const ENVELOPE_SIZE_FIELD_BYTES: u64 = 8;
/// The size of the header for each envelope.
const FILE_HEADER_SIZE_BYTES: u64 =
    VERSION_FIELD_BYTES + TOTAL_COUNT_FIELD_BYTES + ENVELOPE_SIZE_FIELD_BYTES;

/// An error returned when doing an operation on [`FileBackedEnvelopeStack`].
#[derive(Debug, thiserror::Error)]
pub enum FileBackedEnvelopeStackError {
    #[error("failed to perform I/O operation: {0}")]
    Io(#[from] io::Error),

    #[error("failed to work with envelope: {0}")]
    Envelope(#[from] EnvelopeError),

    #[error("failed to get file from the store: {0}")]
    EnvelopeStore(#[from] FileBackedEnvelopeStoreError),

    #[error("the version of the entry in the file is not valid")]
    InvalidVersion,

    #[error("file corruption detected: {0}")]
    Corruption(String),
}

/// An envelope stack that writes and reads envelopes to and from disk files.
///
/// Each [`FileBackedEnvelopeStack`] corresponds to a file on disk, named with the pattern
/// `own_key-sampling_key`. The envelopes are appended to the file in a custom binary format.
///
/// The file format is as follows (from start to end):
/// - `envelope_bytes` (variable length)
/// - `version` (1 byte)
/// - `total_count` (4 bytes, u32 in little-endian)
/// - `envelope_size` (8 bytes, u64 in little-endian)
///
/// The rationale behind this layout is that we seek to the version and read to the right to get
/// all the required metadata and not read more that it's needed. Then, once we know the size of
/// the envelope, we seek back metadata size + envelope size.
///
/// This structure allows for efficient reading from the end of the file and
/// updating of the total count when pushing or popping envelopes.
#[derive(Debug)]
pub struct FileBackedEnvelopeStack {
    project_key_pair: ProjectKeyPair,
    envelope_store: Arc<Mutex<FileBackedEnvelopeStore>>,
}

impl FileBackedEnvelopeStack {
    /// Creates a new [`FileBackedEnvelopeStack`] instance.
    pub fn new(
        project_key_pair: ProjectKeyPair,
        envelope_store: Arc<Mutex<FileBackedEnvelopeStore>>,
    ) -> Self {
        Self {
            project_key_pair,
            envelope_store,
        }
    }
}

impl EnvelopeStack for FileBackedEnvelopeStack {
    type Error = FileBackedEnvelopeStackError;

    async fn push(&mut self, envelope: Box<Envelope>) -> Result<(), Self::Error> {
        let mut envelope_store = self.envelope_store.lock().await;
        let file = envelope_store.get_file(self.project_key_pair).await?;

        relay_statsd::metric!(timer(RelayTimers::BufferSpool), {
            append_envelope(file, &envelope).await?;
        });

        relay_statsd::metric!(counter(RelayCounters::BufferSpooledEnvelopes) += 1);

        Ok(())
    }

    async fn peek(&mut self) -> Result<Option<&Envelope>, Self::Error> {
        // Since we cannot return a reference to data on disk, this method isn't practical
        // We'll need to adjust the trait to return an owned Envelope, or change the design
        Ok(None)
    }

    async fn pop(&mut self) -> Result<Option<Box<Envelope>>, Self::Error> {
        let mut envelope_store = self.envelope_store.lock().await;
        let file = envelope_store.get_file(self.project_key_pair).await?;

        let envelope = relay_statsd::metric!(timer(RelayTimers::BufferUnspool), {
            pop_envelope(file).await
        })?;

        if envelope.is_none() {
            // TODO: we might want to investigate in the future if it's better to let the envelope
            //  store deal with file deletion.
            envelope_store.remove_file(&self.project_key_pair).await?;
        } else {
            relay_statsd::metric!(counter(RelayCounters::BufferUnspooledEnvelopes) += 1);
        }

        Ok(envelope)
    }

    fn flush(self) -> Vec<Box<Envelope>> {
        // Since data is already on disk, no action needed
        vec![]
    }
}

/// Helper method to seek and truncate the file.
///
/// If the file is corrupted or incomplete, it will be truncated and an error will be returned.
///
/// Returns `true` if the file was seeked successfully, `false` if truncation happened.
async fn seek_truncate(
    file: &mut File,
    from_end: u64,
) -> Result<bool, FileBackedEnvelopeStackError> {
    match file.seek(SeekFrom::End(-(from_end as i64))).await {
        Ok(_) => Ok(true),
        Err(error) if ALLOWED_ERROR_KINDS.contains(&error.kind()) => {
            relay_log::error!(error = &error as &dyn Error, "failed to seek the file",);
            truncate_file(file, 0).await?;
            Ok(false)
        }
        Err(e) => Err(e.into()),
    }
}

/// Advances the position file by `offset`.
async fn advance(file: &mut File, offset: u64) -> io::Result<()> {
    file.seek(SeekFrom::Current(offset as i64)).await?;
    Ok(())
}

/// Asserts that the file read from the file is supported.
async fn assert_version(file: &mut File) -> Result<(), FileBackedEnvelopeStackError> {
    let version = file.read_u8().await?;
    if version != CURRENT_FILE_VERSION {
        return Err(FileBackedEnvelopeStackError::InvalidVersion);
    }

    Ok(())
}

/// Helper method to get the total count from the file.
///
/// If the file is empty or doesn't contain a total count field, it returns 0.
/// If the version doesn't match the current version, it throws an error.
pub async fn get_total_count(file: &mut File) -> Result<u32, FileBackedEnvelopeStackError> {
    relay_statsd::metric!(timer(RelayTimers::BufferTotalCountReading), {
        let success = seek_truncate(file, FILE_HEADER_SIZE_BYTES).await?;
        if !success {
            return Ok(0);
        }

        // We read the version and make sure it's correct.
        assert_version(file).await?;

        // We read the total count.
        let total_count = file.read_u32_le().await?;

        Ok(total_count)
    })
}

/// Helper method to truncate the file to a given size.
///
/// This is used to remove corrupted or incomplete data from the file.
pub async fn truncate_file(file: &File, new_size: u64) -> Result<(), FileBackedEnvelopeStackError> {
    file.set_len(new_size).await.map_err(|e| {
        FileBackedEnvelopeStackError::Io(io::Error::new(
            io::ErrorKind::Other,
            format!("failed to truncate file: {}", e),
        ))
    })
}

/// Reads and removes the last envelope from the file.
///
/// If the file is empty when trying to read the last envelope `None` is returned.
///
/// If the file is corrupted or incomplete, it will be truncated and an error will be returned.
pub async fn pop_envelope(
    file: &mut File,
) -> Result<Option<Box<Envelope>>, FileBackedEnvelopeStackError> {
    // Get the file size
    let file_size = file.metadata().await?.len();

    // If the file is empty, return None
    if file_size == 0 {
        return Ok(None);
    }

    // We seek at the start of the header of the entry.
    let success = seek_truncate(file, FILE_HEADER_SIZE_BYTES).await?;
    if !success {
        return Err(FileBackedEnvelopeStackError::Corruption(
            "the envelopes file is corrupted".to_string(),
        ));
    }

    // We read the version and make sure it's correct.
    assert_version(file).await?;

    // We skip the total count field.
    advance(file, TOTAL_COUNT_FIELD_BYTES).await?;

    // We read the envelope size.
    let mut envelope_size = [0u8; ENVELOPE_SIZE_FIELD_BYTES as usize];
    file.read_exact(&mut envelope_size).await?;
    let envelope_size = u64::from_le_bytes(envelope_size);

    // We check if the envelope size is too big, if so, we assume that the file is corrupted.
    // TODO: replace with actual 2 * envelope max size.
    if envelope_size > 1000000000 {
        truncate_file(file, 0).await?;
        return Err(FileBackedEnvelopeStackError::Corruption(
            "encountered an envelope size which was too big".to_string(),
        ));
    }

    // Read the envelope data.
    let mut envelope = vec![0; envelope_size as usize];
    let success = seek_truncate(file, envelope_size + FILE_HEADER_SIZE_BYTES).await?;
    if !success {
        return Err(FileBackedEnvelopeStackError::Corruption(
            "the envelopes file is corrupted".to_string(),
        ));
    }
    file.read_exact(&mut envelope).await?;

    // Deserialize the envelope.
    let envelope = match Envelope::parse_bytes(envelope.into()) {
        Ok(env) => env,
        Err(e) => {
            // Envelope deserialization failed, truncate the file.
            truncate_file(file, file_size - envelope_size - FILE_HEADER_SIZE_BYTES).await?;
            return Err(FileBackedEnvelopeStackError::Corruption(format!(
                "failed to deserialize envelope: {}",
                e
            )));
        }
    };

    // Truncate the file to remove the envelope.
    truncate_file(file, file_size - envelope_size - FILE_HEADER_SIZE_BYTES).await?;

    Ok(Some(envelope))
}

/// Appends an envelope to the file.
///
/// The envelope is serialized and written to the end of the file along with its size,
/// total count, and version.
pub async fn append_envelope(
    file: &mut File,
    envelope: &Envelope,
) -> Result<(), FileBackedEnvelopeStackError> {
    // Get the current total count.
    let total_count = get_total_count(file).await?;

    // Serialize the envelope.
    let envelope_bytes = relay_statsd::metric!(timer(RelayTimers::BufferEnvelopeSerialization), {
        envelope.to_vec()?
    });
    if envelope_bytes.is_empty() {
        return Ok(());
    }

    relay_statsd::metric!(timer(RelayTimers::BufferEnvelopeFileWriting), {
        // We position at the end of the file.
        file.seek(SeekFrom::End(0)).await?;

        // 1. Envelope payload
        file.write_all(&envelope_bytes).await?;
        // 2. Version number of the entry
        file.write_u8(CURRENT_FILE_VERSION).await?;
        // 3. Total count of envelope until this point
        file.write_u32_le(total_count + 1).await?;
        // 4. The size of the envelope in bytes
        file.write_u64_le(envelope_bytes.len() as u64).await?;

        // We flush to disk to make sure all data is flushed from the buffer.
        file.flush().await?;
    });

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::buffer::common::ProjectKeyPair;
    use crate::services::buffer::envelope_store::file_backed::FileBackedEnvelopeStore;
    use crate::services::buffer::testutils::utils::mock_envelopes;
    use relay_base_schema::project::ProjectKey;
    use relay_config::Config;
    use std::sync::Arc;
    use tempfile::TempDir;

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

    async fn setup_envelope_store(
        max_opened_files: usize,
    ) -> (Arc<Mutex<FileBackedEnvelopeStore>>, TempDir) {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let path = temp_dir.path().to_str().unwrap().to_string();
        let config = mock_config(&path, max_opened_files);
        let store = Arc::new(Mutex::new(
            FileBackedEnvelopeStore::new(&config).await.unwrap(),
        ));
        (store, temp_dir)
    }

    #[tokio::test]
    async fn test_push_and_pop() {
        let (envelope_store, _temp_dir) = setup_envelope_store(10).await;
        let project_key_pair = ProjectKeyPair {
            own_key: ProjectKey::parse("c04ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            sampling_key: ProjectKey::parse("c04ae32be2584e0bbd7a4cbb95971fee").unwrap(),
        };
        let mut stack = FileBackedEnvelopeStack::new(project_key_pair, envelope_store.clone());

        let envelopes = mock_envelopes(5);

        // Push envelopes
        for envelope in &envelopes {
            stack.push(envelope.clone()).await.unwrap();
        }

        // Pop envelopes and verify
        for i in (0..5).rev() {
            let popped = stack.pop().await.unwrap().unwrap();
            assert_eq!(popped.event_id(), envelopes[i].event_id());
        }

        // Verify stack is empty
        assert!(stack.pop().await.unwrap().is_none());

        // Verify file is deleted after last pop
        let mut store = envelope_store.lock().await;
        let project_key_pairs_with_counts = store.project_key_pairs_with_counts().await.unwrap();
        assert!(
            project_key_pairs_with_counts.is_empty(),
            "Expected file to be removed after last pop"
        );
    }

    #[tokio::test]
    async fn test_total_count_increment() {
        let (envelope_store, _temp_dir) = setup_envelope_store(10).await;
        let project_key_pair = ProjectKeyPair {
            own_key: ProjectKey::parse("c04ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            sampling_key: ProjectKey::parse("c04ae32be2584e0bbd7a4cbb95971fee").unwrap(),
        };
        let mut stack = FileBackedEnvelopeStack::new(project_key_pair, envelope_store.clone());

        let envelopes = mock_envelopes(3);

        // Push envelopes and verify total count
        for (i, envelope) in envelopes.iter().enumerate() {
            stack.push(envelope.clone()).await.unwrap();

            let mut store = envelope_store.lock().await;
            let file = store.get_file(stack.project_key_pair).await.unwrap();
            let total_count = get_total_count(file).await.unwrap();
            assert_eq!(
                total_count,
                (i + 1) as u32,
                "Total count mismatch after push"
            );
        }

        // Pop envelopes and verify total count
        for i in (0..3).rev() {
            stack.pop().await.unwrap();

            let mut store = envelope_store.lock().await;
            let file = store.get_file(stack.project_key_pair).await.unwrap();
            let total_count = get_total_count(file).await.unwrap();
            assert_eq!(total_count, i as u32, "Total count mismatch after pop");
        }
    }

    #[tokio::test]
    async fn test_version_field() {
        let (envelope_store, _temp_dir) = setup_envelope_store(10).await;
        let project_key_pair = ProjectKeyPair {
            own_key: ProjectKey::parse("c04ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            sampling_key: ProjectKey::parse("c04ae32be2584e0bbd7a4cbb95971fee").unwrap(),
        };
        let mut stack = FileBackedEnvelopeStack::new(project_key_pair, envelope_store.clone());

        let envelope = mock_envelopes(1)[0].clone();

        // Push an envelope
        stack.push(envelope).await.unwrap();

        // Verify version field
        let mut store = envelope_store.lock().await;
        let file = store.get_file(stack.project_key_pair).await.unwrap();

        let mut version_buf = [0u8; VERSION_FIELD_BYTES as usize];
        file.seek(SeekFrom::End(-(FILE_HEADER_SIZE_BYTES as i64)))
            .await
            .unwrap();
        file.read_exact(&mut version_buf).await.unwrap();

        assert_eq!(
            version_buf[0], CURRENT_FILE_VERSION,
            "Version field mismatch"
        );
    }

    #[tokio::test]
    async fn test_file_structure() {
        let (envelope_store, _temp_dir) = setup_envelope_store(10).await;
        let project_key_pair = ProjectKeyPair {
            own_key: ProjectKey::parse("c04ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            sampling_key: ProjectKey::parse("c04ae32be2584e0bbd7a4cbb95971fee").unwrap(),
        };
        let mut stack = FileBackedEnvelopeStack::new(project_key_pair, envelope_store.clone());

        let envelope = mock_envelopes(1)[0].clone();

        // Push an envelope
        stack.push(envelope.clone()).await.unwrap();

        // Verify file structure
        let mut store = envelope_store.lock().await;
        let file = store.get_file(stack.project_key_pair).await.unwrap();
        let file_size = file.metadata().await.unwrap().len();

        // Read metadata from the end of the file
        let metadata_length =
            VERSION_FIELD_BYTES + TOTAL_COUNT_FIELD_BYTES + ENVELOPE_SIZE_FIELD_BYTES;
        file.seek(SeekFrom::End(-(metadata_length as i64)))
            .await
            .unwrap();

        let mut version_buf = [0u8; VERSION_FIELD_BYTES as usize];
        file.read_exact(&mut version_buf).await.unwrap();

        let mut total_count_buf = [0u8; TOTAL_COUNT_FIELD_BYTES as usize];
        file.read_exact(&mut total_count_buf).await.unwrap();

        let mut envelope_size_buf = [0u8; ENVELOPE_SIZE_FIELD_BYTES as usize];
        file.read_exact(&mut envelope_size_buf).await.unwrap();

        let version = version_buf[0];
        let total_count = u32::from_le_bytes(total_count_buf);
        let envelope_size = u64::from_le_bytes(envelope_size_buf);

        assert_eq!(version, CURRENT_FILE_VERSION, "Version mismatch");
        assert_eq!(total_count, 1, "Total count mismatch");

        // Jump back to the start of the envelope
        file.seek(SeekFrom::End(-((metadata_length + envelope_size) as i64)))
            .await
            .unwrap();

        // Read and verify the envelope
        let mut envelope_buf = vec![0u8; envelope_size as usize];
        file.read_exact(&mut envelope_buf).await.unwrap();

        assert_eq!(
            file_size,
            envelope_size + metadata_length,
            "File size mismatch"
        );
    }

    #[tokio::test]
    async fn test_malformed_data() {
        let (envelope_store, _temp_dir) = setup_envelope_store(10).await;
        let project_key_pair = ProjectKeyPair {
            own_key: ProjectKey::parse("c04ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            sampling_key: ProjectKey::parse("c04ae32be2584e0bbd7a4cbb95971fee").unwrap(),
        };
        let mut stack = FileBackedEnvelopeStack::new(project_key_pair, envelope_store.clone());

        // Write malformed data directly to the file
        {
            let mut store = envelope_store.lock().await;
            let file = store.get_file(stack.project_key_pair).await.unwrap();
            file.set_len(0).await.unwrap(); // Clear the file
            file.write_all(b"abc").await.unwrap();
            file.flush().await.unwrap();
        }

        // Attempt to pop from the stack with malformed data
        match stack.pop().await {
            Err(FileBackedEnvelopeStackError::Corruption(_)) => {}
            _ => panic!("Expected a Corruption error"),
        }

        // Verify the file is truncated
        {
            let mut store = envelope_store.lock().await;
            let file = store.get_file(stack.project_key_pair).await.unwrap();
            assert_eq!(file.metadata().await.unwrap().len(), 0);
        }
    }

    #[tokio::test]
    async fn test_incomplete_envelope() {
        let (envelope_store, _temp_dir) = setup_envelope_store(10).await;
        let project_key_pair = ProjectKeyPair {
            own_key: ProjectKey::parse("c04ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            sampling_key: ProjectKey::parse("c04ae32be2584e0bbd7a4cbb95971fee").unwrap(),
        };
        let mut stack = FileBackedEnvelopeStack::new(project_key_pair, envelope_store.clone());

        // Write incomplete envelope data directly to the file
        {
            let mut store = envelope_store.lock().await;
            let file = store.get_file(stack.project_key_pair).await.unwrap();
            file.set_len(0).await.unwrap();
            file.write_all(&[0u8; 4]).await.unwrap();
            file.flush().await.unwrap();
        }

        // Attempt to pop from the stack with incomplete data
        assert!(stack.pop().await.is_err());

        // Verify the file is truncated
        {
            let mut store = envelope_store.lock().await;
            let file = store.get_file(stack.project_key_pair).await.unwrap();
            assert_eq!(file.metadata().await.unwrap().len(), 0);
        }
    }

    #[tokio::test]
    async fn test_empty_file_removal() {
        let (envelope_store, _temp_dir) = setup_envelope_store(10).await;
        let project_key_pair = ProjectKeyPair {
            own_key: ProjectKey::parse("c04ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            sampling_key: ProjectKey::parse("c04ae32be2584e0bbd7a4cbb95971fee").unwrap(),
        };
        let mut stack = FileBackedEnvelopeStack::new(project_key_pair, envelope_store.clone());

        // Create and push an envelope
        let envelope = mock_envelopes(1)[0].clone();

        // Push the envelope
        stack.push(envelope.clone()).await.unwrap();

        // Pop the envelope, which should leave the file empty
        let popped_envelope = stack.pop().await.unwrap();
        assert!(popped_envelope.is_some());
        assert_eq!(
            popped_envelope.unwrap().event_id().unwrap(),
            envelope.event_id().unwrap()
        );

        // Check that the file still exists after the first pop
        {
            let mut store = envelope_store.lock().await;
            let result = store.get_file(stack.project_key_pair).await;
            assert!(
                result.is_ok(),
                "Expected file to still exist after first pop"
            );
        }

        // Attempt to pop from the now empty stack
        assert!(stack.pop().await.unwrap().is_none());

        // Verify the file is removed after the second pop by making sure there are no files
        {
            let mut store = envelope_store.lock().await;
            let project_key_pairs_with_counts =
                store.project_key_pairs_with_counts().await.unwrap();
            assert!(
                project_key_pairs_with_counts.is_empty(),
                "Expected file to be removed after second pop, but it still exists"
            );
        }
    }
}
