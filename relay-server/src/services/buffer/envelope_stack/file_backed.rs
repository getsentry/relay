use crate::envelope::{Envelope, EnvelopeError};
use crate::services::buffer::common::ProjectKeyPair;
use crate::services::buffer::envelope_stack::EnvelopeStack;
use crate::services::buffer::envelope_store::file_backed::{
    FileBackedEnvelopeStore, FileBackedEnvelopeStoreError,
};
use crate::statsd::RelayTimers;
use std::io;
use std::io::SeekFrom;
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::Mutex;

/// The version of the envelope stack file format.
const VERSION: u8 = 1;

/// The size of the version field in bytes.
const VERSION_FIELD_BYTES: u64 = 1;
/// The size of the total count field in bytes.
const TOTAL_COUNT_FIELD_BYTES: u64 = 4;
/// The size of the envelope size field in bytes.
const ENVELOPE_SIZE_FIELD_BYTES: u64 = 8;

/// An error returned when doing an operation on [`FileBackedEnvelopeStack`].
#[derive(Debug, thiserror::Error)]
pub enum FileBackedEnvelopeStackError {
    #[error("failed to perform I/O operation: {0}")]
    Io(#[from] io::Error),

    #[error("failed to work with envelope: {0}")]
    Envelope(#[from] EnvelopeError),

    #[error("failed to get file from the store: {0}")]
    EnvelopeStore(#[from] FileBackedEnvelopeStoreError),

    #[error("file corruption detected: {0}")]
    Corruption(String),
}

/// An envelope stack that writes and reads envelopes to and from disk files.
///
/// Each `FileBackedEnvelopeStack` corresponds to a file on disk, named with the pattern
/// `own_key-sampling_key`. The envelopes are appended to the file in a custom binary format.
///
/// The file format is as follows:
/// - `version` (1 byte)
/// - `total_count` (4 bytes, u32 in little-endian)
/// - `envelope_size` (8 bytes, u64 in little-endian)
/// - `envelope_bytes` (variable length)
///
/// This structure allows for efficient reading from the end of the file and
/// updating of the total count when pushing or popping envelopes.
#[derive(Debug)]
pub struct FileBackedEnvelopeStack {
    project_key_pair: ProjectKeyPair,
    envelope_store: Arc<Mutex<FileBackedEnvelopeStore>>,
}

impl FileBackedEnvelopeStack {
    /// Creates a new `FileBackedEnvelopeStack` instance.
    pub fn new(
        project_key_pair: ProjectKeyPair,
        envelope_store: Arc<Mutex<FileBackedEnvelopeStore>>,
    ) -> Self {
        Self {
            project_key_pair,
            envelope_store,
        }
    }

    /// Reads and removes the last envelope from the file.
    ///
    /// If the file is empty when trying to read the last envelope, the file will be deleted and
    /// `None` is returned.
    ///
    /// If the file is corrupted or incomplete, it will be truncated and an error will be returned.
    async fn read_and_remove_last_envelope(
        &mut self,
    ) -> Result<Option<Box<Envelope>>, FileBackedEnvelopeStackError> {
        let mut envelope_store = self.envelope_store.lock().await;
        let file = envelope_store
            .get_envelopes_file(self.project_key_pair)
            .await?;

        let envelope = read_and_remove_last_envelope(file).await?;
        // In case we didn't get any envelope back, we will remove the file.
        if envelope.is_none() {
            envelope_store.remove_file(&self.project_key_pair).await?;
        }

        Ok(envelope)
    }

    /// Appends an envelope to the file.
    ///
    /// The envelope is serialized and written to the end of the file along with its size,
    /// total count, and version.
    async fn append_envelope(
        &mut self,
        envelope: &Envelope,
    ) -> Result<(), FileBackedEnvelopeStackError> {
        let mut envelope_store = self.envelope_store.lock().await;
        let file = envelope_store
            .get_envelopes_file(self.project_key_pair)
            .await?;

        append_envelope(file, envelope).await
    }
}

impl EnvelopeStack for FileBackedEnvelopeStack {
    type Error = FileBackedEnvelopeStackError;

    async fn push(&mut self, envelope: Box<Envelope>) -> Result<(), Self::Error> {
        relay_statsd::metric!(timer(RelayTimers::BufferPush), {
            self.append_envelope(&envelope).await?;
        });
        Ok(())
    }

    async fn peek(&mut self) -> Result<Option<&Envelope>, Self::Error> {
        // Since we cannot return a reference to data on disk, this method isn't practical
        // We'll need to adjust the trait to return an owned Envelope, or change the design
        Ok(None)
    }

    async fn pop(&mut self) -> Result<Option<Box<Envelope>>, Self::Error> {
        relay_statsd::metric!(timer(RelayTimers::BufferPop), {
            self.read_and_remove_last_envelope().await
        })
    }

    fn flush(self) -> Vec<Box<Envelope>> {
        // Since data is already on disk, no action needed
        vec![]
    }
}

/// Helper method to get the total count from the file.
///
/// If the file is empty or doesn't contain a total count field, it returns 0.
pub async fn get_total_count(file: &mut File) -> Result<u32, FileBackedEnvelopeStackError> {
    let file_size = file.metadata().await?.len();
    if file_size < VERSION_FIELD_BYTES + TOTAL_COUNT_FIELD_BYTES {
        return Ok(0);
    }

    let mut total_count_buf = [0u8; TOTAL_COUNT_FIELD_BYTES as usize];
    file.seek(SeekFrom::End(
        -((VERSION_FIELD_BYTES + TOTAL_COUNT_FIELD_BYTES) as i64),
    ))
    .await?;
    file.read_exact(&mut total_count_buf).await?;

    Ok(u32::from_le_bytes(total_count_buf))
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
pub async fn read_and_remove_last_envelope(
    file: &mut File,
) -> Result<Option<Box<Envelope>>, FileBackedEnvelopeStackError> {
    // Get the file size
    let file_size = file.metadata().await?.len();

    // If the file is empty, return None
    if file_size == 0 {
        return Ok(None);
    }

    // Check if file is corrupted or incomplete
    let header_size = VERSION_FIELD_BYTES + TOTAL_COUNT_FIELD_BYTES + ENVELOPE_SIZE_FIELD_BYTES;

    // Check if the file has no header metadata
    if file_size < header_size {
        truncate_file(file, 0).await?;
        return Err(FileBackedEnvelopeStackError::Corruption(
            "the file is smaller than the minimum required size".to_string(),
        ));
    }

    // Skip version and total count fields, then read the envelope size
    let mut envelope_size_buf = [0u8; ENVELOPE_SIZE_FIELD_BYTES as usize];
    file.seek(SeekFrom::End(-(header_size as i64))).await?;
    file.read_exact(&mut envelope_size_buf).await?;
    let envelope_size = u64::from_le_bytes(envelope_size_buf);

    // Check if file is corrupted or incomplete
    if file_size < envelope_size + header_size {
        truncate_file(file, 0).await?;
        return Err(FileBackedEnvelopeStackError::Corruption(
            "the file size is smaller than expected envelope size".to_string(),
        ));
    }

    // Read the envelope data
    let mut envelope_buf = vec![0; envelope_size as usize];
    file.seek(SeekFrom::End(-((envelope_size + header_size) as i64)))
        .await?;
    file.read_exact(&mut envelope_buf).await?;

    // Deserialize envelope
    let envelope = match Envelope::parse_bytes(envelope_buf.into()) {
        Ok(env) => env,
        Err(e) => {
            // Envelope deserialization failed, truncate the file
            truncate_file(file, file_size - envelope_size - header_size).await?;
            return Err(FileBackedEnvelopeStackError::Corruption(format!(
                "failed to deserialize envelope: {}",
                e
            )));
        }
    };

    // Truncate the file to remove the envelope
    truncate_file(file, file_size - envelope_size - header_size).await?;

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
    // Get the current total count
    let total_count = get_total_count(file).await?;

    // Serialize envelope
    let envelope_bytes = envelope.to_vec()?;

    // Construct buffer to write
    let envelope_size = envelope_bytes.len();
    let mut buffer = Vec::with_capacity(
        envelope_size
            + (VERSION_FIELD_BYTES + TOTAL_COUNT_FIELD_BYTES + ENVELOPE_SIZE_FIELD_BYTES) as usize,
    );
    buffer.extend_from_slice(&envelope_bytes);
    buffer.extend_from_slice(&envelope_size.to_le_bytes());
    buffer.extend_from_slice(&(total_count + 1).to_le_bytes());
    buffer.push(VERSION);

    // Write data
    file.seek(SeekFrom::End(0)).await?;
    file.write_all(&buffer).await?;
    file.flush().await?;

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

    async fn setup_envelope_store(max_opened_files: usize) -> Arc<Mutex<FileBackedEnvelopeStore>> {
        let path = std::env::temp_dir()
            .join(Uuid::new_v4().to_string())
            .into_os_string()
            .into_string()
            .unwrap();
        let config = mock_config(&path, max_opened_files);
        Arc::new(Mutex::new(
            FileBackedEnvelopeStore::new(&config).await.unwrap(),
        ))
    }

    #[tokio::test]
    async fn test_push_and_pop() {
        let envelope_store = setup_envelope_store(10).await;
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
        let envelope_store = setup_envelope_store(10).await;
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
            let file = store
                .get_envelopes_file(stack.project_key_pair)
                .await
                .unwrap();
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
            let file = store
                .get_envelopes_file(stack.project_key_pair)
                .await
                .unwrap();
            let total_count = get_total_count(file).await.unwrap();
            assert_eq!(total_count, i as u32, "Total count mismatch after pop");
        }
    }

    #[tokio::test]
    async fn test_version_field() {
        let envelope_store = setup_envelope_store(10).await;
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
        let file = store
            .get_envelopes_file(stack.project_key_pair)
            .await
            .unwrap();

        let mut version_buf = [0u8; VERSION_FIELD_BYTES as usize];
        file.seek(SeekFrom::End(-(VERSION_FIELD_BYTES as i64)))
            .await
            .unwrap();
        file.read_exact(&mut version_buf).await.unwrap();

        assert_eq!(version_buf[0], VERSION, "Version field mismatch");
    }

    #[tokio::test]
    async fn test_file_structure() {
        let envelope_store = setup_envelope_store(10).await;
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
        let file = store
            .get_envelopes_file(stack.project_key_pair)
            .await
            .unwrap();
        let file_size = file.metadata().await.unwrap().len();

        // Read fields from the end of the file
        file.seek(SeekFrom::End(-(VERSION_FIELD_BYTES as i64)))
            .await
            .unwrap();
        let mut version_buf = [0u8; VERSION_FIELD_BYTES as usize];
        file.read_exact(&mut version_buf).await.unwrap();

        file.seek(SeekFrom::End(
            -((VERSION_FIELD_BYTES + TOTAL_COUNT_FIELD_BYTES) as i64),
        ))
        .await
        .unwrap();
        let mut total_count_buf = [0u8; TOTAL_COUNT_FIELD_BYTES as usize];
        file.read_exact(&mut total_count_buf).await.unwrap();

        file.seek(SeekFrom::End(
            -((VERSION_FIELD_BYTES + TOTAL_COUNT_FIELD_BYTES + ENVELOPE_SIZE_FIELD_BYTES) as i64),
        ))
        .await
        .unwrap();
        let mut envelope_size_buf = [0u8; ENVELOPE_SIZE_FIELD_BYTES as usize];
        file.read_exact(&mut envelope_size_buf).await.unwrap();

        let version = version_buf[0];
        let total_count = u32::from_le_bytes(total_count_buf);
        let envelope_size = u64::from_le_bytes(envelope_size_buf);

        assert_eq!(version, VERSION, "Version mismatch");
        assert_eq!(total_count, 1, "Total count mismatch");
        assert_eq!(
            file_size,
            envelope_size
                + VERSION_FIELD_BYTES
                + TOTAL_COUNT_FIELD_BYTES
                + ENVELOPE_SIZE_FIELD_BYTES,
            "File size mismatch"
        );
    }

    #[tokio::test]
    async fn test_malformed_data() {
        let envelope_store = setup_envelope_store(10).await;
        let project_key_pair = ProjectKeyPair {
            own_key: ProjectKey::parse("c04ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            sampling_key: ProjectKey::parse("c04ae32be2584e0bbd7a4cbb95971fee").unwrap(),
        };
        let mut stack = FileBackedEnvelopeStack::new(project_key_pair, envelope_store.clone());

        // Write malformed data directly to the file
        {
            let mut store = envelope_store.lock().await;
            let file = store
                .get_envelopes_file(stack.project_key_pair)
                .await
                .unwrap();
            file.set_len(0).await.unwrap(); // Clear the file
            file.write_all(b"malformed data").await.unwrap();
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
            let file = store
                .get_envelopes_file(stack.project_key_pair)
                .await
                .unwrap();
            assert_eq!(file.metadata().await.unwrap().len(), 0);
        }
    }

    #[tokio::test]
    async fn test_incomplete_envelope() {
        let envelope_store = setup_envelope_store(10).await;
        let project_key_pair = ProjectKeyPair {
            own_key: ProjectKey::parse("c04ae32be2584e0bbd7a4cbb95971fee").unwrap(),
            sampling_key: ProjectKey::parse("c04ae32be2584e0bbd7a4cbb95971fee").unwrap(),
        };
        let mut stack = FileBackedEnvelopeStack::new(project_key_pair, envelope_store.clone());

        // Write incomplete envelope data directly to the file
        {
            let mut store = envelope_store.lock().await;
            let file = store
                .get_envelopes_file(stack.project_key_pair)
                .await
                .unwrap();
            file.set_len(0).await.unwrap();
            file.write_all(&[0u8; 4]).await.unwrap();
            file.flush().await.unwrap();
        }

        // Attempt to pop from the stack with incomplete data
        assert!(stack.pop().await.is_err());

        // Verify the file is truncated
        {
            let mut store = envelope_store.lock().await;
            let file = store
                .get_envelopes_file(stack.project_key_pair)
                .await
                .unwrap();
            assert_eq!(file.metadata().await.unwrap().len(), 0);
        }
    }

    #[tokio::test]
    async fn test_empty_file_removal() {
        let envelope_store = setup_envelope_store(10).await;
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
            let result = store.get_envelopes_file(stack.project_key_pair).await;
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
