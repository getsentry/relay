use crate::envelope::{Envelope, EnvelopeError};
use crate::services::buffer::common::ProjectKeyPair;
use crate::services::buffer::envelope_stack::EnvelopeStack;
use crate::services::buffer::files_manager::{FilesManager, FilesManagerError};
use crate::statsd::RelayTimers;
use std::io;
use std::io::SeekFrom;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::Mutex;

const ENVELOPE_SIZE_FIELD_BYTES: u64 = 8;

/// An error returned when doing an operation on [`FileBackedEnvelopeStack`].
#[derive(Debug, thiserror::Error)]
pub enum FileBackedEnvelopeStackError {
    #[error("failed to perform I/O operation: {0}")]
    Io(#[from] io::Error),

    #[error("failed to work with envelope: {0}")]
    Envelope(#[from] EnvelopeError),

    #[error("failed to get file from FilesManager: {0}")]
    FilesManager(#[from] FilesManagerError),
}

/// An envelope stack that writes and reads envelopes to and from disk files.
///
/// Each `FileBackedEnvelopeStack` corresponds to a file on disk, named with the pattern
/// `own_key-sampling_key`. The envelopes are appended to the file in a custom binary format.
///
/// The format for each envelope in the file is:
/// - `envelope_bytes` (variable length)
/// - `size` of the envelope_bytes (8 bytes, u64 in little-endian)
///
/// This allows reading the file from the end by first reading the `size`, then the `envelope_bytes`.
#[derive(Debug)]
pub struct FileBackedEnvelopeStack {
    project_key_pair: ProjectKeyPair,
    files_manager: Arc<Mutex<FilesManager>>,
}

impl FileBackedEnvelopeStack {
    pub fn new(project_key_pair: ProjectKeyPair, files_manager: Arc<Mutex<FilesManager>>) -> Self {
        Self {
            project_key_pair,
            files_manager,
        }
    }

    /// Reads and removes the last envelope from the file.
    async fn read_and_remove_last_envelope(
        &mut self,
    ) -> Result<Option<Box<Envelope>>, FileBackedEnvelopeStackError> {
        let mut files_manager = self.files_manager.lock().await;
        let file = files_manager.get_file(self.project_key_pair).await?;

        // Get the file size
        let file_size = file.metadata().await?.len();
        if file_size < ENVELOPE_SIZE_FIELD_BYTES {
            return Ok(None);
        }

        // Read the size of the last envelope
        let mut envelope_size_buf = [0u8; ENVELOPE_SIZE_FIELD_BYTES as usize];
        file.seek(SeekFrom::End(-(ENVELOPE_SIZE_FIELD_BYTES as i64)))
            .await?;
        file.read_exact(&mut envelope_size_buf).await?;
        let envelope_size = u64::from_le_bytes(envelope_size_buf);
        if file_size < envelope_size + ENVELOPE_SIZE_FIELD_BYTES {
            // File is corrupted or incomplete
            return Ok(None);
        }

        // Read the envelope data
        let mut envelope_buf = Vec::with_capacity(envelope_size as usize);
        file.seek(SeekFrom::End(
            -((envelope_size + ENVELOPE_SIZE_FIELD_BYTES) as i64),
        ))
        .await?;
        file.read_exact(&mut envelope_buf).await?;

        // Deserialize envelope
        let envelope = Envelope::parse_bytes(envelope_buf.into())
            .map_err(FileBackedEnvelopeStackError::Envelope)?;

        // Truncate the file to remove the envelope
        let new_size = file_size - envelope_size - ENVELOPE_SIZE_FIELD_BYTES;
        file.set_len(new_size).await?;

        Ok(Some(envelope))
    }

    /// Appends an envelope to the file.
    async fn append_envelope(
        &mut self,
        envelope: &Envelope,
    ) -> Result<(), FileBackedEnvelopeStackError> {
        let mut files_manager = self.files_manager.lock().await;
        let file = files_manager.get_file(self.project_key_pair).await?;

        // Serialize envelope
        let envelope_bytes = envelope.to_vec()?;

        // Compute total size
        let size = envelope_bytes.len();

        // Construct buffer to write
        let mut buffer = Vec::with_capacity(size + (ENVELOPE_SIZE_FIELD_BYTES as usize));
        buffer.extend_from_slice(&envelope_bytes);
        buffer.extend_from_slice(&size.to_le_bytes());

        // Write data
        file.seek(SeekFrom::End(0)).await?;
        file.write_all(&buffer).await?;

        Ok(())
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
            if let Some(envelope) = self.read_and_remove_last_envelope().await? {
                Ok(Some(envelope))
            } else {
                Ok(None)
            }
        })
    }

    fn flush(self) -> Vec<Box<Envelope>> {
        // Since data is already on disk, no action needed
        vec![]
    }
}
