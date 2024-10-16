use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use crate::envelope::EnvelopeError;
use crate::{Envelope, EnvelopeStack};

struct FileEnvelopeStack {
    file: File,
}

impl EnvelopeStack for FileEnvelopeStack {
    type Error = Error;

    async fn push(&mut self, envelope: Box<Envelope>) -> Result<(), Self::Error> {
        dbg!(self.file.stream_position().await);
        let data = envelope.to_vec()?;
        self.file.write_all(&data).await?;
        self.file
            .write_all(&(data.len() as Size).to_le_bytes())
            .await?;
        Ok(())
    }

    async fn pop(&mut self) -> Result<Option<Box<Envelope>>, Self::Error> {
        // NOTE: When this function errors, we could truncate the entire file
        // to repair it.
        let pos = self.file.stream_position().await?;
        if pos == 0 {
            return Ok(None);
        }

        self.file
            .seek(std::io::SeekFrom::Current(-(SIZE_BYTES as i64)))
            .await?;
        let mut envelope_size = [0u8; SIZE_BYTES];
        self.file.read_exact(&mut envelope_size).await?;
        let envelope_size = Size::from_le_bytes(envelope_size);
        let start_of_envelope = self
            .file
            .seek(std::io::SeekFrom::Current(
                -((SIZE_BYTES as Size + envelope_size) as i64),
            ))
            .await?;

        let mut data = vec![0u8; envelope_size as usize];
        self.file.read_exact(&mut data).await?;

        let envelope = Envelope::parse_bytes(data.into())?;

        self.file.set_len(start_of_envelope).await?;

        Ok(Some(envelope))
    }

    fn flush(self) -> Vec<Box<Envelope>> {
        todo!()
    }
}

#[derive(Debug, thiserror::Error)]
enum Error {
    #[error("serialization error: {0}")]
    Envelope(#[from] EnvelopeError),
    #[error("io error: {0}")]
    Io(#[from] tokio::io::Error),
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use tempfile::NamedTempFile;
    use tokio::fs;

    use crate::extractors::RequestMeta;

    use super::*;

    struct CleanupStack(PathBuf, FileEnvelopeStack);

    impl Drop for CleanupStack {
        fn drop(&mut self) {
            std::fs::remove_file(&self.0).unwrap();
        }
    }

    async fn new_stack(initial_data: &[u8]) -> CleanupStack {
        let temp_file = NamedTempFile::new().unwrap();
        let (_, path) = temp_file.keep().unwrap();
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)
            .await
            .unwrap();
        file.write_all(initial_data).await.unwrap();
        let stack = FileEnvelopeStack { file };
        CleanupStack(path, stack)
    }

    #[tokio::test]
    async fn pop_empty() {
        let mut stack = new_stack(&[]).await;

        assert!(stack.1.pop().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn pop_too_short() {
        let mut stack = new_stack(&[1, 2, 3]).await;

        insta::assert_debug_snapshot!(stack.1.pop().await, @r###"
        Err(
            Io(
                Custom {
                    kind: UnexpectedEof,
                    error: "early eof",
                },
            ),
        )
        "###);
    }

    #[tokio::test]
    async fn pop_invalid_length() {
        let mut stack = new_stack(&[0, 0, 0, 1, 2, 3, 4]).await;

        insta::assert_debug_snapshot!(stack.1.pop().await, @r###"

        "###);
    }

    #[tokio::test]
    async fn pop_valid_length_but_garbage_data() {
        let mut stack = new_stack(&[0, 1, 0, 0, 0]).await;
        insta::assert_debug_snapshot!(stack.1.pop().await, @r###"

        "###);
    }

    #[tokio::test]
    async fn roundtrip() {
        let mut stack = new_stack(&[]).await;
        let dsn = "https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42"
            .parse()
            .unwrap();
        let request_meta = RequestMeta::new(dsn);
        let envelope = Envelope::from_request(None, request_meta);

        stack.1.push(envelope).await.unwrap();

        let envelope = stack.1.pop().await.unwrap().unwrap();

        assert_eq!(
            &envelope.meta().dsn().to_string(),
            "https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42"
        );

        assert!(stack.1.pop().await.unwrap().is_none());
    }
}
