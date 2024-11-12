use chrono::{DateTime, Utc};

use super::EnvelopeStack;
use crate::envelope::Envelope;

/// An envelope stack implementation that caches one element in memory and delegates
/// to another envelope stack for additional storage.
#[derive(Debug)]
pub struct CachingEnvelopeStack<S> {
    /// The underlying envelope stack
    inner: S,
    /// The cached envelope (if any)
    cached: Option<Box<Envelope>>,
}

impl<S> CachingEnvelopeStack<S>
where
    S: EnvelopeStack,
{
    /// Creates a new `CachingEnvelopeStack` wrapping the provided envelope stack
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            cached: None,
        }
    }
}

impl<S> EnvelopeStack for CachingEnvelopeStack<S>
where
    S: EnvelopeStack,
{
    type Error = S::Error;

    async fn push(&mut self, envelope: Box<Envelope>) -> Result<(), Self::Error> {
        if self.cached.is_none() {
            // If we don't have a cached envelope, store this one in cache
            self.cached = Some(envelope);
            Ok(())
        } else {
            // If we already have a cached envelope, push the current cached one
            // to the inner stack and keep the new one in cache
            let cached = self.cached.take().unwrap();
            self.inner.push(cached).await?;
            self.cached = Some(envelope);
            Ok(())
        }
    }

    async fn peek(&mut self) -> Result<Option<DateTime<Utc>>, Self::Error> {
        if let Some(ref envelope) = self.cached {
            Ok(Some(envelope.received_at()))
        } else {
            self.inner.peek().await
        }
    }

    async fn pop(&mut self) -> Result<Option<Box<Envelope>>, Self::Error> {
        if let Some(envelope) = self.cached.take() {
            Ok(Some(envelope))
        } else {
            self.inner.pop().await
        }
    }

    async fn flush(mut self) {
        if let Some(envelope) = self.cached {
            let _ = self.inner.push(envelope).await;
        }
        self.inner.flush().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::buffer::envelope_stack::memory::MemoryEnvelopeStack;
    use crate::services::buffer::testutils::utils::mock_envelope;

    #[tokio::test]
    async fn test_caching_stack() {
        let inner = MemoryEnvelopeStack::new();
        let mut stack = CachingEnvelopeStack::new(inner);

        // Create test envelopes with different timestamps
        let env1 = mock_envelope(Utc::now());
        let env2 = mock_envelope(Utc::now());
        let env3 = mock_envelope(Utc::now());

        // Push envelopes
        stack.push(env1).await.unwrap();
        stack.push(env2).await.unwrap();
        stack.push(env3).await.unwrap();

        // Pop them back
        assert!(stack.pop().await.unwrap().is_some()); // Should come from cache
        assert!(stack.pop().await.unwrap().is_some()); // Should come from inner
        assert!(stack.pop().await.unwrap().is_some()); // Should come from inner
        assert!(stack.pop().await.unwrap().is_none()); // Should be empty
    }
}
