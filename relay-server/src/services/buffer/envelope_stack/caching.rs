use chrono::{DateTime, Utc};

use super::EnvelopeStack;
use crate::envelope::Envelope;
use crate::managed::Managed;

/// An envelope stack implementation that caches one element in memory and delegates
/// to another envelope stack for additional storage.
#[derive(Debug)]
pub struct CachingEnvelopeStack<S> {
    /// The underlying envelope stack
    inner: S,
    /// The cached managed envelope (if any)
    cached: Option<Managed<Box<Envelope>>>,
}

impl<S> CachingEnvelopeStack<S>
where
    S: EnvelopeStack,
{
    /// Creates a new [`CachingEnvelopeStack`] wrapping the provided envelope stack
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

    async fn push(&mut self, envelope: Managed<Box<Envelope>>) -> Result<(), Self::Error> {
        // If there's a cached envelope, push it to the inner stack first
        if let Some(cached) = self.cached.take() {
            self.inner.push(cached).await?;
        }
        // Cache the new managed envelope (not yet accepted)
        self.cached = Some(envelope);

        Ok(())
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
            // Accept the envelope when popping it - it's been successfully stored in cache
            Ok(Some(envelope.accept(|e| e)))
        } else {
            self.inner.pop().await
        }
    }

    async fn flush(mut self) {
        if let Some(envelope) = self.cached
            && self.inner.push(envelope).await.is_err()
        {
            relay_log::error!(
                "error while pushing the cached envelope in the inner stack during flushing",
            );
            // The managed envelope will be dropped here, automatically rejecting it
        }
        self.inner.flush().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::buffer::envelope_stack::memory::MemoryEnvelopeStack;
    use crate::services::buffer::testutils::utils::managed_envelope;

    #[tokio::test]
    async fn test_caching_stack() {
        let inner = MemoryEnvelopeStack::new();
        let mut stack = CachingEnvelopeStack::new(inner);

        // Create test envelopes with different timestamps
        let envelope_1 = managed_envelope(Utc::now());
        let envelope_2 = managed_envelope(Utc::now());

        // Push 2 envelopes
        stack.push(envelope_1).await.unwrap();
        stack.push(envelope_2).await.unwrap();

        // We pop the cached element.
        assert!(stack.pop().await.unwrap().is_some());

        // We peek the stack expecting it peeks the inner one.
        assert!(stack.peek().await.unwrap().is_some());

        // We pop the element and then check if the stack is empty.
        assert!(stack.pop().await.unwrap().is_some());
        assert!(stack.peek().await.unwrap().is_none());
        assert!(stack.pop().await.unwrap().is_none());
    }
}
