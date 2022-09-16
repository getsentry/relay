use std::future::Future;
use std::pin::Pin;
use std::task::Poll;
use std::time::Duration;

/// A future wrapper around [`tokio::time::Sleep`].
///
/// This has two internal states, either it is pending indefinite or it wakes up after a certain
/// duration of time has elapsed.
pub struct SleepHandle(Option<Pin<Box<tokio::time::Sleep>>>);

impl SleepHandle {
    /// Creates [`SleepHandle`] and sets its internal state to an indefinitely pending future.
    pub fn idle() -> Self {
        Self(None)
    }

    /// Resets the internal state to an indefinitely pending future.
    pub fn reset(&mut self) {
        self.0 = None;
    }

    /// Sets the internal state to a future that will yield after `duration` time has elapsed.
    pub fn set(&mut self, duration: Duration) {
        self.0 = Some(Box::pin(tokio::time::sleep(duration)));
    }

    /// Checks wether the internal state is currently pending indefinite.
    pub fn is_idle(&self) -> bool {
        self.0.is_none()
    }
}

impl Future for SleepHandle {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        match &mut self.0 {
            Some(sleep) => Pin::new(sleep).poll(cx),
            None => Poll::Pending,
        }
    }
}
