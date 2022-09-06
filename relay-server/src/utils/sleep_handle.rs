use std::future::Future;
use std::pin::Pin;
use std::task::Poll;
use std::time::Duration;

pub struct SleepHandle(Option<Pin<Box<tokio::time::Sleep>>>);

impl SleepHandle {
    pub fn idle() -> Self {
        Self(None)
    }

    pub fn reset(&mut self) {
        self.0 = None;
    }

    pub fn set(&mut self, duration: Duration) {
        self.0 = Some(Box::pin(tokio::time::sleep(duration)));
    }

    pub fn is_idle(&self) -> bool {
        self.0.is_none()
    }
}

impl Future for SleepHandle {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        match unsafe { &mut self.get_unchecked_mut().0 } {
            Some(sleep) => Pin::new(sleep).poll(cx),
            None => Poll::Pending,
        }
    }
}
