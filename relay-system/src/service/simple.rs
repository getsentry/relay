use crate::{Interface, Service};

/// A service that handles messages one at a time.
///
/// When the message handler cannot keep up with incoming messages,
/// it applies backpressure into the input queue.
pub trait SimpleService {
    /// The message interface (see [`Service::Interface`]).
    type Interface: Interface;

    /// The asynchronous message handler for this service.
    fn handle_message(&self, message: Self::Interface) -> impl Future<Output = ()> + Send;
}

impl<T: SimpleService + Send + 'static> Service for T {
    type Interface = T::Interface;

    async fn run(self, mut rx: super::Receiver<Self::Interface>) {
        while let Some(message) = rx.recv().await {
            self.handle_message(message).await;
        }
    }
}
