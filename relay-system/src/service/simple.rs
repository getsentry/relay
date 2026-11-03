use crate::{Interface, Service};

/// A service that handles messages one at a time.
///
/// When the message handler cannot keep up with incoming messages,
/// it applies backpressure into the input queue.
pub struct SimpleService<T>(T);

impl<T: MessageHandler> SimpleService<T> {
    /// Wraps a message handler in a service loop.
    pub fn from_message_handler(message_handler: T) -> Self {
        Self(message_handler)
    }
}

impl<T: MessageHandler + Send + 'static> Service for SimpleService<T> {
    type Interface = T::Interface;

    async fn run(self, mut rx: super::Receiver<Self::Interface>) {
        while let Some(message) = rx.recv().await {
            self.0.handle_message(message).await;
        }
    }
}

/// Basic building block for composing services.
pub trait MessageHandler {
    /// The message interface (see [`Service::Interface`]).
    type Interface: Interface;

    /// The asynchronous message handler for this service.
    fn handle_message(&self, message: Self::Interface) -> impl Future<Output = ()> + Send;
}
