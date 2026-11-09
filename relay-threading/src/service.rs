use std::sync::Arc;

use futures::future::BoxFuture;
use relay_log::sentry::SentryFutureExt;
use relay_system::{MessageHandler, Receiver, Service};

use crate::AsyncPool;

/// An async thread pool used for scheduling processing tasks in a service.
pub type ServicePool = AsyncPool<BoxFuture<'static, ()>>;

/// A service that delegates its work to an [`ServicePool`].
pub struct AsyncPoolService<T> {
    handler: T,
    pool: Arc<ServicePool>,
}

impl<T> AsyncPoolService<T> {
    /// Creates a new service from a handler and a pool.
    pub fn new(handler: T, pool: Arc<ServicePool>) -> Self {
        Self { handler, pool }
    }
}

impl<T: MessageHandler + Clone + Send + 'static> Service for AsyncPoolService<T> {
    type Interface = T::Interface;

    async fn run(self, mut rx: Receiver<Self::Interface>) {
        while let Some(message) = rx.recv().await {
            let handler = self.handler.clone();
            // Create a new hub to prevent sentry scopes from bleeding to other tasks.
            let hub = relay_log::Hub::new_from_top(relay_log::Hub::current());

            self.pool
                .spawn_async(Box::pin(
                    async move { handler.handle_message(message).await }.bind_hub(hub),
                ))
                .await;
        }
    }
}
