use crate::{Interface, Service};

pub trait SimpleService {
    type Interface: Interface;

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
