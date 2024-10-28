use crate::{channel, Addr, Controller, Interface};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::runtime::Runtime;
use tokio::sync::{mpsc, oneshot, RwLock};

// Message is constructed
// Message is sent and wrapped with a channel that accepts a response
// When the response is returned, the corresponding channel is used to send the response
// Each actor has a state which is modified during the runtime of the actor

pub trait MessageRequest: Send + std::fmt::Debug {}

pub trait MessageResponse: Send + std::fmt::Debug {}

pub trait State: Send + std::fmt::Debug + 'static {
    type Public: Send + std::fmt::Debug + 'static;

    type Private: Send + std::fmt::Debug + 'static;

    fn split(self) -> (Self::Public, Self::Private);
}

pub enum SystemMessage {
    Shutdown(Option<Duration>),
}

pub enum ServiceMessage<REQ>
where
    REQ: MessageRequest,
{
    System(SystemMessage),
    User(REQ),
}

pub enum LoopError {
    Expected,
    Critical,
}

enum ActorMessage<REQ, RES> {
    NoResponse(REQ),
    WithResponse(REQ, oneshot::Sender<RES>),
    Barrier(oneshot::Sender<()>),
}

pub struct Addr2<REQ, RES> {
    tx: mpsc::UnboundedSender<ActorMessage<REQ, RES>>,
    queue_size: Arc<AtomicU64>,
}

impl<REQ, RES> Addr2<REQ, RES>
where
    REQ: MessageRequest,
    RES: MessageResponse,
{
    pub fn new() -> (Addr2<REQ, RES>, Receiver2<REQ, RES>) {
        let queue_size = Arc::new(AtomicU64::new(0));
        let (tx, rx) = mpsc::unbounded_channel();

        let addr = Addr2 {
            tx,
            queue_size: queue_size.clone(),
        };
        let receiver = Receiver2 { rx, queue_size };

        (addr, receiver)
    }

    pub fn send(&mut self, message: REQ) {
        self.tx.send(ActorMessage::NoResponse(message)).unwrap()
    }

    pub fn barrier(&mut self) -> ActorResponse<()> {
        let (tx, rx) = oneshot::channel();

        self.tx.send(ActorMessage::Barrier(tx)).unwrap();

        ActorResponse { rx }
    }

    pub fn request(&mut self, message: REQ) -> ActorResponse<RES> {
        let (tx, rx) = oneshot::channel();

        self.tx
            .send(ActorMessage::WithResponse(message, tx))
            .unwrap();

        ActorResponse { rx }
    }
}

pub struct ActorResponse<RES> {
    rx: oneshot::Receiver<RES>,
}

impl<RES> Future for ActorResponse<RES> {
    type Output = RES;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.rx).poll(cx).map(Result::unwrap)
    }
}

pub struct Receiver2<REQ, RES> {
    rx: mpsc::UnboundedReceiver<ActorMessage<REQ, RES>>,
    queue_size: Arc<AtomicU64>,
}

impl<REQ, RES> Receiver2<REQ, RES>
where
    REQ: MessageRequest,
    RES: MessageResponse,
{
    pub async fn recv(&mut self) -> Option<ActorMessage<REQ, RES>> {
        self.rx.recv().await
    }
}

pub trait Service2: Sized + Send + 'static {
    type MessageRequest: MessageRequest;

    type MessageResponse: MessageResponse;

    type State: State;

    fn initialize_state(self) -> Self::State;

    fn handle(
        state: &mut <Self::State as State>::Private,
        message: ServiceMessage<Self::MessageRequest>,
    ) -> impl Future<Output = Result<Option<Self::MessageResponse>, LoopError>> + Send;

    fn start(
        self,
    ) -> (
        <Self::State as State>::Public,
        Addr2<Self::MessageRequest, Self::MessageResponse>,
    ) {
        let (addr, mut rx) = Addr2::<Self::MessageRequest, Self::MessageResponse>::new();

        let (public_state, mut private_state) = self.initialize_state().split();

        tokio::spawn(async move {
            let mut shutdown = Controller::shutdown_handle();

            loop {
                tokio::select! {
                    Some(actor_message) = rx.recv() => {
                        match actor_message {
                            ActorMessage::NoResponse(message) => {
                                let _ = Self::handle(&mut private_state, ServiceMessage::User(message)).await;
                            },
                            ActorMessage::WithResponse(message, tx) => {
                                let result = Self::handle(&mut private_state, ServiceMessage::User(message)).await;
                                if let Ok(Some(response)) = result {
                                    tx.send(response).unwrap();
                                }
                            }
                            ActorMessage::Barrier(tx) => {
                                tx.send(()).unwrap();
                            }
                        }
                    }
                    shutdown = shutdown.notified() => {
                        let _ = Self::handle(&mut private_state, ServiceMessage::System(SystemMessage::Shutdown(shutdown.timeout))).await;
                    }
                    else => break
                }
            }
        });

        (public_state, addr)
    }

    fn start_in(
        self,
        runtime: &Runtime,
    ) -> (
        <Self::State as State>::Public,
        Addr2<Self::MessageRequest, Self::MessageResponse>,
    ) {
        let _guard = runtime.enter();
        self.start()
    }

    fn name() -> &'static str {
        std::any::type_name::<Self>()
    }
}

/// Example usages

#[derive(Debug)]
pub enum CounterRequest {
    Increment,
    GetCount,
}

impl MessageRequest for CounterRequest {}

#[derive(Debug)]
pub enum CounterResponse {
    Count(u64),
}

impl MessageResponse for CounterResponse {}

#[derive(Debug)]
pub struct InnerCounterState {
    count: u64,
}

#[derive(Debug, Clone)]
pub struct CounterState(Arc<RwLock<InnerCounterState>>);

impl State for CounterState {
    type Public = CounterState;
    type Private = CounterState;

    fn split(self) -> (Self::Public, Self::Private) {
        (self.clone(), self)
    }
}

pub struct CounterService {}

impl CounterService {
    pub fn new() -> Self {
        Self {}
    }
}

impl Service2 for CounterService {
    type MessageRequest = CounterRequest;

    type MessageResponse = CounterResponse;

    type State = CounterState;

    fn initialize_state(self) -> Self::State {
        CounterState(Arc::new(RwLock::new(InnerCounterState { count: 0 })))
    }

    async fn handle(
        state: &mut CounterState,
        message: ServiceMessage<Self::MessageRequest>,
    ) -> Result<Option<Self::MessageResponse>, LoopError> {
        match message {
            ServiceMessage::User(req) => match req {
                CounterRequest::Increment => {
                    state.0.write().await.count += 1;
                    Ok(None)
                }
                CounterRequest::GetCount => {
                    Ok(Some(CounterResponse::Count(state.0.read().await.count)))
                }
            },
            ServiceMessage::System(_) => {
                // Handle shutdown gracefully
                Ok(None)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_counter_service_basic() {
        // Create and start the service
        let service = CounterService::new();
        let (state, mut addr) = service.start();

        // Test increment
        addr.send(CounterRequest::Increment);
        addr.send(CounterRequest::Increment);

        // Test get count
        let response = addr.request(CounterRequest::GetCount).await;
        let CounterResponse::Count(count) = response;

        assert_eq!(count, 2, "counter should have value 2 after two increments");

        addr.send(CounterRequest::Increment);
        addr.barrier().await;

        assert_eq!(
            state.0.read().await.count,
            3,
            "state counter should have value 3 after three increments"
        );
    }
}
