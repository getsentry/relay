use crate::{Controller, Interface};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::runtime::Runtime;
use tokio::sync::{mpsc, oneshot, RwLock};

/// A trait for requests that can be sent to a service.
///
/// All request types must implement Send and Debug.
pub trait MessageRequest: Send + std::fmt::Debug {}

/// A trait for responses that can be returned from a service.
///
/// All response types must implement Send and Debug.
pub trait MessageResponse: Send + std::fmt::Debug {}

/// Defines how a service's state can be split into public and private parts.
///
/// The public part is accessible from outside the service, while the private part
/// is only accessible within the service's message handling logic.
pub trait State: Send + std::fmt::Debug + 'static {
    /// The public portion of the state that will be exposed via ServiceHandle
    type Public: Send + std::fmt::Debug + 'static;

    /// The private portion of the state that will be used in message handling
    type Private: Send + std::fmt::Debug + 'static;

    /// Splits the state into its public and private components
    fn split(self) -> (Self::Public, Self::Private);
}

/// System-level messages that can be sent to any service
#[derive(Debug)]
pub enum SystemMessage {
    /// Request for the service to shut down
    ///
    /// If a Duration is provided, the service should attempt to shut down
    /// within that time limit.
    Shutdown(Option<Duration>),
}

/// Combined enum for both system and user-defined messages
#[derive(Debug)]
pub enum InnerMessage<REQ>
where
    REQ: MessageRequest,
{
    /// System-level control message
    System(SystemMessage),
    /// User-defined request message
    User(REQ),
}

/// Possible errors that can occur during message handling
#[derive(Debug)]
pub enum HandlingError {
    /// An expected error that should not crash the service
    Expected,
    /// A critical error that should cause the service to shut down
    Critical,
}

enum ServiceMessage<REQ, RES> {
    NoResponse(REQ),
    WithResponse(REQ, oneshot::Sender<RES>),
    Barrier(oneshot::Sender<()>),
}

/// Handle for sending messages to a service.
///
/// This type provides methods to:
/// - Send messages without expecting responses
/// - Send requests and await responses
/// - Create barriers for synchronization
#[derive(Debug)]
pub struct ServiceAddr<REQ, RES> {
    tx: mpsc::UnboundedSender<ServiceMessage<REQ, RES>>,
    queue_size: Arc<AtomicU64>,
}

impl<REQ, RES> ServiceAddr<REQ, RES>
where
    REQ: MessageRequest,
    RES: MessageResponse,
{
    pub fn new() -> (ServiceAddr<REQ, RES>, ServiceReceiver<REQ, RES>) {
        let queue_size = Arc::new(AtomicU64::new(0));
        let (tx, rx) = mpsc::unbounded_channel();

        let addr = ServiceAddr {
            tx,
            queue_size: queue_size.clone(),
        };
        let receiver = ServiceReceiver { rx, queue_size };

        (addr, receiver)
    }

    pub fn send(&mut self, message: REQ) {
        self.tx.send(ServiceMessage::NoResponse(message)).unwrap()
    }

    pub fn barrier(&mut self) -> ServiceResponse<()> {
        let (tx, rx) = oneshot::channel();

        self.tx.send(ServiceMessage::Barrier(tx)).unwrap();

        ServiceResponse { rx }
    }

    pub fn request(&mut self, message: REQ) -> ServiceResponse<RES> {
        let (tx, rx) = oneshot::channel();

        self.tx
            .send(ServiceMessage::WithResponse(message, tx))
            .unwrap();

        ServiceResponse { rx }
    }
}

/// A future that represents a pending response from a service.
///
/// This type implements `Future` and will resolve to the response value when it becomes available.
/// It is returned by methods like `ServiceAddr::request()` and `ServiceAddr::barrier()`.
pub struct ServiceResponse<RES> {
    rx: oneshot::Receiver<RES>,
}

impl<RES> Future for ServiceResponse<RES> {
    type Output = RES;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.rx).poll(cx).map(Result::unwrap)
    }
}

/// The receiving half of a service channel.
///
/// This type is responsible for receiving and processing messages sent to the service.
/// It keeps track of the number of messages in the queue via the `queue_size` counter.
pub struct ServiceReceiver<REQ, RES> {
    rx: mpsc::UnboundedReceiver<ServiceMessage<REQ, RES>>,
    queue_size: Arc<AtomicU64>,
}

impl<REQ, RES> ServiceReceiver<REQ, RES>
where
    REQ: MessageRequest,
    RES: MessageResponse,
{
    /// Receives the next message from the service channel.
    ///
    /// Returns `None` if the channel has been closed.
    async fn recv(&mut self) -> Option<ServiceMessage<REQ, RES>> {
        self.rx.recv().await
    }
}

/// A handle containing a service's public state and address.
///
/// This is returned when starting a service and provides access to:
/// - The service's public state
/// - An address for sending messages to the service
#[derive(Debug)]
pub struct ServiceHandle<S, REQ, RES>
where
    S: State,
    REQ: MessageRequest,
    RES: MessageResponse,
{
    pub state: S::Public,
    pub addr: ServiceAddr<REQ, RES>,
}

/// Main trait for implementing a service.
///
/// Services are long-running components that:
/// - Maintain state
/// - Handle messages asynchronously
/// - Can respond to system events (like shutdown)
///
/// # Type Parameters
///
/// - `MessageRequest`: The type of requests this service handles
/// - `MessageResponse`: The type of responses this service returns
/// - `State`: The service's state type
///
/// # Implementation Example
///
/// ```rust
/// # use relay_system::service_new::{ServiceNew, State, MessageRequest, MessageResponse, InnerMessage, HandlingError};
/// # #[derive(Debug)]
/// # enum MyRequest { DoSomething }
/// # impl MessageRequest for MyRequest {}
/// # #[derive(Debug)]
/// # enum MyResponse { Done }
/// # impl MessageResponse for MyResponse {}
/// # #[derive(Debug)]
/// # struct MyState;
/// # impl State for MyState {
/// #     type Public = ();
/// #     type Private = MyState;
/// #     fn split(self) -> (Self::Public, Self::Private) { ((), self) }
/// # }
/// struct MyService;
///
/// impl ServiceNew for MyService {
///     type MessageRequest = MyRequest;
///     type MessageResponse = MyResponse;
///     type State = MyState;
///
///     fn initialize_state(self) -> Self::State {
///         MyState
///     }
///
///     async fn handle(
///         state: &mut MyState,
///         message: InnerMessage<Self::MessageRequest>
///     ) -> Result<Option<Self::MessageResponse>, HandlingError> {
///         match message {
///             InnerMessage::User(req) => Ok(Some(MyResponse::Done)),
///             InnerMessage::System(_) => Ok(None),
///         }
///     }
/// }
/// ```
pub trait ServiceNew: Sized + Send + 'static {
    type MessageRequest: MessageRequest;

    type MessageResponse: MessageResponse;

    type State: State;

    fn initialize_state(self) -> Self::State;

    fn handle(
        state: &mut <Self::State as State>::Private,
        message: InnerMessage<Self::MessageRequest>,
    ) -> impl Future<Output = Result<Option<Self::MessageResponse>, HandlingError>> + Send;

    fn start(self) -> ServiceHandle<Self::State, Self::MessageRequest, Self::MessageResponse> {
        let (addr, mut rx) = ServiceAddr::<Self::MessageRequest, Self::MessageResponse>::new();
        let (public_state, mut private_state) = self.initialize_state().split();

        tokio::spawn(async move {
            let mut shutdown = Controller::shutdown_handle();

            loop {
                tokio::select! {
                    Some(actor_message) = rx.recv() => {
                        match actor_message {
                            ServiceMessage::NoResponse(message) => {
                                let _ = Self::handle(&mut private_state, InnerMessage::User(message)).await;
                            },
                            ServiceMessage::WithResponse(message, tx) => {
                                let result = Self::handle(&mut private_state, InnerMessage::User(message)).await;
                                if let Ok(Some(response)) = result {
                                    tx.send(response).unwrap();
                                }
                            }
                            ServiceMessage::Barrier(tx) => {
                                tx.send(()).unwrap();
                            }
                        }
                    }
                    shutdown = shutdown.notified() => {
                        let _ = Self::handle(&mut private_state, InnerMessage::System(SystemMessage::Shutdown(shutdown.timeout))).await;
                    }
                    else => break
                }
            }
        });

        ServiceHandle {
            state: public_state,
            addr,
        }
    }

    fn start_in(
        self,
        runtime: &Runtime,
    ) -> ServiceHandle<Self::State, Self::MessageRequest, Self::MessageResponse> {
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

impl ServiceNew for CounterService {
    type MessageRequest = CounterRequest;

    type MessageResponse = CounterResponse;

    type State = CounterState;

    fn initialize_state(self) -> Self::State {
        CounterState(Arc::new(RwLock::new(InnerCounterState { count: 0 })))
    }

    async fn handle(
        state: &mut CounterState,
        message: InnerMessage<Self::MessageRequest>,
    ) -> Result<Option<Self::MessageResponse>, HandlingError> {
        match message {
            InnerMessage::User(message) => match message {
                CounterRequest::Increment => {
                    state.0.write().await.count += 1;
                    Ok(None)
                }
                CounterRequest::GetCount => {
                    Ok(Some(CounterResponse::Count(state.0.read().await.count)))
                }
            },
            InnerMessage::System(message) => {
                match message {
                    SystemMessage::Shutdown(timeout) => {
                        // TODO: handle shutdown.
                    }
                }

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
        let ServiceHandle { state, mut addr } = service.start();

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
