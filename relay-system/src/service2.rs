use crate::{channel, Addr, Controller, Interface};
use std::future::Future;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Runtime;
use tokio::sync::{mpsc, oneshot};

// Message is constructed
// Message is sent and wrapped with a channel that accepts a response
// When the response is returned, the corresponding channel is used to send the response
// Each actor has a state which is modified during the runtime of the actor

pub trait MessageRequest: Send + std::fmt::Debug {}

pub trait MessageResponse: Send + std::fmt::Debug {}

pub trait State: Send + std::fmt::Debug + 'static {}

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
}

impl<REQ, RES> ActorMessage<REQ, RES>
where
    REQ: MessageRequest,
    RES: MessageResponse,
{
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

    fn setup(&mut self);

    fn loop_iter(
        &mut self,
        message: ServiceMessage<Self::MessageRequest>,
    ) -> impl Future<Output = Result<Option<Self::MessageResponse>, LoopError>> + Send;

    fn start(mut self) -> Addr2<Self::MessageRequest, Self::MessageResponse> {
        let (addr, mut rx) = Addr2::<Self::MessageRequest, Self::MessageResponse>::new();

        tokio::spawn(async move {
            let mut shutdown = Controller::shutdown_handle();

            loop {
                tokio::select! {
                    Some(actor_message) = rx.recv() => {
                        match actor_message {
                            ActorMessage::NoResponse(message) => {
                                let _ = self.loop_iter(ServiceMessage::User(message)).await;
                            },
                            ActorMessage::WithResponse(message, tx) => {
                                let result = self.loop_iter(ServiceMessage::User(message)).await;
                                if let Ok(Some(response)) = result {
                                    tx.send(response).unwrap();
                                }
                            }
                        }
                    }
                    shutdown = shutdown.notified() => {
                        let _ = self.loop_iter(ServiceMessage::System(SystemMessage::Shutdown(shutdown.timeout))).await;
                    }
                    else => break
                }
            }
        });

        addr
    }

    fn start_in(self, runtime: &Runtime) -> Addr2<Self::MessageRequest, Self::MessageResponse> {
        let _guard = runtime.enter();
        self.start()
    }

    fn name() -> &'static str {
        std::any::type_name::<Self>()
    }
}
