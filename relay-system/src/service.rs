use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::Context;
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};
use tokio::time::MissedTickBehavior;

use crate::statsd::SystemGauges;

/// Interval for recording backlog metrics on service channels.
const BACKLOG_INTERVAL: Duration = Duration::from_secs(1);

/// A message interface for [services](Service).
///
/// Most commonly, this interface is an enumeration of messages, but it can also be implemented on a
/// single message. For each individual message, this type needs to implement the [`FromMessage`]
/// trait.
///
/// # Implementating Interfaces
///
/// There are three main ways to implement interfaces, which depends on the number of messages and
/// their return values. The simplest way is an interface consisting of a **single message** with
/// **no return value**. For this case, use the message directly as interface and choose
/// `NoResponse` as response:
///
/// ```
/// use relay_system::{FromMessage, Interface, NoResponse};
///
/// #[derive(Debug)]
/// pub struct MyMessage;
///
/// impl Interface for MyMessage {}
///
/// impl FromMessage<Self> for MyMessage {
///     type Response = NoResponse;
///
///     fn from_message(message: Self, _: ()) -> Self {
///         message
///     }
/// }
/// ```
///
/// If there is a **single message with a return value**, implement the interface as a wrapper for
/// the message and the return [`Sender`]:
///
/// ```
/// use relay_system::{AsyncResponse, FromMessage, Interface, Sender};
///
/// #[derive(Debug)]
/// pub struct MyMessage;
///
/// #[derive(Debug)]
/// pub struct MyInterface(MyMessage, Sender<bool>);
///
/// impl Interface for MyInterface {}
///
/// impl FromMessage<MyMessage> for MyInterface {
///     type Response = AsyncResponse<bool>;
///
///     fn from_message(message: MyMessage, sender: Sender<bool>) -> Self {
///         Self(message, sender)
///     }
/// }
/// ```
///
/// Finally, interfaces with **multiple messages** of any kind can most commonly be implemented
/// through an enumeration for every message. The variants of messages with return values need a
/// `Sender` again:
///
/// ```
/// use relay_system::{AsyncResponse, FromMessage, Interface, NoResponse, Sender};
///
/// #[derive(Debug)]
/// pub struct GetFlag;
///
/// #[derive(Debug)]
/// pub struct SetFlag(pub bool);
///
/// #[derive(Debug)]
/// pub enum MyInterface {
///     Get(GetFlag, Sender<bool>),
///     Set(SetFlag),
/// }
///
/// impl Interface for MyInterface {}
///
/// impl FromMessage<GetFlag> for MyInterface {
///     type Response = AsyncResponse<bool>;
///
///     fn from_message(message: GetFlag, sender: Sender<bool>) -> Self {
///         Self::Get(message, sender)
///     }
/// }
///
/// impl FromMessage<SetFlag> for MyInterface {
///     type Response = NoResponse;
///
///     fn from_message(message: SetFlag, _: ()) -> Self {
///         Self::Set(message)
///     }
/// }
/// ```
///
/// # Requirements
///
/// Interfaces are meant to be sent to services via channels. As such, they need to be both `Send`
/// and `'static`. It is highly encouraged to implement `Debug` on all interfaces and their
/// messages.
pub trait Interface: Send + 'static {}

/// Services without messages can use `()` as their interface.
impl Interface for () {}

/// An error when [sending](Addr::send) a message to a service fails.
#[derive(Clone, Copy, Debug)]
pub struct SendError;

impl fmt::Display for SendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "failed to send message to service")
    }
}

impl std::error::Error for SendError {}

/// Response behavior of an [`Interface`] message.
///
/// It defines how a service handles and responds to interface messages, such as through
/// asynchronous responses or fire-and-forget without responding. [`FromMessage`] implementations
/// declare this behavior on the interface.
///
/// See [`FromMessage`] for more information on how to use this trait.
pub trait MessageResponse {
    /// Sends responses from the service back to the waiting recipient.
    type Sender;

    /// The type returned from [`Addr::send`].
    ///
    /// This type can be either synchronous and asynchronous based on the responder.
    type Output;

    /// Returns the response channel for an interface message.
    fn channel() -> (Self::Sender, Self::Output);
}

/// The request when sending an asynchronous message to a service.
///
/// This is returned from [`Addr::send`] when the message responds asynchronously through
/// [`AsyncResponse`]. It is a future that should be awaited. The message still runs to
/// completion if this future is dropped.
pub struct Request<T>(oneshot::Receiver<T>);

impl<T> fmt::Debug for Request<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Request").finish_non_exhaustive()
    }
}

impl<T> Future for Request<T> {
    type Output = Result<T, SendError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> std::task::Poll<Self::Output> {
        Pin::new(&mut self.0)
            .poll(cx)
            .map(|r| r.map_err(|_| SendError))
    }
}

/// Sends a message response from a service back to the waiting [`Request`].
///
/// The sender is part of an [`AsyncResponse`] and should be moved into the service interface
/// type. If this sender is dropped without calling [`send`](Self::send), the request fails with
/// [`SendError`].
pub struct Sender<T>(oneshot::Sender<T>);

impl<T> fmt::Debug for Sender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sender")
            .field("open", &!self.0.is_closed())
            .finish()
    }
}

impl<T> Sender<T> {
    /// Sends the response value and closes the [`Request`].
    ///
    /// This silenly drops the value if the request has been dropped.
    pub fn send(self, value: T) {
        self.0.send(value).ok();
    }
}

/// Message response resulting in an asynchronous [`Request`].
///
/// The sender must be placed on the interface in [`FromMessage::from_message`].
pub struct AsyncResponse<T>(PhantomData<T>);

impl<T> fmt::Debug for AsyncResponse<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("AsyncResponse")
    }
}

impl<T> MessageResponse for AsyncResponse<T> {
    type Sender = Sender<T>;
    type Output = Request<T>;

    fn channel() -> (Self::Sender, Self::Output) {
        let (tx, rx) = oneshot::channel();
        (Sender(tx), Request(rx))
    }
}

/// Message response for fire-and-forget messages with no output.
///
/// There is no sender associated to this response. When implementing [`FromMessage`], the sender
/// can be ignored.
pub struct NoResponse;

impl fmt::Debug for NoResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("NoResponse")
    }
}

impl MessageResponse for NoResponse {
    type Sender = ();
    type Output = ();

    fn channel() -> (Self::Sender, Self::Output) {
        ((), ())
    }
}

/// Declares a message as part of an [`Interface`].
///
/// Messages have an associated `Response` type that determines the return value of sending the
/// message. Within an interface, the responder can vary for each message. There are two provided
/// responders.
///
/// # No Response
///
/// [`NoResponse`] is used for fire-and-forget messages that do not return any values. These
/// messages do not spawn futures and cannot be awaited. It is neither possible to verify whether
/// the message was delivered to the service.
///
/// When implementing `FromMessage` for such messages, the second argument can be ignored by
/// convention:
///
/// ```
/// use relay_system::{FromMessage, Interface, NoResponse};
///
/// struct MyMessage;
///
/// enum MyInterface {
///     MyMessage(MyMessage),
///     // ...
/// }
///
/// impl Interface for MyInterface {}
///
/// impl FromMessage<MyMessage> for MyInterface {
///     type Response = NoResponse;
///
///     fn from_message(message: MyMessage, _: ()) -> Self {
///         Self::MyMessage(message)
///     }
/// }
/// ```
///
/// # Asynchronous Responses
///
/// [`AsyncResponse`] is used for messages that resolve to some future value. This value is sent
/// back by the service through a [`Sender`], which must be added into the interface:
///
/// ```
/// use relay_system::{AsyncResponse, FromMessage, Interface, Sender};
///
/// struct MyMessage;
///
/// enum MyInterface {
///     MyMessage(MyMessage, Sender<bool>),
///     // ...
/// }
///
/// impl Interface for MyInterface {}
///
/// impl FromMessage<MyMessage> for MyInterface {
///     type Response = AsyncResponse<bool>;
///
///     fn from_message(message: MyMessage, sender: Sender<bool>) -> Self {
///         Self::MyMessage(message, sender)
///     }
/// }
/// ```
///
/// See [`Interface`] for more examples on how to build interfaces using this trait.
pub trait FromMessage<M>: Interface {
    /// The behavior declaring the return value when sending this message.
    type Response: MessageResponse;

    /// Converts the message into the service interface.
    fn from_message(message: M, sender: <Self::Response as MessageResponse>::Sender) -> Self;
}

/// The address of a [`Service`].
///
/// The address of a [`Service`] allows you to [send](Self::send) messages to the service as
/// long as the service is running. It can be freely cloned.
pub struct Addr<I: Interface> {
    tx: mpsc::UnboundedSender<I>,
    queue_size: Arc<AtomicU64>,
}

impl<I: Interface> fmt::Debug for Addr<I> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Addr")
            .field("open", &!self.tx.is_closed())
            .field("queue_size", &self.queue_size.load(Ordering::Relaxed))
            .finish()
    }
}

// Manually derive `Clone` since we do not require `I: Clone` and the Clone derive adds this
// constraint.
impl<I: Interface> Clone for Addr<I> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            queue_size: self.queue_size.clone(),
        }
    }
}

impl<I: Interface> Addr<I> {
    /// Sends a message to the service and returns the response.
    ///
    /// Depending on the message's response behavior, this either returns a future resolving to the
    /// return value, or does not return anything for fire-and-forget messages. The communication
    /// channel with the service is unbounded, so backlogs could occur when sending too many
    /// messages.
    ///
    /// Sending asynchronous messages can fail with `Err(SendError)` if the service has shut down.
    /// The result of asynchronous messages does not have to be awaited. The message will be
    /// delivered and handled regardless:
    pub fn send<M>(&self, message: M) -> <I::Response as MessageResponse>::Output
    where
        I: FromMessage<M>,
    {
        let (tx, rx) = I::Response::channel();
        self.queue_size.fetch_add(1, Ordering::SeqCst);
        self.tx.send(I::from_message(message, tx)).ok(); // it's ok to drop, the response will fail
        rx
    }
}

/// Inbound channel for messages sent through an [`Addr`].
///
/// This channel is meant to be polled in a [`Service`].
///
/// Instances are created automatically when [spawning](Service::spawn_handler) a service, or can be
/// created through [`channel`]. The channel closes when all associated [`Addr`]s are dropped.
pub struct Receiver<I: Interface> {
    rx: mpsc::UnboundedReceiver<I>,
    name: &'static str,
    interval: tokio::time::Interval,
    queue_size: Arc<AtomicU64>,
}

impl<I: Interface> Receiver<I> {
    /// Receives the next value for this receiver.
    ///
    /// This method returns `None` if the channel has been closed and there are
    /// no remaining messages in the channel's buffer. This indicates that no
    /// further values can ever be received from this `Receiver`. The channel is
    /// closed when all senders have been dropped.
    ///
    /// If there are no messages in the channel's buffer, but the channel has
    /// not yet been closed, this method will sleep until a message is sent or
    /// the channel is closed.
    pub async fn recv(&mut self) -> Option<I> {
        loop {
            tokio::select! {
                biased;

                _ = self.interval.tick() => {
                    let backlog = self.queue_size.load(Ordering::Relaxed);
                    relay_statsd::metric!(
                        gauge(SystemGauges::ServiceBackPressure) = backlog,
                        service = self.name
                    );
                },
                message = self.rx.recv() => {
                    self.queue_size.fetch_sub(1, Ordering::SeqCst);
                    return message;
                },
            }
        }
    }
}

impl<I: Interface> fmt::Debug for Receiver<I> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Receiver")
            .field("name", &self.name)
            .field("queue_size", &self.queue_size.load(Ordering::Relaxed))
            .finish()
    }
}

/// Creates an unbounded channel for communicating with a [`Service`].
///
/// The `Addr` as the sending part provides public access to the service, while the `Receiver`
/// should remain internal to the service.
pub fn channel<I: Interface>(name: &'static str) -> (Addr<I>, Receiver<I>) {
    let queue_size = Arc::new(AtomicU64::new(0));
    let (tx, rx) = mpsc::unbounded_channel();

    let addr = Addr {
        tx,
        queue_size: queue_size.clone(),
    };

    let mut interval = tokio::time::interval(BACKLOG_INTERVAL);
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

    let receiver = Receiver {
        rx,
        name,
        interval,
        queue_size,
    };

    (addr, receiver)
}

/// An asynchronous unit responding to messages.
///
/// Services receive messages conforming to some [`Interface`] through an [`Addr`] and handle them
/// one by one. Internally, services are free to concurrently process these messages or not, most
/// probably should.
///
/// Individual messages can have a response which will be sent once the message is handled by the
/// service. The sender can asynchronously await the responses of such messages.
///
/// To start a service, create an instance of the service and use [`Service::start`].
///
/// # Implementing Services
///
/// The standard way to implement services is through the `run` function. It receives an inbound
/// channel for all messages sent through the service's address. Note that this function is
/// synchronous, so that this needs to spawn at least one task internally:
///
/// ```no_run
/// use relay_system::{FromMessage, Interface, NoResponse, Receiver, Service};
///
/// struct MyMessage;
///
/// impl Interface for MyMessage {}
///
/// impl FromMessage<Self> for MyMessage {
///     type Response = NoResponse;
///
///     fn from_message(message: Self, _: ()) -> Self {
///         message
///     }
/// }
///
/// struct MyService;
///
/// impl Service for MyService {
///     type Interface = MyMessage;
///
///     fn spawn_handler(self, mut rx: Receiver<Self::Interface>) {
///         tokio::spawn(async move {
///             while let Some(message) = rx.recv().await {
///                 // handle the message
///             }
///         });
///     }
/// }
///
/// let addr = MyService.start();
/// ```
pub trait Service: Sized {
    /// The interface of messages this service implements.
    ///
    /// The interface can be a single message type or an enumeration of all the messages that
    /// can be handled by this service.
    type Interface: Interface;

    /// Spawns a task to handle service messages.
    ///
    /// Receives an inbound channel for all messages sent through the service's [`Addr`]. Note
    /// that this function is synchronous, so that this needs to spawn a task internally.
    fn spawn_handler(self, rx: Receiver<Self::Interface>);

    /// Starts the service in the current runtime and returns an address for it.
    fn start(self) -> Addr<Self::Interface> {
        let (addr, rx) = channel(Self::name());
        self.spawn_handler(rx);
        addr
    }

    /// Returns a unique name for this service implementation.
    ///
    /// This is used for internal diagnostics and uses the fully qualified type name of the service
    /// implementor by default.
    fn name() -> &'static str {
        std::any::type_name::<Self>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MockMessage;

    impl Interface for MockMessage {}

    impl FromMessage<Self> for MockMessage {
        type Response = NoResponse;

        fn from_message(message: Self, _: ()) -> Self {
            message
        }
    }

    struct MockService;

    impl Service for MockService {
        type Interface = MockMessage;

        fn spawn_handler(self, mut rx: Receiver<Self::Interface>) {
            tokio::spawn(async move {
                while rx.recv().await.is_some() {
                    tokio::time::sleep(BACKLOG_INTERVAL * 2).await;
                }
            });
        }

        fn name() -> &'static str {
            "mock"
        }
    }

    #[test]
    fn test_backpressure_metrics() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();

        let _guard = rt.enter();
        tokio::time::pause();

        // Mock service takes 2 * BACKLOG_INTERVAL for every message
        let addr = MockService.start();

        // Advance the timer by a tiny offset to trigger the first metric emission.
        let captures = relay_statsd::with_capturing_test_client(|| {
            rt.block_on(async {
                tokio::time::sleep(Duration::from_millis(10)).await;
            })
        });

        assert_eq!(captures, ["service.back_pressure:0|g|#service:mock"]);

        // Send messages and advance to 0.5 * INTERVAL. No metrics expected at this point.
        let captures = relay_statsd::with_capturing_test_client(|| {
            rt.block_on(async {
                addr.send(MockMessage); // will be pulled immediately
                addr.send(MockMessage);
                addr.send(MockMessage);

                tokio::time::sleep(BACKLOG_INTERVAL / 2).await;
            })
        });

        assert!(captures.is_empty());

        // Advance to 6.5 * INTERVAL. The service should pull the first message immediately, another
        // message every 2 INTERVALS. The messages are fully handled after 6 INTERVALS, but we
        // cannot observe that since the last message exits the queue at 4.
        let captures = relay_statsd::with_capturing_test_client(|| {
            rt.block_on(async {
                tokio::time::sleep(BACKLOG_INTERVAL * 6).await;
            })
        });

        assert_eq!(
            captures,
            [
                "service.back_pressure:2|g|#service:mock", // 2 * INTERVAL
                "service.back_pressure:1|g|#service:mock", // 4 * INTERVAL
                "service.back_pressure:0|g|#service:mock", // 6 * INTERVAL
            ]
        );
    }
}
