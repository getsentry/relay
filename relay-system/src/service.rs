use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use futures::future::Shared;
use futures::FutureExt;
use tokio::runtime::Runtime;
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
#[derive(Clone, Copy, Debug, PartialEq)]
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
///
/// See [`FromMessage`] and [`Service`] for implementation advice and examples.
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
///
/// See [`FromMessage`] and [`Service`] for implementation advice and examples.
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

/// Initial response to a [`BroadcastRequest`].
#[derive(Debug)]
enum InitialResponse<T> {
    /// The response value is immediately ready.
    ///
    /// The sender did not attach to a broadcast channel and instead resolved the requested value
    /// immediately. The request is now ready and can resolve. See [`BroadcastChannel::attach`].
    Ready(T),
    /// The sender is attached to a channel that needs to be polled.
    Poll(Shared<oneshot::Receiver<T>>),
}

/// States of a [`BroadcastRequest`].
enum BroadcastState<T> {
    /// The request is waiting for an initial response.
    Pending(oneshot::Receiver<InitialResponse<T>>),
    /// The request is attached to a [`BroadcastChannel`].
    Attached(Shared<oneshot::Receiver<T>>),
}

/// The request when sending an asynchronous message to a service.
///
/// This is returned from [`Addr::send`] when the message responds asynchronously through
/// [`BroadcastResponse`]. It is a future that should be awaited. The message still runs to
/// completion if this future is dropped.
///
/// # Panics
///
/// This future is not fused and panics if it is polled again after it has resolved.
pub struct BroadcastRequest<T>(BroadcastState<T>)
where
    T: Clone;

impl<T: Clone> Future for BroadcastRequest<T> {
    type Output = Result<T, SendError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(loop {
            match self.0 {
                BroadcastState::Pending(ref mut pending) => {
                    match futures::ready!(Pin::new(pending).poll(cx)) {
                        Ok(InitialResponse::Ready(value)) => break Ok(value),
                        Ok(InitialResponse::Poll(shared)) => {
                            self.0 = BroadcastState::Attached(shared)
                        }
                        Err(_) => break Err(SendError),
                    }
                }
                BroadcastState::Attached(ref mut shared) => {
                    match futures::ready!(Pin::new(shared).poll(cx)) {
                        Ok(value) => break Ok(value),
                        Err(_) => break Err(SendError),
                    }
                }
            }
        })
    }
}

/// A channel that broadcasts values to attached [senders](BroadcastSender).
///
/// This is part of the [`BroadcastResponse`] message behavior to efficiently send delayed responses
/// to a large number of senders. All requests that are attached to this channel via their senders
/// resolve with the same value.
///
/// # Example
///
/// ```
/// use relay_system::{BroadcastChannel, BroadcastSender};
///
/// struct MyService {
///     channel: Option<BroadcastChannel<String>>,
/// }
///
/// impl MyService {
///     fn handle_message(&mut self, sender: BroadcastSender<String>) {
///         if let Some(ref mut channel) = self.channel {
///             channel.attach(sender);
///         } else {
///             self.channel = Some(sender.into_channel());
///         }
///     }
///
///     fn finish_compute(&mut self, value: String) {
///         if let Some(channel) = self.channel.take() {
///             channel.send(value);
///         }
///     }
/// }
/// ```
#[derive(Debug)]
pub struct BroadcastChannel<T>
where
    T: Clone,
{
    tx: oneshot::Sender<T>,
    rx: Shared<oneshot::Receiver<T>>,
}

impl<T: Clone> BroadcastChannel<T> {
    /// Creates a standalone channel.
    ///
    /// Use [`attach`](Self::attach) to add senders to this channel. Alternatively, create a channel
    /// with [`BroadcastSender::into_channel`].
    pub fn new() -> Self {
        let (tx, rx) = oneshot::channel();
        Self {
            tx,
            rx: rx.shared(),
        }
    }

    /// Attaches a sender of another message to this channel to receive the same value.
    ///
    /// # Example
    ///
    /// ```
    /// use relay_system::{BroadcastChannel, BroadcastResponse, BroadcastSender};
    /// # use relay_system::MessageResponse;
    ///
    /// // This is usually done as part of `Addr::send`
    /// let (sender, rx) = BroadcastResponse::<&str>::channel();
    ///
    /// let mut channel = BroadcastChannel::new();
    /// channel.attach(sender);
    /// ```
    pub fn attach(&mut self, sender: BroadcastSender<T>) {
        sender.0.send(InitialResponse::Poll(self.rx.clone())).ok();
    }

    /// Sends a value to all attached senders and closes the channel.
    ///
    /// This method succeeds even if no senders are attached to this channel anymore. To check if
    /// this channel is still active with senders attached, use [`is_attached`](Self::is_attached).
    ///
    /// # Example
    ///
    /// ```
    /// use relay_system::BroadcastResponse;
    /// # use relay_system::MessageResponse;
    /// # tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
    ///
    /// // This is usually done as part of `Addr::send`
    /// let (sender, rx) = BroadcastResponse::<&str>::channel();
    ///
    /// let channel = sender.into_channel();
    /// channel.send("test");
    /// assert_eq!(rx.await, Ok("test"));
    /// # })
    /// ```
    pub fn send(self, value: T) {
        self.tx.send(value).ok();
    }

    /// Returns `true` if there are [requests](BroadcastRequest) waiting for this channel.
    ///
    /// The channel is not permanently closed when all waiting requests have detached. A new sender
    /// can be attached using [`attach`](Self::attach) even after this method returns `false`.
    ///
    /// # Example
    ///
    /// ```
    /// use relay_system::BroadcastResponse;
    /// # use relay_system::MessageResponse;
    ///
    /// // This is usually done as part of `Addr::send`
    /// let (sender, rx) = BroadcastResponse::<&str>::channel();
    ///
    /// let channel = sender.into_channel();
    /// assert!(channel.is_attached());
    ///
    /// drop(rx);
    /// assert!(!channel.is_attached());
    /// ```
    pub fn is_attached(&self) -> bool {
        self.rx.strong_count() > Some(1)
    }
}

impl<T: Clone> Default for BroadcastChannel<T> {
    fn default() -> Self {
        Self::new()
    }
}

/// Sends a message response from a service back to the waiting [`BroadcastRequest`].
///
/// The sender is part of an [`BroadcastResponse`] and should be moved into the service interface
/// type. If this sender is dropped without calling [`send`](Self::send), the request fails with
/// [`SendError`].
///
/// As opposed to the regular [`Sender`] for asynchronous messages, this sender can be converted
/// into a [channel](Self::into_channel) that efficiently shares a common response for multiple
/// requests to the same data value. This is useful if resolving or computing the value takes more
/// time.
///
/// # Example
///
/// ```
/// use relay_system::BroadcastResponse;
/// # use relay_system::MessageResponse;
/// # tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
///
/// // This is usually done as part of `Addr::send`
/// let (sender1, rx1) = BroadcastResponse::<&str>::channel();
/// let (sender2, rx2) = BroadcastResponse::<&str>::channel();
///
/// // On the first time, convert the sender into a channel
/// let mut channel = sender1.into_channel();
///
/// // The second time, attach the sender to the existing channel
/// channel.attach(sender2);
///
/// // Send a value into the channel to resolve all requests simultaneously
/// channel.send("test");
/// assert_eq!(rx1.await, Ok("test"));
/// assert_eq!(rx2.await, Ok("test"));
/// # })
/// ```
#[derive(Debug)]
pub struct BroadcastSender<T>(oneshot::Sender<InitialResponse<T>>)
where
    T: Clone;

impl<T: Clone> BroadcastSender<T> {
    /// Immediately resolve a ready value.
    ///
    /// This bypasses shared channels and directly sends the a value to the waiting
    /// [request](BroadcastRequest). In terms of performance and behavior, using `send` is
    /// equivalent to calling [`Sender::send`] for a regular [`AsyncResponse`].
    ///
    /// # Example
    ///
    /// ```
    /// use relay_system::BroadcastResponse;
    /// # use relay_system::MessageResponse;
    /// # tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
    ///
    /// // This is usually done as part of `Addr::send`
    /// let (sender, rx) = BroadcastResponse::<&str>::channel();
    ///
    /// // sender is NOT converted into a channel!
    ///
    /// sender.send("test");
    /// assert_eq!(rx.await, Ok("test"));
    /// # })
    /// ```
    pub fn send(self, value: T) {
        self.0.send(InitialResponse::Ready(value)).ok();
    }

    /// Creates a channel from this sender that can be shared with other senders.
    ///
    /// To add more senders to the created channel at a later point, use
    /// [`attach`](BroadcastChannel::attach).
    ///
    /// # Example
    ///
    /// ```
    /// use relay_system::{BroadcastChannel, BroadcastResponse};
    /// # use relay_system::MessageResponse;
    ///
    /// // This is usually done as part of `Addr::send`
    /// let (sender, rx) = BroadcastResponse::<&str>::channel();
    ///
    /// let channel: BroadcastChannel<&str> = sender.into_channel();
    /// ```
    pub fn into_channel(self) -> BroadcastChannel<T> {
        let mut channel = BroadcastChannel::new();
        channel.attach(self);
        channel
    }
}

/// Variation of [`AsyncResponse`] that efficiently broadcasts responses to many requests.
///
/// This response behavior is useful for services that cache or debounce requests. Instead of
/// responding to each equivalent request via its individual sender, the broadcast behavior allows
/// to create a [`BroadcastChannel`] that efficiently resolves all pending requests once the value
/// is ready.
///
/// Similar to `AsyncResponse`, the service receives a sender that it can use to send a value
/// directly back to the waiting request. Additionally, the sender can be converted into a channel
/// or attached to an already existing channel, if the service expects more requests while computing
/// the response.
///
/// See [`FromMessage`] and [`Service`] for implementation advice and examples.
pub struct BroadcastResponse<T>(PhantomData<T>)
where
    T: Clone;

impl<T: Clone> fmt::Debug for BroadcastResponse<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("BroadcastResponse")
    }
}

impl<T: Clone> MessageResponse for BroadcastResponse<T> {
    type Sender = BroadcastSender<T>;
    type Output = BroadcastRequest<T>;

    fn channel() -> (Self::Sender, Self::Output) {
        let (tx, rx) = oneshot::channel();
        (
            BroadcastSender(tx),
            BroadcastRequest(BroadcastState::Pending(rx)),
        )
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
/// # Broadcast Responses
///
/// [`BroadcastResponse`] is similar to the previous asynchronous response, but it additionally
/// allows to efficiently handle duplicate requests for services that debounce equivalent requests
/// or cache results. On the requesting side, this behavior is identical to the asynchronous
/// behavior, but it provides more utilities to the implementing service.
///
/// ```
/// use relay_system::{BroadcastResponse, BroadcastSender, FromMessage, Interface};
///
/// struct MyMessage;
///
/// enum MyInterface {
///     MyMessage(MyMessage, BroadcastSender<bool>),
///     // ...
/// }
///
/// impl Interface for MyInterface {}
///
/// impl FromMessage<MyMessage> for MyInterface {
///     type Response = BroadcastResponse<bool>;
///
///     fn from_message(message: MyMessage, sender: BroadcastSender<bool>) -> Self {
///         Self::MyMessage(message, sender)
///     }
/// }
/// ```
///
/// See [`Interface`] for more examples on how to build interfaces using this trait and [`Service`]
/// documentation for patterns and advice to handle messages.
pub trait FromMessage<M>: Interface {
    /// The behavior declaring the return value when sending this message.
    type Response: MessageResponse;

    /// Converts the message into the service interface.
    fn from_message(message: M, sender: <Self::Response as MessageResponse>::Sender) -> Self;
}

/// Abstraction over address types for service channels.
trait SendDispatch<M>: Send + Sync {
    /// The behavior declaring the return value when sending this message.
    ///
    /// When this is implemented for a type bound to an [`Interface`], this is the same behavior as
    /// used in [`FromMessage::Response`].
    type Response: MessageResponse;

    /// Sends a message to the service and returns the response.
    ///
    /// See [`Addr::send`] for more information on a concrete type.
    fn send(&self, message: M) -> <Self::Response as MessageResponse>::Output;

    /// Returns a trait object of this type.
    fn to_trait_object(&self) -> Box<dyn SendDispatch<M, Response = Self::Response>>;
}

/// An address to a [`Service`] implementing any interface that takes a given message.
///
/// This is similar to an [`Addr`], but it is bound to a single message rather than an interface. As
/// such, this type is not meant for communicating with a service implementation, but rather as a
/// handle to any service that can consume a given message. These can be back-channels or hooks that
/// are configured externally through Inversion of Control (IoC).
///
/// Recipients are created through [`Addr::recipient`].
pub struct Recipient<M, R> {
    inner: Box<dyn SendDispatch<M, Response = R>>,
}

impl<M, R> Recipient<M, R>
where
    R: MessageResponse,
{
    /// Sends a message to the service and returns the response.
    ///
    /// This is equivalent to [`send`](Addr::send) on the originating address.
    pub fn send(&self, message: M) -> R::Output {
        self.inner.send(message)
    }
}

// Manual implementation since `XSender` cannot require `Clone` for object safety.
impl<M, R: MessageResponse> Clone for Recipient<M, R> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.to_trait_object(),
        }
    }
}

/// The address of a [`Service`].
///
/// Addresses allow to [send](Self::send) messages to a service that implements a corresponding
/// [`Interface`] as long as the service is running.
///
/// Addresses can be freely cloned. When the last clone is dropped, the message channel of the
/// service closes permanently, which signals to the service that it can shut down.
pub struct Addr<I: Interface> {
    tx: mpsc::UnboundedSender<I>,
    queue_size: Arc<AtomicU64>,
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

    /// Returns a handle that can receive a given message independent of the interface.
    ///
    /// See [`Recipient`] for more information and examples.
    pub fn recipient<M>(self) -> Recipient<M, I::Response>
    where
        I: FromMessage<M>,
    {
        Recipient {
            inner: Box::new(self),
        }
    }

    /// Custom address used for testing.
    ///
    /// Returns the receiving end of the channel for inspection.
    pub fn custom() -> (Self, mpsc::UnboundedReceiver<I>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (
            Addr {
                tx,
                queue_size: Default::default(),
            },
            rx,
        )
    }

    /// Dummy address used for testing.
    pub fn dummy() -> Self {
        Self::custom().0
    }
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

impl<I, M> SendDispatch<M> for Addr<I>
where
    I: Interface + FromMessage<M>,
{
    type Response = <I as FromMessage<M>>::Response;

    fn send(&self, message: M) -> <Self::Response as MessageResponse>::Output {
        Addr::send(self, message)
    }

    fn to_trait_object(&self) -> Box<dyn SendDispatch<M, Response = Self::Response>> {
        Box::new(self.clone())
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
///
/// ## Debounce and Caching
///
/// Services that cache or debounce their responses can benefit from the [`BroadcastResponse`]
/// behavior. To use this behavior, implement the message and interface identical to
/// [`AsyncResponse`] above. This will provide a different sender type that can be converted into a
/// channel to debounce responses. It is still possible to send values directly via the sender
/// without a broadcast channel.
///
/// ```
/// use std::collections::btree_map::{BTreeMap, Entry};
/// use relay_system::{BroadcastChannel, BroadcastSender};
///
/// // FromMessage implementation using BroadcastResponse omitted for brevity.
///
/// struct MyService {
///     cache: BTreeMap<u32, String>,
///     channels: BTreeMap<u32, BroadcastChannel<String>>,
/// }
///
/// impl MyService {
///     fn handle_message(&mut self, id: u32, sender: BroadcastSender<String>) {
///         if let Some(cached) = self.cache.get(&id) {
///             sender.send(cached.clone());
///             return;
///         }
///
///         match self.channels.entry(id) {
///             Entry::Vacant(entry) => {
///                 entry.insert(sender.into_channel());
///                 // Start async computation here.
///             }
///             Entry::Occupied(mut entry) => {
///                 entry.get_mut().attach(sender);
///             }
///         }
///     }
///
///     fn finish_compute(&mut self, id: u32, value: String) {
///         if let Some(channel) = self.channels.remove(&id) {
///             channel.send(value.clone());
///         }
///
///         self.cache.insert(id, value);
///     }
/// }
/// ```
///
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

    /// Starts the service in the given runtime and returns an address for it.
    fn start_in(self, runtime: &Runtime) -> Addr<Self::Interface> {
        let _guard = runtime.enter();
        self.start()
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
