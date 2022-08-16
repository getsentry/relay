//! Our definition of a service.

use std::fmt;
use std::future::Future;

use tokio::sync::{mpsc, oneshot};

/// Our definition of a service.
///
/// Services are much like actors: they receive messages from an inbox and handles them one
/// by one.  Services are free to concurrently process these messages or not, most probably
/// should.
///
/// Messages always have a response which will be sent once the message is handled by the
/// service.
pub trait Service {
    /// The envelope is what is sent to the inbox of this service.
    ///
    /// It is an enum of all the message types that can be handled by this service together
    /// with the response [sender](oneshot::Sender) for each message.
    type Envelope: Send + 'static;
}

/// A message which can be sent to a service.
///
/// Messages have an associated `Response` type and can be unconditionally converted into
/// the envelope type of their [`Service`].
pub trait ServiceMessage<S: Service> {
    /// The type of the `Response`.
    type Response: Send + 'static;

    /// Creates and returns an envelope for the message and the receiver to the transmitter
    ///  contained in the Envelope.
    fn into_envelope(self) -> (S::Envelope, oneshot::Receiver<Self::Response>);
}

/// An error when [sending](Addr::send) a message to a service fails.
#[derive(Clone, Copy, Debug)]
pub struct SendError;

impl fmt::Display for SendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "failed to send message to service")
    }
}

impl std::error::Error for SendError {}

/// The address for a [`Service`].
///
/// The address of a [`Service`] allows you to [send](Addr::send) messages to the service as
/// long as the service is running.  It can be freely cloned.
#[derive(Debug)]
pub struct Addr<S: Service> {
    /// The transmitter of the channel used to communicate with the Service
    pub tx: mpsc::UnboundedSender<S::Envelope>,
}

// Manually derive clone since we do not require `S: Clone` and the Clone derive adds this
// constraint.
impl<S: Service> Clone for Addr<S> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
        }
    }
}

impl<S: Service> Addr<S> {
    /// Sends an asynchronous message to the service and waits for the response.
    ///
    /// The result of the message does not have to be awaited. The message will be delivered and
    /// handled regardless. The communication channel with the service is unbounded, so backlogs
    /// could occur when sending too many messages.
    ///
    /// Sending the message can fail with `Err(SendError)` if the service has shut down.
    // Note: this is written as returning `impl Future` instead of `async fn` in order not
    // to capture the lifetime of `&self` in the returned future.
    pub fn send<M>(&self, message: M) -> impl Future<Output = Result<M::Response, SendError>>
    where
        M: ServiceMessage<S>,
    {
        let (envelope, response_rx) = message.into_envelope();
        let res = self.tx.send(envelope);
        async move {
            res.map_err(|_| SendError)?;
            response_rx.await.map_err(|_| SendError)
        }
    }
}
