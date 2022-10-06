//! Compatibility layer for bridging between `actix` and `tokio` 1.0.
//!
//! Needed to allow services to communicate with the legacy actors (using `actix` 0.7.9).
//! The problem that is addressed arises when the queues of the channels fill up and tasks are
//! getting parked. When that happens `task::current()` is executed which panics.

use actix::prelude::*;
use futures::channel::oneshot;
use futures01::prelude::*;
use once_cell::sync::OnceCell;

/// Custom `tokio` 0.1 runtime.
///
/// Needed when sending messages from the new Actors to old Actors to avoid panics when the
/// channel queues fill up and tasks are being parked.
static RUNTIME: OnceCell<tokio01::runtime::Runtime> = OnceCell::new();

/// Needed so that `EXECUTOR.spawn` can be called [`send`].
static EXECUTOR: OnceCell<tokio01::runtime::TaskExecutor> = OnceCell::new();

/// Initializes the compatibility layer.
pub fn init() {
    let system = System::current();

    let runtime = RUNTIME.get_or_init(move || {
        tokio01::runtime::Builder::new()
            .core_threads(1)
            .blocking_threads(1)
            .after_start(move || System::set_current(system.clone()))
            .build()
            .unwrap()
    });

    EXECUTOR.get_or_init(|| runtime.executor());
}

/// Sends a message to an actor using `actix` 0.7 from a `tokio` 1.0 context.
///
/// The message is internally forwarded through a `tokio` 0.1 runtime in order to avoid panics when
/// the channel queues fill up and tasks are getting parked.
///
/// # Panics
///
/// Panics if this is invoked outside of the [`Controller`](crate::Controller).
pub async fn send<A, M>(addr: Addr<A>, msg: M) -> Result<M::Result, MailboxError>
where
    A: Actor + Send, // NOTE: Addr only implements Send if the actor does
    M: Message + Send + 'static,
    M::Result: Send,
    A: Handler<M>,
    A::Context: dev::ToEnvelope<A, M>,
{
    let (tx, rx) = oneshot::channel();
    let f = futures01::future::lazy(move || addr.send(msg))
        .then(|res| tx.send(res))
        .map_err(|_| ());
    EXECUTOR.get().unwrap().spawn(f);
    rx.await.map_err(|_| MailboxError::Closed)?
}

/// Sends a message to a recipient using `actix` 0.7 from a `tokio` 1.0 context.
///
/// The message is internally forwarded through a `tokio` 0.1 runtime in order to avoid panics when
/// the channel queues fill up and tasks are getting parked.
///
/// # Panics
///
/// Panics if this is invoked outside of the [`Controller`](crate::Controller).
///
/// NOTE: required by ProjectCache and will be removed with ProjectCache migration.
pub async fn send_to_recipient<M>(addr: Recipient<M>, msg: M) -> Result<M::Result, MailboxError>
where
    M: Message + Send + 'static,
    M::Result: Send,
{
    let (tx, rx) = oneshot::channel();
    let f = futures01::future::lazy(move || addr.send(msg))
        .then(|res| tx.send(res))
        .map_err(|_| ());
    EXECUTOR.get().unwrap().spawn(f);
    rx.await.map_err(|_| MailboxError::Closed)?
}
