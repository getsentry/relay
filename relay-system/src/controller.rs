use std::fmt;
use std::time::Duration;

use actix::actors::signal;
use actix::fut;
use actix::prelude::*;
use actix::SystemRunner;
use futures01::future;
use futures01::prelude::*;
use once_cell::sync::OnceCell;
use tokio::sync::watch;

#[doc(inline)]
pub use actix::actors::signal::{Signal, SignalType};

use crate::compat;

type ShutdownChannel = (
    watch::Sender<Option<Shutdown>>,
    watch::Receiver<Option<Shutdown>>,
);

/// Global [`ShutdownChannel`] for all services.
static SHUTDOWN: OnceCell<ShutdownChannel> = OnceCell::new();

/// Notifies a service about an upcoming shutdown.
// TODO: The receiver of this message can not yet signal they have completed
// shutdown.
// TODO: Refactor this to a `Recipient`.
pub struct ShutdownHandle(watch::Receiver<Option<Shutdown>>);

impl ShutdownHandle {
    /// Wait for a shutdown.
    ///
    /// This method is cancellation safe and can be used in `select!`.
    pub async fn notified(&mut self) -> Shutdown {
        while self.0.changed().await.is_ok() {
            if let Some(shutdown) = &*self.0.borrow() {
                return shutdown.clone();
            }
        }

        Shutdown { timeout: None }
    }
}

/// Actor to start and gracefully stop an actix system.
///
/// This actor contains a static `run` method which will run an actix system and block the current
/// thread until the system shuts down again.
///
/// This actor starts with default configuration. To change this configuration, send the
/// [`Configure`] message.
///
/// To shut down more gracefully, other actors can register with the [`Subscribe`] message. When a
/// shutdown signal is sent to the process, they will receive a [`Shutdown`] message with an
/// optional timeout. They can respond with a future, after which they will be stopped. Once all
/// registered actors have stopped successfully, the entire system will stop.
///
/// ### Example
///
/// ```
/// use actix::prelude::*;
/// use relay_system::{Controller, Shutdown};
///
/// struct MyActor;
///
/// impl Actor for MyActor {
///     type Context = Context<Self>;
///
///     fn started(&mut self, context: &mut Self::Context) {
///         Controller::subscribe(context.address());
///     }
/// }
///
/// impl Handler<Shutdown> for MyActor {
///     type Result = Result<(), ()>;
///
///     fn handle(&mut self, message: Shutdown, _context: &mut Self::Context) -> Self::Result {
///         // Implement custom logic here
///         Ok(())
///     }
/// }
///
///
/// Controller::run(tokio::runtime::Runtime::new().unwrap().handle(), System::new("my-actix-system"), || -> Result<(), ()> {
///     MyActor.start();
///     # System::current().stop();
///     Ok(())
/// }).unwrap();
/// ```
pub struct Controller {
    /// Configured timeout for graceful shutdowns.
    timeout: Duration,
    /// Subscribed actors for the shutdown message.
    subscribers: Vec<Recipient<Shutdown>>,
}

impl Controller {
    /// Get actor's address from system registry.
    pub fn from_registry() -> Addr<Self> {
        SystemService::from_registry()
    }

    /// Runs the `factory` to start actors.
    ///
    /// The function accepts the old tokio 0.x [`actix::SystemRunner`] and the reference to
    /// [`tokio::runtime::Handle`] from the new tokio 1.x runtime, which we enter before the
    /// factory is run, to make sure that two systems, old and new one, are available.
    ///
    /// The factory may be used to start actors in the actix system before it runs. If the factory
    /// returns an error, the actix system is not started and instead an error returned. Otherwise,
    /// the system blocks the current thread until a shutdown signal is sent to the server and all
    /// actors have completed a graceful shutdown.
    pub fn run<F, R, E>(
        handle: &tokio::runtime::Handle,
        sys: SystemRunner,
        factory: F,
    ) -> Result<(), E>
    where
        F: FnOnce() -> Result<R, E> + 'static,
        F: Sync + Send,
    {
        compat::init();

        // While starting http server ensure that the new tokio 1.x system is available.
        let _guard = handle.enter();
        factory()?;

        // Ensure that the controller starts if no actor has started it yet. It will register with
        // `ProcessSignals` shut down even if no actors have subscribed. If we remove this line, the
        // controller will not be instantiated and our system will not listen for signals.
        Controller::from_registry();

        // All actors have started successfully. Run the system, which blocks the current thread
        // until a signal arrives or `Controller::stop` is called.
        relay_log::info!("relay server starting");
        sys.run();

        Ok(())
    }

    /// Subscribes the provided actor to the [`Shutdown`] signal of the system controller.
    pub fn subscribe<A>(addr: Addr<A>)
    where
        A: Handler<Shutdown>,
        A::Context: actix::dev::ToEnvelope<A, Shutdown>,
    {
        Controller::from_registry().do_send(Subscribe(addr.recipient()))
    }

    /// Returns a [handle](ShutdownHandle) to receive shutdown notifications.
    pub fn shutdown_handle() -> ShutdownHandle {
        let (_, ref rx) = SHUTDOWN.get_or_init(|| watch::channel(None));
        ShutdownHandle(rx.clone())
    }

    /// Performs a graceful shutdown with the given timeout.
    ///
    /// This sends a `Shutdown` message to all subscribed actors and waits for them to finish. As
    /// soon as all actors have completed, `Controller::stop` is called.
    fn shutdown(&mut self, context: &mut Context<Self>, timeout: Option<Duration>) {
        // Send a shutdown signal to all registered subscribers (including self). They will report
        // when the shutdown has completed. Note that we ignore all errors to make sure that we
        // don't cancel the shutdown of other actors if one actor fails.
        let (tx, _) = SHUTDOWN.get_or_init(|| watch::channel(None));
        tx.send(Some(Shutdown { timeout })).ok();

        let futures: Vec<_> = self
            .subscribers
            .iter()
            .map(|recipient| recipient.send(Shutdown { timeout }))
            .map(|future| future.then(|_| Ok(())))
            .collect();

        future::join_all(futures)
            .into_actor(self)
            .and_then(move |_, _, ctx| {
                // Once all shutdowns have completed, we can schedule a stop of the actix system. It is
                // performed with a slight delay to give pending synced futures a chance to perform their
                // error handlers.
                //
                // Delay the shutdown for 100ms to allow synchronized futures to execute their error
                // handlers. Once `System::stop` is called, futures won't be polled anymore and we will not
                // be able to print error messages.
                let when =
                    timeout.unwrap_or_else(|| Duration::from_secs(0)) + Duration::from_millis(100);

                ctx.run_later(when, |_, _| {
                    System::current().stop();
                });
                fut::ok(())
            })
            .spawn(context);
    }
}

impl Default for Controller {
    fn default() -> Self {
        Controller {
            timeout: Duration::from_secs(0),
            subscribers: Vec::new(),
        }
    }
}

impl fmt::Debug for Controller {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Controller")
            .field("timeout", &self.timeout)
            .field("subscribers", &self.subscribers.len())
            .finish()
    }
}

impl Actor for Controller {
    type Context = Context<Self>;

    fn started(&mut self, context: &mut Self::Context) {
        signal::ProcessSignals::from_registry()
            .do_send(signal::Subscribe(context.address().recipient()));
    }
}

impl Supervised for Controller {}

impl SystemService for Controller {}

impl Handler<signal::Signal> for Controller {
    type Result = ();

    fn handle(&mut self, message: signal::Signal, context: &mut Self::Context) -> Self::Result {
        match message.0 {
            signal::SignalType::Int => {
                relay_log::info!("SIGINT received, exiting");
                self.shutdown(context, None);
            }
            signal::SignalType::Quit => {
                relay_log::info!("SIGQUIT received, exiting");
                self.shutdown(context, None);
            }
            signal::SignalType::Term => {
                let timeout = self.timeout;
                relay_log::info!("SIGTERM received, stopping in {}s", timeout.as_secs());
                self.shutdown(context, Some(timeout));
            }
            _ => (),
        }
    }
}

/// Configures the [`Controller`] with new parameters.
#[derive(Debug)]
pub struct Configure {
    /// The maximum shutdown timeout before killing actors.
    pub shutdown_timeout: Duration,
}

impl Message for Configure {
    type Result = ();
}

impl Handler<Configure> for Controller {
    type Result = ();

    fn handle(&mut self, message: Configure, _context: &mut Self::Context) -> Self::Result {
        self.timeout = message.shutdown_timeout;
    }
}

/// Subscribtion message for [`Shutdown`] events.
pub struct Subscribe(pub Recipient<Shutdown>);

impl fmt::Debug for Subscribe {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Subscribe(Shutdown)")
    }
}

impl Message for Subscribe {
    type Result = ();
}

impl Handler<Subscribe> for Controller {
    type Result = ();

    fn handle(&mut self, message: Subscribe, _context: &mut Self::Context) -> Self::Result {
        self.subscribers.push(message.0)
    }
}

/// Shutdown request message sent by the [`Controller`] to subscribed actors.
///
/// A handler has to ensure that it doesn't take longer than `timeout` to resolve the future.
/// Ideally, open work is persisted or finished in an orderly manner but no new requests are
/// accepted anymore.
///
/// After the timeout the system will shut down regardless of what the receivers of this message
/// do.
///
/// The return value is fully ignored. It is only `Result` such that futures can be executed inside
/// a handler.
#[derive(Debug, Clone)]
pub struct Shutdown {
    /// The timeout for this shutdown. `None` indicates an immediate forced shutdown.
    pub timeout: Option<Duration>,
}

impl Message for Shutdown {
    type Result = Result<(), ()>;
}
