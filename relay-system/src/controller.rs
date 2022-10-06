use std::fmt;
use std::time::Duration;

use actix::actors::signal;
use actix::fut;
use actix::prelude::*;
use futures::compat::Future01CompatExt;
use futures01::future;
use futures01::prelude::*;
use tokio::sync::watch;

#[doc(inline)]
pub use actix::actors::signal::{Signal, SignalType};

use crate::compat;

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
/// Controller::run(|| -> Result<(), ()> {
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
    /// Handed out to actors who wish to subscribe to the [`Shutdown`] message.
    shutdown_receiver: watch::Receiver<Option<Shutdown>>,
    /// The sender for the [`Shutdown`] message.
    shutdown_sender: watch::Sender<Option<Shutdown>>,
}

impl Controller {
    /// Get actor's address from system registry.
    pub fn from_registry() -> Addr<Self> {
        SystemService::from_registry()
    }

    /// Starts an actix system and runs the `factory` to start actors.
    ///
    /// The factory may be used to start actors in the actix system before it runs. If the factory
    /// returns an error, the actix system is not started and instead an error returned. Otherwise,
    /// the system blocks the current thread until a shutdown signal is sent to the server and all
    /// actors have completed a graceful shutdown.
    pub fn run<F, R, E>(factory: F) -> Result<(), E>
    where
        F: FnOnce() -> Result<R, E>,
    {
        let sys = System::new("relay");

        compat::init();

        // Run the factory and exit early if an error happens. The return value of the factory is
        // discarded for convenience, to allow shorthand notations.
        factory()?;

        // Ensure that the controller starts if no actor has started it yet. It will register with
        // `ProcessSignals` shut down even if no actors have subscribed. If we remove this line, the
        // controller will not be instantiated and our system will not listen for signals.
        Controller::from_registry();

        // All actors have started successfully. Run the system, which blocks the current thread
        // until a signal arrives or `Controller::stop` is called.
        relay_log::info!("relay server starting");
        sys.run();
        relay_log::info!("relay shutdown complete");

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

    /// Subscribes to the [`Shutdown`] message to handle graceful shutdown.
    ///
    /// Returns a receiver for the [`Shutdown`] message, to be used to gracefully
    /// shutdown.  This sends a message to the [`Controller`] actor so will
    /// block until this actor is running.
    ///
    /// TODO(tobias): The receiver of this message can not yet signal they have completed
    /// shutdown.
    pub async fn subscribe_v2() -> watch::Receiver<Option<Shutdown>> {
        Controller::from_registry()
            .send(SubscribeV2())
            .compat()
            .await
            .unwrap() // FIXME: Remove this later
    }

    /// Performs a graceful shutdown with the given timeout.
    ///
    /// This sends a `Shutdown` message to all subscribed actors and waits for them to finish. As
    /// soon as all actors have completed, `Controller::stop` is called.
    fn shutdown(&mut self, context: &mut Context<Self>, timeout: Option<Duration>) {
        // Send a shutdown signal to all registered subscribers (including self). They will report
        // when the shutdown has completed. Note that we ignore all errors to make sure that we
        // don't cancel the shutdown of other actors if one actor fails.
        self.shutdown_sender.send(Some(Shutdown { timeout })).ok();

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
        let (shutdown_sender, shutdown_receiver) = watch::channel(None);

        Controller {
            timeout: Duration::from_secs(0),
            subscribers: Vec::new(),
            shutdown_receiver,
            shutdown_sender,
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

/// Internal message to handle subscribing to the [`Shutdown`] message.
#[derive(Debug)]
pub struct SubscribeV2();

impl Message for SubscribeV2 {
    type Result = watch::Receiver<Option<Shutdown>>;
}

impl Handler<SubscribeV2> for Controller {
    type Result = MessageResult<SubscribeV2>;

    fn handle(&mut self, _: SubscribeV2, _: &mut Self::Context) -> Self::Result {
        MessageResult(self.shutdown_receiver.clone())
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
#[derive(Debug)]
pub struct Shutdown {
    /// The timeout for this shutdown. `None` indicates an immediate forced shutdown.
    pub timeout: Option<Duration>,
}

impl Message for Shutdown {
    type Result = Result<(), ()>;
}
