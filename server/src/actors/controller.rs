//! Defines an actor to control system run and shutdown.
//!
//! See the [`Controller`] struct for more information.
//!
//! [`Controller`]: struct.Controller.html

use std::time::Duration;

use ::actix::actors::signal;
use ::actix::fut;
use ::actix::prelude::*;
use futures::future;
use futures::prelude::*;

use crate::constants::SHUTDOWN_TIMEOUT;

pub use crate::service::ServerError;
pub use crate::utils::TimeoutError;

/// Actor to start and gracefully stop an actix system.
///
/// This actor contains a static `run` method which will run an actix system and block the current
/// thread until the system shuts down again.
///
/// To shut down more gracefully, other actors can register with the [`Subscribe`] message. When a
/// shutdown signal is sent to the process, they will receive a [`Shutdown`] message with an
/// optional timeout. They can respond with a future, after which they will be stopped. Once all
/// registered actors have stopped successfully, the entire system will stop.
///
/// ### Example
///
/// ```ignore
/// # use ::actix::prelude::*;
/// struct MyActor;
///
/// impl Actor for MyActor {
///     type Context = Context<Self>;
///
///     fn started(&mut self, context: &mut Self::Context) {
///         Controller::from_registry()
///             .do_send(Subscribe(context.address().recipient()));
///     }
/// }
///
/// impl Handler<Shutdown> for MyActor {
///     type Result = Result<(), TimeoutError>;
///
///     fn handle(&mut self, message: Shutdown, _context: &mut Self::Context) -> Self::Result {
///         // Implement custom logic here
///         Ok(())
///     }
/// }
///
/// fn main() {
///     Controller::run(|| {
///         MyActor.start();
///         # System::current().stop()
///     })
/// }
/// ```
///
/// [`Subscribe`]: struct.Subscribe.html
/// [`Shutdown`]: struct.Shutdown.html
pub struct Controller {
    /// Configured timeout for graceful shutdowns.
    timeout: Duration,
    /// Subscribed actors for the shutdown message.
    subscribers: Vec<Recipient<Shutdown>>,
}

impl Controller {
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

        // Run the factory and exit early if an error happens. The return value of the factory is
        // discarded for convenience, to allow shorthand notations.
        factory()?;

        // Ensure that the controller starts if no actor has started it yet. It will register with
        // `ProcessSignals` shut down even if no actors have subscribed. If we remove this line, the
        // controller will not be instanciated and our system will not listen for signals.
        Controller::from_registry();

        // All actors have started successfully. Run the system, which blocks the current thread
        // until a signal arrives or `Controller::stop` is called.
        log::info!("relay server starting");
        sys.run();
        log::info!("relay shutdown complete");

        Ok(())
    }

    /// Stops the current system immediately without sending a shutdown signal.
    pub fn stop(&mut self, context: &mut Context<Self>) {
        // Delay the shutdown for 100ms to allow synchronized futures to execute their error
        // handlers. Once `System::stop` is called, futures won't be polled anymore and we will not
        // be able to print error messages.
        context.run_later(Duration::from_millis(100), |_, _| System::current().stop());
    }

    /// Performs a graceful shutdown with the given timeout.
    ///
    /// This sends a `Shutdown` message to all subscribed actors and waits for them to finish. As
    /// soon as all actors have completed, `Controller::stop` is called.
    fn shutdown(&mut self, context: &mut Context<Self>, timeout: Option<Duration>) {
        // Send a shutdown signal to all registered subscribers (including self). They will report
        // when the shutdown has completed. Note that we ignore all errors to make sure that we
        // don't cancel the shutdown of other actors if one actor fails.
        let futures: Vec<_> = self
            .subscribers
            .iter()
            .map(|recipient| recipient.send(Shutdown { timeout }))
            .map(|future| future.then(|_| Ok(())))
            .collect();

        // Once all shutdowns have completed, we can schedule a stop of the actix system. It is
        // performed with a slight delay to give pending synced futures a chance to perform their
        // error handlers.
        future::join_all(futures)
            .into_actor(self)
            .and_then(|_, slf, ctx| {
                slf.stop(ctx);
                fut::ok(())
            })
            .spawn(context);
    }
}

impl Default for Controller {
    fn default() -> Self {
        Controller {
            timeout: Duration::from_secs(SHUTDOWN_TIMEOUT.into()),
            subscribers: Vec::new(),
        }
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
                log::info!("SIGINT received, exiting");
                self.shutdown(context, None);
            }
            signal::SignalType::Quit => {
                log::info!("SIGQUIT received, exiting");
                self.shutdown(context, None);
            }
            signal::SignalType::Term => {
                let timeout = self.timeout;
                log::info!("SIGTERM received, stopping in {}s", timeout.as_secs());
                self.shutdown(context, Some(timeout));
            }
            _ => (),
        }
    }
}

/// Subscribtion message for [`Shutdown`] events.
///
/// [`Shutdown`]: struct.Shutdown.html
pub struct Subscribe(pub Recipient<Shutdown>);

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
/// The specified timeout is only a hint to the implementor of this message. A handler has to
/// ensure that it doesn't take significantly longer to resolve the future. Ideally, open work is
/// persisted or finished in an orderly manner but no new requests are accepted anymore.
///
/// The implementor may indicate a timeout by responding with `Err(TimeoutError)`. At the moment,
/// this does not have any consequences for the shutdown.
///
/// [`Controller`]: struct.Controller.html
pub struct Shutdown {
    /// The timeout for this shutdown. `None` indicates an immediate forced shutdown.
    pub timeout: Option<Duration>,
}

impl Message for Shutdown {
    type Result = Result<(), TimeoutError>;
}

/// Signals to the controller that it should initiate immediate shutdown.
///
/// The controller will send [`Shutdown`] messages to all subscribed actors, but then immediately
/// stop the system.
///
/// [`Shutdown`]: struct.Shutdown.html
pub struct Stop;

impl Message for Stop {
    type Result = ();
}

impl Handler<Stop> for Controller {
    type Result = ();

    fn handle(&mut self, _message: Stop, context: &mut Self::Context) -> Self::Result {
        log::info!("initiating immediate shutdown");
        self.shutdown(context, None);
    }
}
