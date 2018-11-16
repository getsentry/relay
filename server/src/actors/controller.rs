use std::time::Duration;

use actix::actors::signal;
use actix::fut;
use actix::prelude::*;
use futures::future;
use futures::prelude::*;

use crate::constants::SHUTDOWN_TIMEOUT;

pub use crate::service::ServerError;
pub use crate::utils::TimeoutError;

pub struct Controller {
    timeout: Duration,
    subscribers: Vec<Recipient<Shutdown>>,
}

impl Controller {
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
        info!("relay server starting");
        sys.run();
        info!("relay shutdown complete");

        Ok(())
    }

    pub fn stop(&mut self, context: &mut Context<Self>) {
        context.run_later(Duration::from_millis(100), |_, _| System::current().stop());
    }

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
            }).spawn(context);
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
                info!("SIGINT received, exiting");
                self.shutdown(context, None);
            }
            signal::SignalType::Quit => {
                info!("SIGQUIT received, exiting");
                self.shutdown(context, None);
            }
            signal::SignalType::Term => {
                let timeout = self.timeout;
                info!("SIGTERM received, stopping in {}s", timeout.as_secs());
                self.shutdown(context, Some(timeout));
            }
            _ => (),
        }
    }
}

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

pub struct Shutdown {
    pub timeout: Option<Duration>,
}

impl Message for Shutdown {
    type Result = Result<(), TimeoutError>;
}
