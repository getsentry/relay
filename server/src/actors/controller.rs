use std::time::Duration;

use actix::actors::signal;
use actix::prelude::*;
use actix_web::server::StopServer;
use futures::future;
use futures::prelude::*;

use semaphore_common::Config;

use service::{self, ServiceState};

pub use service::ServerError;
pub use utils::TimeoutError;

pub struct Controller {
    http_server: Recipient<StopServer>,
    subscribers: Vec<Recipient<Shutdown>>,
}

impl Controller {
    pub fn start(config: Config) -> Result<Addr<Self>, ServerError> {
        let controller = Controller {
            http_server: service::start(ServiceState::start(config))?,
            subscribers: Vec::new(),
        };

        Ok(controller.start())
    }

    pub fn run(config: Config) -> Result<(), ServerError> {
        let sys = System::new("relay");
        Self::start(config)?;

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
            .and_then(|_, actor, context| future::ok(actor.stop(context)).into_actor(actor))
            .spawn(context);
    }
}

impl Actor for Controller {
    type Context = Context<Self>;

    fn started(&mut self, context: &mut Self::Context) {
        // Subscribe for process signals. This will emit the appropriate shutdown events
        signal::ProcessSignals::from_registry()
            .do_send(signal::Subscribe(context.address().recipient()));

        // Register for our own shutdown events to shut down the managed actix-web http server
        let recipient = context.address().recipient();
        context.notify(Subscribe(recipient));
    }
}

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
                let timeout = Duration::from_secs(5); // TODO(ja)
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

impl Handler<Shutdown> for Controller {
    type Result = ResponseFuture<(), TimeoutError>;

    fn handle(&mut self, message: Shutdown, _context: &mut Self::Context) -> Self::Result {
        let graceful = message.timeout.is_some();

        // We assume graceful shutdown if we're given a timeout. The actix-web http server is
        // configured with the same timeout, so it will match. Unfortunately, we have to drop any
        // errors  and replace them with the generic `TimeoutError`.
        let future = self
            .http_server
            .send(StopServer { graceful })
            .map_err(|_| TimeoutError)
            .and_then(|result| result.map_err(|_| TimeoutError));

        Box::new(future)
    }
}
