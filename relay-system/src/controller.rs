use std::io;
use std::time::Duration;

use once_cell::sync::OnceCell;
use tokio::sync::watch;

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

/// Service to start and gracefully stop an the system runtime.
///
/// This service contains a static `run` method which will run a tokio system and block the current
/// thread until the system shuts down again.
///
/// To shut down more gracefully, other actors can register with [`Controller::shutdown_handle`].
/// When a shutdown signal is sent to the process, they will receive a [`Shutdown`] message with an
/// optional timeout. They can respond with a future, after which they will be stopped. Once all
/// registered actors have stopped successfully, the entire system will stop.
///
/// ### Example
///
/// ```ignore
/// // TODO: This example is outdated and needs to be updated when the Controller is rewritten.
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
/// Controller::run(|| -> Result<(), ()> {
///     MyActor.start();
///     # System::current().stop();
///     Ok(())
/// }).unwrap();
/// ```
#[derive(Debug)]
pub struct Controller;

impl Controller {
    /// Starts a controller that monitors shutdown signals.
    pub fn start(shutdown_timeout: Duration) {
        tokio::spawn(monitor_shutdown(shutdown_timeout));
    }

    /// Initiates the shutdown process of the system.
    pub fn trigger_shutdown(graceful: bool) {
        todo!("graceful {graceful}")
    }

    /// Returns a [handle](ShutdownHandle) to receive shutdown notifications.
    pub fn shutdown_handle() -> ShutdownHandle {
        let (_, ref rx) = SHUTDOWN.get_or_init(|| watch::channel(None));
        ShutdownHandle(rx.clone())
    }

    /// Wait for the shutdown and timeout.
    ///
    /// This waits for the first shutdown signal and then conditionally waits for the shutdown
    /// timeout. If the shutdown timeout is interrupted by another signal, this function resolves
    /// immediately.
    pub async fn shutdown() {
        // Wait for the first signal to initiate shutdown.
        let mut handle = Controller::shutdown_handle();
        let shutdown = handle.notified().await;

        // If this is a graceful signal, wait for either the timeout to elapse, or any other signal
        // to upgrade to an immediate shutdown.
        if let Some(timeout) = shutdown.timeout {
            tokio::select! {
                _ = handle.notified() => (),
                _ = tokio::time::sleep(timeout) => (),
            }
        }
    }
}

#[cfg(unix)]
async fn monitor_shutdown(timeout: Duration) -> io::Result<()> {
    use tokio::signal::unix::{signal, SignalKind};

    let mut sig_int = signal(SignalKind::interrupt())?;
    let mut sig_quit = signal(SignalKind::quit())?;
    let mut sig_term = signal(SignalKind::terminate())?;

    let (tx, _) = SHUTDOWN.get_or_init(|| watch::channel(None));

    loop {
        let timeout = tokio::select! {
            biased;

            Some(()) = sig_int.recv() => {
                relay_log::info!("SIGINT received, exiting");
                None
            }
            Some(()) = sig_quit.recv() => {
                relay_log::info!("SIGQUIT received, exiting");
                None
            }
            Some(()) = sig_term.recv() => {
                relay_log::info!("SIGTERM received, stopping in {}s", timeout.as_secs());
                Some(timeout)
            }

            else => break,
        };

        tx.send(Some(Shutdown { timeout })).ok();
    }

    Ok(())
}

#[cfg(windows)]
async fn monitor_shutdown(timeout: Duration) -> io::Result<()> {
    use tokio::signal::windows::{ctrl_break, ctrl_c, ctrl_close};

    let mut ctrl_c = ctrl_c()?;
    let mut ctrl_break = ctrl_break()?;
    let mut ctrl_close = ctrl_close()?;

    let (tx, _) = SHUTDOWN.get_or_init(|| watch::channel(None));

    loop {
        tokio::select! {
            biased;

            Some(()) = ctrl_c.recv() => relay_log::info!("CTRL-C received, exiting"),
            Some(()) = ctrl_break.recv() => relay_log::info!("CTRL-BREAK received, exiting"),
            Some(()) = ctrl_close.recv() => relay_log::info!("CTRL-CLOSE received, exiting"),

            else => break,
        };

        tx.send(Some(Shutdown { timeout: None })).ok();
    }

    Ok(())
}
