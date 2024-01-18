use std::io;
use std::time::Duration;

use once_cell::sync::Lazy;
use tokio::sync::watch;

/// Determines how to shut down the Relay system.
///
/// To initiate a shutdown, use [`Controller::shutdown`].
#[derive(Clone, Copy, Debug)]
pub enum ShutdownMode {
    /// Shut down gracefully within the configured timeout.
    ///
    /// This will signal all components to finish their work and leaves time to submit pending data
    /// to the upstream or preserve it for restart.
    Graceful,
    /// Shut down immediately without finishing pending work.
    ///
    /// Pending data may be lost.
    Immediate,
}

/// Shutdown request message sent by the [`Controller`] to subscribed services.
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

type Channel<T> = (watch::Sender<Option<T>>, watch::Receiver<Option<T>>);

/// Global channel to notify all services of a shutdown.
static SHUTDOWN: Lazy<Channel<Shutdown>> = Lazy::new(|| watch::channel(None));

/// Internal channel to trigger a manual shutdown via [`Controller::shutdown`].
static MANUAL_SHUTDOWN: Lazy<Channel<ShutdownMode>> = Lazy::new(|| watch::channel(None));

/// Notifies a service about an upcoming shutdown.
///
/// This handle is returned by [`Controller::shutdown_handle`].
// TODO: The receiver of this message can not yet signal they have completed shutdown.
pub struct ShutdownHandle(watch::Receiver<Option<Shutdown>>);

impl ShutdownHandle {
    /// Wait for a shutdown.
    ///
    /// This receives all shutdown signals since the [`Controller`] has been started, even before
    /// this shutdown handle has been obtained.
    ///
    /// # Cancel safety
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

    /// Wait for the shutdown and timeout to complete.
    ///
    /// This waits for the first shutdown signal and then conditionally waits for the shutdown
    /// timeout. If the shutdown timeout is interrupted by another signal, this function resolves
    /// immediately.
    ///
    /// # Cancel safety
    ///
    /// This method is **not** cancel safe.
    pub async fn finished(mut self) {
        // Wait for the first signal to initiate shutdown.
        let shutdown = self.notified().await;

        // If this is a graceful signal, wait for either the timeout to elapse, or any other signal
        // to upgrade to an immediate shutdown.
        if let Some(timeout) = shutdown.timeout {
            tokio::select! {
                _ = self.notified() => (),
                _ = tokio::time::sleep(timeout) => (),
            }
        }
    }
}

/// Service to start and gracefully stop the system runtime.
///
/// This service offers a static API to wait for a shutdown signal or manually initiate the Relay
/// shutdown. To use this functionality, it first needs to be started with [`Controller::start`].
///
/// To shut down gracefully, other services can register with [`Controller::shutdown_handle`]. When
/// a shutdown signal is sent to the process, every service will receive a [`Shutdown`] message with
/// an optional timeout. To wait for the entire shutdown sequence including the shutdown timeout
/// instead, use [`finished`](ShutdownHandle::finished). It resolves when the shutdown has
/// completed.
///
/// ## Signals
///
/// By default, the controller watches for process signals and converts them into graceful or
/// immediate shutdown messages. These signals are platform-dependent:
///
///  - Unix: `SIGINT` and `SIGQUIT` trigger an immediate shutdown, `SIGTERM` a graceful one.
///  - Windows: `CTRL-C`, `CTRL-BREAK`, `CTRL-CLOSE` all trigger an immediate shutdown.
///
/// ### Example
///
/// ```
/// use relay_system::{Controller, Service, Shutdown, ShutdownMode};
/// use std::time::Duration;
///
/// struct MyService;
///
/// impl Service for MyService {
///     type Interface = ();
///
///     fn spawn_handler(self, mut rx: relay_system::Receiver<Self::Interface>) {
///         tokio::spawn(async move {
///             let mut shutdown = Controller::shutdown_handle();
///
///             loop {
///                 tokio::select! {
///                     shutdown = shutdown.notified() => break, // Handle shutdown here
///                     Some(message) = rx.recv() => (),         // Process incoming message
///                 }
///             }
///         });
///     }
/// }
///
/// #[tokio::main(flavor = "current_thread")]
/// async fn main() {
///     // Start the controller near the beginning of application bootstrap. This allows other
///     // services to register for shutdown messages.
///     Controller::start(Duration::from_millis(10));
///
///     // Start all other services. Controller::shutdown_handle will use the same controller
///     // instance and receives the configured shutdown timeout.
///     let addr = MyService.start();
///
///     // By triggering a shutdown, all attached services will be notified. This happens
///     // automatically when a signal is sent to the process (e.g. SIGINT or SIGTERM).
///     Controller::shutdown(ShutdownMode::Graceful);
///
///     // Wait for the system to shut down before winding down the application.
///     Controller::shutdown_handle().finished().await;
/// }
/// ```
#[derive(Debug)]
pub struct Controller;

impl Controller {
    /// Starts a controller that monitors shutdown signals.
    pub fn start(shutdown_timeout: Duration) {
        tokio::spawn(monitor_shutdown(shutdown_timeout));
    }

    /// Manually initiates the shutdown process of the system.
    pub fn shutdown(mode: ShutdownMode) {
        let (ref tx, _) = *MANUAL_SHUTDOWN;
        tx.send(Some(mode)).ok();
    }

    /// Returns a [handle](ShutdownHandle) to receive shutdown notifications.
    pub fn shutdown_handle() -> ShutdownHandle {
        let (_, ref rx) = *SHUTDOWN;
        ShutdownHandle(rx.clone())
    }
}

#[cfg(unix)]
async fn monitor_shutdown(timeout: Duration) -> io::Result<()> {
    use tokio::signal::unix::{signal, SignalKind};

    let mut sig_int = signal(SignalKind::interrupt())?;
    let mut sig_quit = signal(SignalKind::quit())?;
    let mut sig_term = signal(SignalKind::terminate())?;

    let (ref tx, _) = *SHUTDOWN;
    let mut manual = MANUAL_SHUTDOWN.1.clone();

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
            Ok(()) = manual.changed() => match *manual.borrow() {
                Some(ShutdownMode::Graceful) => {
                    relay_log::info!("Graceful shutdown initiated, stopping in {}s", timeout.as_secs());
                    Some(timeout)
                }
                Some(ShutdownMode::Immediate) => {
                    relay_log::info!("Immediate shutdown initiated");
                    None
                },
                None => continue,
            },

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

    let (ref tx, _) = *SHUTDOWN;
    let mut manual = MANUAL_SHUTDOWN.1.clone();

    loop {
        let timeout = tokio::select! {
            biased;

            Some(()) = ctrl_c.recv() => {
                relay_log::info!("CTRL-C received, exiting");
                None
            }
            Some(()) = ctrl_break.recv() => {
                relay_log::info!("CTRL-BREAK received, exiting");
                None
            }
            Some(()) = ctrl_close.recv() => {
                relay_log::info!("CTRL-CLOSE received, exiting");
                None
            }
            Ok(()) = manual.changed() => match *manual.borrow() {
                Some(ShutdownMode::Graceful) => {
                    relay_log::info!("Graceful shutdown initiated, stopping in {}s", timeout.as_secs());
                    Some(timeout)
                }
                Some(ShutdownMode::Immediate) => {
                    relay_log::info!("Immediate shutdown initiated");
                    None
                },
                None => continue,
            },

            else => break,
        };

        tx.send(Some(Shutdown { timeout })).ok();
    }

    Ok(())
}

/*
TODO: Tests disabled since there is no isloation. Should be re-enabled once Controller-instances are
passed into services.

#[cfg(test)]
mod tests {
    use tokio::time::Instant;

    use super::*;

    #[tokio::test]
    async fn handle_receives_immediate_shutdown() {
        tokio::time::pause();

        Controller::start(Duration::from_secs(1));
        let mut handle = Controller::shutdown_handle();

        Controller::shutdown(ShutdownMode::Immediate);
        let shutdown = handle.notified().await;
        assert_eq!(shutdown.timeout, None);
    }

    #[tokio::test]
    async fn receives_graceful_shutdown() {
        tokio::time::pause();

        let timeout = Duration::from_secs(1);
        Controller::start(timeout);
        let mut handle = Controller::shutdown_handle();

        Controller::shutdown(ShutdownMode::Immediate);
        let shutdown = handle.notified().await;
        assert_eq!(shutdown.timeout, Some(timeout));
    }

    #[tokio::test]
    async fn handle_receives_past_shutdown() {
        tokio::time::pause();

        Controller::start(Duration::from_secs(1));
        Controller::shutdown(ShutdownMode::Immediate);

        Controller::shutdown_handle().notified().await;
        // should not block
    }

    #[tokio::test]
    async fn handle_waits_for_timeout() {
        tokio::time::pause();

        let timeout = Duration::from_secs(1);
        Controller::start(timeout);
        let shutdown = Controller::shutdown_handle();

        let start = Instant::now();
        Controller::shutdown(ShutdownMode::Graceful);
        shutdown.finished().await;

        assert_eq!(Instant::now() - start, timeout);
    }

    #[tokio::test]
    async fn finish_exits_early() {
        tokio::time::pause();

        Controller::start(Duration::from_secs(1));
        let shutdown = Controller::shutdown_handle();

        let start = Instant::now();
        Controller::shutdown(ShutdownMode::Graceful);
        Controller::shutdown(ShutdownMode::Immediate);

        shutdown.finished().await;
        assert_eq!(Instant::now(), start);
    }
}
*/
