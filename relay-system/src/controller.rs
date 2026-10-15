use std::io;

/// Awaits until a shutdown signal has been received.
#[cfg(unix)]
pub async fn shutdown() -> io::Result<()> {
    use tokio::signal::unix::{SignalKind, signal};

    let mut sig_int = signal(SignalKind::interrupt())?;
    let mut sig_quit = signal(SignalKind::quit())?;
    let mut sig_term = signal(SignalKind::terminate())?;

    tokio::select! {
        biased;

        Some(()) = sig_int.recv() => {
            relay_log::info!("SIGINT received, exiting");

        }
        Some(()) = sig_quit.recv() => {
            relay_log::info!("SIGQUIT received, exiting");

        }
        Some(()) = sig_term.recv() => {
            relay_log::info!("SIGTERM received, exiting");

        }
    };

    Ok(())
}

/// Awaits until a shutdown signal has been received.
#[cfg(windows)]
pub async fn shutdown(timeout: Duration) -> io::Result<()> {
    use tokio::signal::windows::{ctrl_break, ctrl_c, ctrl_close};

    let mut ctrl_c = ctrl_c()?;
    let mut ctrl_break = ctrl_break()?;
    let mut ctrl_close = ctrl_close()?;

    tokio::select! {
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
    };

    Ok(())
}
