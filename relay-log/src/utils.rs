use std::error::Error;
use std::fmt;

#[cfg(feature = "admin")]
use once_cell::sync::Lazy;
#[cfg(feature = "admin")]
use tokio::sync::broadcast;
use tracing::Level;

/// Channel to deliver logs.
#[cfg(feature = "admin")]
pub static LOGS: Lazy<broadcast::Sender<Vec<u8>>> = Lazy::new(|| {
    let (tx, _) = broadcast::channel(2000);
    tx
});

/// Returns `true` if backtrace printing is enabled.
///
/// # Example
///
/// ```
/// std::env::set_var("RUST_BACKTRACE", "full");
/// assert!(relay_log::backtrace_enabled());
/// ```
pub fn backtrace_enabled() -> bool {
    matches!(
        std::env::var("RUST_BACKTRACE").as_ref().map(String::as_str),
        Ok("1") | Ok("full")
    )
}

/// Logs an error to the configured logger or `stderr` if not yet configured.
///
/// Prefer to use [`relay_log::error`](crate::error) over this function whenever possible. This
/// function is intended to be used during startup, where initializing the logger may fail or when
/// errors need to be logged before the logger has been initialized.
pub fn ensure_error<E: AsRef<dyn Error>>(error: E) {
    if tracing::event_enabled!(Level::ERROR) {
        crate::error!(error = error.as_ref());
    } else {
        eprintln!("error: {}", LogError(error.as_ref()));
    }
}

/// A wrapper around an error that prints its causes.
struct LogError<'a, E: Error + ?Sized>(pub &'a E);

impl<'a, E: Error + ?Sized> fmt::Display for LogError<'a, E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)?;

        let mut source = self.0.source();
        while let Some(s) = source {
            write!(f, "\n  caused by: {s}")?;
            source = s.source();
        }

        // NOTE: This is where we would print a backtrace, once stabilized.

        Ok(())
    }
}
