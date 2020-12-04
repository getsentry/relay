use std::fmt;

use failure::AsFail;

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
///
/// # Example
///
/// ```
/// if let Err(error) = std::env::var("FOO") {
///     relay_log::ensure_error(&error);
/// }
/// ```
pub fn ensure_error<E: failure::AsFail>(error: &E) {
    if log::log_enabled!(log::Level::Error) {
        log::error!("{}", LogError(error));
    } else {
        eprintln!("error: {}", LogError(error));
    }
}

/// A wrapper around a [`Fail`](failure::Fail) that prints its causes.
///
/// # Example
///
/// ```
/// use relay_log::LogError;
///
/// if let Err(error) = std::env::var("FOO") {
///     relay_log::error!("env failed: {}", LogError(&error));
/// }
/// ```
pub struct LogError<'a, E: AsFail + ?Sized>(pub &'a E);

impl<'a, E: AsFail + ?Sized> fmt::Display for LogError<'a, E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let fail = self.0.as_fail();

        write!(f, "{}", fail)?;
        for cause in fail.iter_causes() {
            write!(f, "\n  caused by: {}", cause)?;
        }

        if backtrace_enabled() {
            if let Some(backtrace) = fail.backtrace() {
                write!(f, "\n\n{:?}", backtrace)?;
            }
        }

        Ok(())
    }
}
