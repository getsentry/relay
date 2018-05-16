use std::net::TcpListener;

use actix_web::error::Error;
use failure::Fail;
use sentry::integrations::failure::exception_from_single_fail;
use sentry::protocol::{Event, Level};
use sentry::with_client_and_scope;
use uuid::Uuid;

/// Returns all externally passed TCP listeners.
pub fn get_external_tcp_listeners() -> Vec<TcpListener> {
    #[cfg(not(windows))]
    {
        use libc::getpid;
        use std::env;
        use std::os::unix::io::{FromRawFd, RawFd};

        // catflap
        if let Some(fd) = env::var("LISTEN_FD").ok().and_then(|fd| fd.parse().ok()) {
            return vec![unsafe { TcpListener::from_raw_fd(fd) }];
        }

        // systemd
        if env::var("LISTEN_PID").ok().and_then(|x| x.parse().ok()) == Some(unsafe { getpid() }) {
            let count: Option<usize> = env::var("LISTEN_FDS").ok().and_then(|x| x.parse().ok());
            env::remove_var("LISTEN_PID");
            env::remove_var("LISTEN_FDS");
            if let Some(count) = count {
                return (0..count)
                    .map(|offset| unsafe { TcpListener::from_raw_fd(3 + offset as RawFd) })
                    .collect();
            }
        }

        vec![]
    }
    #[cfg(windows)]
    {
        vec![]
    }
}

/// Reports an actix error to sentry.
pub fn report_actix_error_to_sentry(err: &Error) -> Uuid {
    with_client_and_scope(|client, scope| {
        let mut exceptions = vec![
            exception_from_single_fail(err.cause(), Some(err.backtrace())),
        ];

        let mut ptr: Option<&Fail> = err.cause().cause();
        while let Some(cause) = ptr {
            exceptions.push(exception_from_single_fail(cause, cause.backtrace()));
            ptr = Some(cause);
        }

        exceptions.reverse();

        client.capture_event(
            Event {
                exceptions: exceptions,
                level: Level::Error,
                ..Default::default()
            },
            Some(scope),
        )
    })
}
