use std::net::TcpListener;

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
