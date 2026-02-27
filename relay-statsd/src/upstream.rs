use std::io::{self, Write};
use std::net::{Ipv4Addr, UdpSocket};
#[cfg(unix)]
use std::os::unix::net::{UnixDatagram, UnixStream};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use statsdproxy::middleware::Middleware;
use statsdproxy::types::Metric;

pub enum Remote {
    Udp(UdpSocket),
    #[cfg(unix)]
    UnixStream(UnixStream),
    #[cfg(unix)]
    UnixDatagram(UnixDatagram),
}

impl Remote {
    fn connect(addr: &str) -> io::Result<Self> {
        // Try treating the address as a fully-qualified URL, where the scheme is the transport identifier.
        if let Some((scheme, path)) = addr.split_once("://") {
            return match scheme {
                "udp" => {
                    let socket = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0))?;
                    socket.connect(path)?;
                    socket.set_nonblocking(true)?;

                    Ok(Self::Udp(socket))
                }
                #[cfg(unix)]
                "unixstream" => {
                    let socket = UnixStream::connect(path)?;
                    socket.set_nonblocking(true)?;

                    Ok(Self::UnixStream(socket))
                }
                #[cfg(unix)]
                "unixgram" => {
                    let socket = UnixDatagram::unbound()?;
                    socket.connect(path)?;
                    socket.set_nonblocking(true)?;

                    Ok(Self::UnixDatagram(socket))
                }
                _ => Err(io::Error::other(format!(
                    "invalid scheme '{scheme}', expected one of 'udp', 'unixstream', 'unixgram'"
                ))),
            };
        }

        // If there is no scheme, fall back to a UDP socket
        let socket = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0))?;
        socket.connect(addr)?;
        socket.set_nonblocking(true)?;

        Ok(Self::Udp(socket))
    }

    fn bufsize(&self) -> usize {
        match self {
            // The original statsdproxy uses.
            Self::Udp(_) => 512,
            // Datadog recommends 8192 for unix sockets.
            //
            // See: <https://docs.datadoghq.com/extend/dogstatsd/high_throughput/?tab=go#ensure-proper-packet-sizes>
            #[cfg(unix)]
            Self::UnixStream(_) => 8192,
            #[cfg(unix)]
            Self::UnixDatagram(_) => 1024,
        }
    }

    fn send(&mut self, buf: &[u8]) {
        let result = match self {
            Self::Udp(socket) => socket.send(buf).map(drop),
            #[cfg(unix)]
            Self::UnixStream(socket) => {
                let length = (buf.len() as u32).to_le_bytes();
                let length = io::IoSlice::new(&length);
                let buffer = io::IoSlice::new(buf);

                write_all_vectored(socket, [length, buffer].as_mut_slice())
            }
            #[cfg(unix)]
            Self::UnixDatagram(socket) => socket.send(buf).map(drop),
        };

        if let Err(err) = result {
            relay_log::warn!("failed to send metrics to upstream: {err}");
        }
    }
}

pub struct Upstream {
    remote: Remote,
    buffer: Vec<u8>,
    buf_used: usize,
    last_sent_at: SystemTime,
}

impl Upstream {
    pub fn connect(upstream: &str) -> io::Result<Self> {
        let remote = Remote::connect(upstream)?;

        Ok(Upstream {
            buffer: vec![0; remote.bufsize()],
            remote,
            buf_used: 0,
            last_sent_at: UNIX_EPOCH,
        })
    }

    fn flush(&mut self) {
        if self.buf_used > 0 {
            self.remote.send(&self.buffer[..self.buf_used]);
            self.buf_used = 0;
        }
        self.last_sent_at = SystemTime::now(); // Annoyingly superfluous call to now().
    }

    fn timed_flush(&mut self) {
        let now = SystemTime::now();
        if now
            .duration_since(self.last_sent_at)
            .map_or(true, |x| x > Duration::from_secs(1))
        {
            // We have not sent any metrics in a while. Flush the buffer.
            self.flush();
        }
    }
}

impl Drop for Upstream {
    fn drop(&mut self) {
        self.flush();
    }
}

impl Middleware for Upstream {
    fn submit(&mut self, metric: &mut Metric) {
        let metric_len = metric.raw.len();
        if metric_len + 1 > self.buffer.len() - self.buf_used {
            // Message bigger than space left in buffer. Flush the buffer.
            self.flush();
        }
        if metric_len > self.buffer.len() {
            // Message too big for the entire buffer, send it and pray.
            self.remote.send(&metric.raw);
        } else {
            // Put the message in the buffer, separating it from the previous message if any.
            if self.buf_used > 0 {
                self.buffer[self.buf_used] = b'\n';
                self.buf_used += 1;
            }
            self.buffer[self.buf_used..self.buf_used + metric_len].copy_from_slice(&metric.raw);
            self.buf_used += metric_len;
        }
        // poll gets called before submit, so if the buffer needed to be flushed for time reasons,
        // it already was.
    }

    fn poll(&mut self) {
        self.timed_flush();
    }
}

/// A [`Upstream`] which falls back to noop operations if connecting to the statsd sink fails.
pub enum TryUpstream {
    Upstream(Upstream),
    Error,
}

impl TryUpstream {
    pub fn connect(upstream: &str) -> Self {
        match Upstream::connect(upstream) {
            Ok(upstream) => Self::Upstream(upstream),
            Err(err) => {
                relay_log::error!(
                    error = &err as &dyn std::error::Error,
                    "failed to connect to statsd sink at {upstream}"
                );
                Self::Error
            }
        }
    }
}

impl Middleware for TryUpstream {
    fn submit(&mut self, metric: &mut Metric) {
        match self {
            Self::Upstream(upstream) => upstream.submit(metric),
            Self::Error => {}
        }
    }
}

/// Like [`UnixStream::write_vectored`], but ensuing all bytes are written,
///
/// The implementation is taken from the nightly `write_all_vectored` function.
#[cfg(unix)]
fn write_all_vectored(stream: &mut UnixStream, mut bufs: &mut [io::IoSlice<'_>]) -> io::Result<()> {
    // Guarantee that bufs is empty if it contains no data,
    // to avoid calling write_vectored if there is no data to be written.
    io::IoSlice::advance_slices(&mut bufs, 0);
    while !bufs.is_empty() {
        match stream.write_vectored(bufs) {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "failed to write whole buffer",
                ));
            }
            Ok(n) => io::IoSlice::advance_slices(&mut bufs, n),
            Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}
