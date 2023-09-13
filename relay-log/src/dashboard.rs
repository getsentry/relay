use once_cell::sync::Lazy;
use tokio::sync::broadcast;
use tracing::Subscriber;
use tracing_subscriber::Layer;

/// Channel to deliver logs.
pub static LOGS: Lazy<broadcast::Sender<Vec<u8>>> = Lazy::new(|| {
    let (tx, _) = broadcast::channel(2000);
    tx
});

/// Writer for the logs out to the [`tokio::sync::broadcast`] channel.
pub struct LogsWriter;

impl std::io::Write for LogsWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let tx = &*LOGS;
        let buf_len = buf.len();
        tx.send(buf.to_vec()).ok();
        Ok(buf_len)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Returns the log writer.
fn make_logs_writer() -> impl std::io::Write {
    LogsWriter
}

/// Returns the dashbaord subscriber.
pub fn dashboard_subscriber<S>() -> impl Layer<S> + Send + Sync
where
    S: Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    tracing_subscriber::fmt::layer()
        .with_writer(make_logs_writer)
        .with_target(true)
        .with_ansi(true)
        .compact()
}
