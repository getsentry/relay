use relay_config::Config;
use relay_statsd::metric;
use relay_system::{Controller, Service, Shutdown};

use crate::service::HttpServer;
use crate::statsd::RelayCounters;

pub struct ServerService {
    http_server: HttpServer,
}

impl ServerService {
    pub fn start(config: Config) -> anyhow::Result<()> {
        metric!(counter(RelayCounters::ServerStarting) += 1);
        let http_server = HttpServer::start(config)?;
        Self { http_server }.start();
        Ok(())
    }
}

impl Service for ServerService {
    type Interface = ();

    fn spawn_handler(self, _rx: relay_system::Receiver<Self::Interface>) {
        tokio::spawn(async move {
            let mut shutdown = Controller::shutdown_handle();
            loop {
                tokio::select! {
                    Shutdown { timeout } = shutdown.notified() => {
                       self.http_server.shutdown(timeout.is_some());
                    },
                    else => break,
                }
            }
        });
    }
}
