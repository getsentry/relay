//! Provides a metered client connector for upstream connections.
//!
//! See the [`MeteredConnector`] struct for more information.

use std::sync::Arc;

use actix::prelude::*;
use actix_web::client::{ClientConnector, ClientConnectorStats};

use relay_common::metric;
use relay_config::Config;

use crate::metrics::{RelayCounters, RelayHistograms};

/// Actor that reports connection metrics.
///
/// This actor implements a receiver for `ClientConnectorStats`, which provide periodic updates on
/// the client connector used to establish upstream connections. The numbers are reported into the
/// `connector.*` counter and historgram metrics.
///
/// # Example
///
/// To start the connector, use [`MeteredConnector::start`]. This also starts the actual client
/// connector and registers it as default system service. After that, each client request uses this
/// connector by default.
///
/// ```ignore
/// use std::sync::Arc;
/// use relay_config::Config;
///
/// let config = Arc::new(Config::default());
/// MeteredConnector::start(config);
///
/// let request = actix_web::client::get("http://example.org")
///    .finish();
/// ```
#[derive(Debug)]
pub struct MeteredConnector;

impl MeteredConnector {
    pub fn start(config: Arc<Config>) -> Addr<Self> {
        let metered_connector = Self.start();

        let connector = ClientConnector::default()
            .limit(config.max_concurrent_requests())
            .stats(metered_connector.clone().recipient())
            .start();

        // Register as default system service.
        System::current().registry().set(connector);

        metered_connector
    }
}

impl Default for MeteredConnector {
    fn default() -> Self {
        Self
    }
}

impl Actor for MeteredConnector {
    type Context = Context<Self>;

    fn started(&mut self, _context: &mut Self::Context) {
        relay_log::info!("metered connector started");
    }

    fn stopped(&mut self, _context: &mut Self::Context) {
        relay_log::info!("metered connector stopped");
    }
}

impl Handler<ClientConnectorStats> for MeteredConnector {
    type Result = ();

    fn handle(&mut self, message: ClientConnectorStats, _context: &mut Self::Context) {
        metric!(histogram(RelayHistograms::ConnectorWaitQueue) = message.wait_queue as u64);
        metric!(counter(RelayCounters::ConnectorReused) += message.reused as i64);
        metric!(counter(RelayCounters::ConnectorOpened) += message.opened as i64);
        metric!(counter(RelayCounters::ConnectorClosed) += message.closed as i64);
        metric!(counter(RelayCounters::ConnectorErrors) += message.errors as i64);
        metric!(counter(RelayCounters::ConnectorTimeouts) += message.timeouts as i64);
    }
}
