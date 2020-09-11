//! Provides a metered client connector for upstream connections.
//!
//! See the [`MeteredConnector`] struct for more information.
//!
//! [`MeteredConnector`]: struct.MeteredConnector.html

use std::sync::Arc;
use std::time::Duration;

use actix::actors::resolver::Resolver;
use actix::prelude::{Actor, Addr, Context, Handler, System};
use actix_web::client::{ClientConnector, ClientConnectorStats};
use trust_dns_resolver::config::ResolverOpts;

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

        // Relay communicates with a single upstream that is supposed to be known ahead of time. If
        // that upstream cannot be resolved, this is usually due to one of few reasons:
        //
        //  1. A typo in the configuration. This is usually caught during development time and then
        //     fixed by the author.
        //  2. A service like load balancer or docker container starting up with delay. In such a
        //     case, Relay is expected to connect as soon as the service becomes available.
        //  3. Misconfiguration in the network, most likely DNS server.
        //
        // By default, Relay detects these issues during startup time and employs exponential
        // backoff to reconnect. Since outgoing traffic is of low volume at this point, the TTL for
        // negative DNS caches can be set to a few seconds.
        let mut opts = ResolverOpts::default();
        opts.negative_max_ttl = Some(Duration::from_secs(3));
        let resolver = Resolver::new(Default::default(), opts).start();

        System::current().registry().set(resolver);

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
        log::info!("metered connector started");
    }

    fn stopped(&mut self, _context: &mut Self::Context) {
        log::info!("metered connector stopped");
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
