use actix::prelude::*;
use actix_web::server::StopServer;

use semaphore_common::Config;

use service::{self, ServiceState};

pub use service::ServerError;

pub struct Controller {
    services: ServiceState,
    http_server: Recipient<StopServer>,
}

impl Controller {
    pub fn start(config: Config) -> Result<Addr<Self>, ServerError> {
        let services = ServiceState::start(config);
        let http_server = service::start(services.clone())?;

        Ok(Controller {
            services,
            http_server,
        }.start())
    }

    pub fn run(config: Config) -> Result<(), ServerError> {
        let sys = System::new("relay");
        Self::start(config)?;

        info!("relay server starting");
        sys.run();
        info!("relay shutdown complete");

        Ok(())
    }
}

impl Actor for Controller {
    type Context = Context<Self>;
}
