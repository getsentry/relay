use actix::prelude::*;
use std::thread;
use std::time;

pub struct AwsExtension;

impl Actor for AwsExtension {
    type Context = Context<Self>;

    fn started(&mut self, context: &mut Self::Context) {
        relay_log::info!("AWS extension started");
        context.notify(NextEvent {});
    }

    fn stopped(&mut self, _context: &mut Self::Context) {
        relay_log::info!("AWS extension stopped");
    }
}

impl Default for AwsExtension {
    fn default() -> Self {
        unimplemented!("register with the SystemRegistry instead")
    }
}

impl Supervised for AwsExtension {}

impl SystemService for AwsExtension {}

pub struct NextEvent;

impl Message for NextEvent {
    type Result = ();
}

impl Handler<NextEvent> for AwsExtension {
    type Result = ();

    fn handle(&mut self, _message: NextEvent, context: &mut Self::Context) -> Self::Result {
        relay_log::info!("Handling next event");
        thread::sleep(time::Duration::from_secs(1));
        context.notify(NextEvent {});
    }
}
