use actix::prelude::*;

pub struct AwsExtension;

impl Actor for AwsExtension {
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Self::Context) {
        relay_log::info!("AWS extension started");
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
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
