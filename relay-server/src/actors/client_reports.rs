use actix::{Actor, Context, Handler, Message, Supervised, SystemService};
use relay_general::protocol::ClientReport;

pub struct SendClientReport(pub ClientReport);

impl Message for SendClientReport {
    type Result = Result<(), ()>;
}

pub struct ClientReportAggregator {}

impl Actor for ClientReportAggregator {
    type Context = Context<Self>;
}

impl Supervised for ClientReportAggregator {}

impl SystemService for ClientReportAggregator {}

impl Default for ClientReportAggregator {
    fn default() -> Self {
        unimplemented!("register with the SystemRegistry instead")
    }
}

impl Handler<SendClientReport> for ClientReportAggregator {
    type Result = Result<(), ()>;

    fn handle(&mut self, msg: SendClientReport, ctx: &mut Self::Context) -> Self::Result {
        relay_log::trace!("Client Report aggregation requested: {:?}", msg.0);
        Ok(())
    }
}
