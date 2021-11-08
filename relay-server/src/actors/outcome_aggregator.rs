use actix::{Actor, Context, Handler, Supervised, SystemService};

use super::outcome::{OutcomeError, TrackOutcome};

pub struct OutcomeAggregator {}

impl Actor for OutcomeAggregator {
    type Context = Context<Self>;
}

impl Supervised for OutcomeAggregator {}

impl SystemService for OutcomeAggregator {}

impl Default for OutcomeAggregator {
    fn default() -> Self {
        unimplemented!("register with the SystemRegistry instead")
    }
}

impl Handler<TrackOutcome> for OutcomeAggregator {
    type Result = Result<(), OutcomeError>;

    fn handle(&mut self, msg: TrackOutcome, ctx: &mut Self::Context) -> Self::Result {
        relay_log::trace!("Outcome aggregation requested: {:?}", msg);
        Ok(())
    }
}
