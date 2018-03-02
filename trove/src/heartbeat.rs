use std::sync::Arc;
use std::time::Duration;

use futures::Future;
use tokio_core::reactor::Timeout;

use types::TroveContext;

pub(crate) fn spawn_heartbeat(ctx: Arc<TroveContext>) {
    info!("starting heartbeat service");
    schedule_heartbeat(ctx);
}

fn schedule_heartbeat(ctx: Arc<TroveContext>) {
    let inner_ctx = ctx.clone();
    let config = &ctx.state().config;
    ctx.handle().spawn(
        Timeout::new(config.heartbeat_interval.to_std().unwrap(), &ctx.handle())
            .unwrap()
            .and_then(|_| {
                perform_heartbeat(inner_ctx);
                Ok(())
            })
            .or_else(|_| -> Result<_, _> {
                panic!("failed to schedule register");
            }),
    );
}

fn perform_heartbeat(ctx: Arc<TroveContext>) {
    let state = ctx.state();

    // if we encounter a non authenticated state we shut down.  The authenticator
    // code will respawn us.
    if !state.auth_state().is_authenticated() {
        warn!("heartbeat service encountered non authenticated trove. shutting down");
        return;
    }

    schedule_heartbeat(ctx);
}
