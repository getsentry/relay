use std::sync::Arc;

use futures::Future;
use tokio_core::reactor::Timeout;

use types::TroveContext;

use smith_aorta::{HeartbeatResponse, QueryStatus};

pub(crate) fn spawn_heartbeat(ctx: Arc<TroveContext>) {
    info!("starting heartbeat service");
    schedule_heartbeat(ctx);
}

fn schedule_heartbeat(ctx: Arc<TroveContext>) {
    let inner_ctx = ctx.clone();
    let config = &ctx.state().config();
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

    let hb_req = state.query_manager().next_heartbeat_request();
    let inner_ctx_success = ctx.clone();
    let inner_ctx_failure = ctx.clone();

    ctx.handle().spawn(
        ctx.aorta_request(&hb_req)
            .and_then(move |response| {
                handle_heartbeat_response(inner_ctx_success.clone(), response);
                schedule_heartbeat(inner_ctx_success);
                Ok(())
            })
            .or_else(|err| {
                error!("heartbeat failed: {}", &err);
                schedule_heartbeat(inner_ctx_failure);
                Err(())
            }),
    );
}

fn handle_heartbeat_response(ctx: Arc<TroveContext>, response: HeartbeatResponse) {
    let state = ctx.state();
    let query_manager = state.query_manager();
    for (query_id, result) in response.query_results.into_iter() {
        if result.status == QueryStatus::Pending {
            continue;
        }
        if let Some((project_id, mut callback)) = query_manager.pop_callback(query_id) {
            if let Some(project_state) = state.get_project_state(project_id) {
                if let Some(data) = result.result {
                    callback(&project_state, data, result.status == QueryStatus::Success);
                }
            }
        }
    }
}
