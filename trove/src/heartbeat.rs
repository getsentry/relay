use std::sync::Arc;
use std::time::Duration;

use futures::Future;
use tokio_core::reactor::Timeout;

use types::TroveContext;

use smith_aorta::{HeartbeatResponse, QueryStatus};

pub(crate) fn spawn_heartbeat(ctx: Arc<TroveContext>) {
    info!("starting heartbeat service");
    schedule_heartbeat(ctx, Duration::from_secs(1));
}

fn schedule_heartbeat(ctx: Arc<TroveContext>, timeout: Duration) {
    let inner_ctx = ctx.clone();
    ctx.handle().spawn(
        Timeout::new(timeout, &ctx.handle())
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

    let (hb_req_opt, timeout) = state.request_manager().next_heartbeat_request();

    match hb_req_opt {
        Some(hb_req) => {
            let inner_ctx_success = ctx.clone();
            let inner_ctx_failure = ctx.clone();
            ctx.handle().spawn(
                ctx.aorta_request(&hb_req)
                    .and_then(move |response| {
                        handle_heartbeat_response(inner_ctx_success.clone(), response);
                        schedule_heartbeat(inner_ctx_success, timeout);
                        Ok(())
                    })
                    .or_else(|err| {
                        error!("heartbeat failed: {}", &err);
                        // on error retry after a second
                        schedule_heartbeat(inner_ctx_failure, Duration::from_secs(1));
                        Err(())
                    }),
            );
        }
        None => {
            schedule_heartbeat(ctx, timeout);
        }
    }
}

fn handle_heartbeat_response(ctx: Arc<TroveContext>, response: HeartbeatResponse) {
    let state = ctx.state();
    let request_manager = state.request_manager();
    for (query_id, result) in response.query_results.into_iter() {
        if result.status == QueryStatus::Pending {
            continue;
        }
        if let Some((project_id, mut callback)) = request_manager.pop_callback(query_id) {
            if let Some(project_state) = state.get_project_state(project_id) {
                if let Some(data) = result.result {
                    callback(&project_state, data, result.status == QueryStatus::Ok);
                }
            }
        }
    }
}
