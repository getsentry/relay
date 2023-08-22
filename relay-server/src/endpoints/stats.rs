use axum::{
    extract::ws::{WebSocket, WebSocketUpgrade},
    response::Response,
};
use relay_statsd::hijack_metrics;
use tokio::task::spawn_blocking;

pub async fn handle(ws: WebSocketUpgrade) -> Response {
    ws.on_upgrade(handle_socket)
}

async fn handle_socket(mut socket: WebSocket) {
    let rx = hijack_metrics();
    loop {
        let rx = rx.clone();
        if let Ok(Ok(bytes)) = spawn_blocking(move || rx.recv()).await {
            socket.send(bytes.into()).await.ok();
        }
    }
}
